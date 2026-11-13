"""上传有界闸门 BoundedUploadGate 的并发单元测试（纯 asyncio，无需 HTTP/DB）。

通过 anyio 的 pytest 插件在事件循环里执行 async 测试（项目已依赖 anyio）。
"""
import asyncio

import pytest

pytestmark = pytest.mark.anyio

from reporter.upload_gate import (
    BoundedUploadGate,
    GateFullError,
    UploadCancelledError,
)


@pytest.fixture()
def gate():
    return BoundedUploadGate(max_active=2, max_queued=3)


async def _enqueue_acquire(g, results, name):
    token = await g.try_enqueue()
    await g.acquire(token)
    results.append(name)
    return token


class TestBoundedUploadGate:
    async def test_full_capacity_rejects_with_503_error(self, gate):
        tokens = [await gate.try_enqueue() for _ in range(5)]
        with pytest.raises(GateFullError):
            await gate.try_enqueue()
        assert gate.stats()["inflight"] == 5
        for t in tokens[:2]:
            await gate.acquire(t)
        for t in tokens[2:]:
            await gate.request_cancel(t)
        for t in tokens[:2]:
            await gate.release(t)
        assert gate.stats()["inflight"] == 0

    async def test_active_slots_granted_then_queue_waits(self, gate):
        # 前 2 个直接拿到执行名额
        t1, t2 = await gate.try_enqueue(), await gate.try_enqueue()
        await gate.acquire(t1)
        await gate.acquire(t2)
        s = gate.stats()
        assert (s["active"], s["queued"]) == (2, 0)

        # 第 3 个排队
        t3 = await gate.try_enqueue()
        acquire_task = asyncio.create_task(gate.acquire(t3))
        await asyncio.sleep(0.05)
        assert not acquire_task.done()
        assert gate.stats()["queued"] == 1

        # 归还一个执行名额后，排队者获得名额
        await gate.release(t1)
        await asyncio.wait_for(acquire_task, timeout=1)
        assert gate.stats()["active"] == 2
        await gate.release(t2)
        await gate.release(t3)
        assert gate.stats()["inflight"] == 0

    async def test_cancel_queued_never_starts(self, gate):
        t1, t2 = await gate.try_enqueue(), await gate.try_enqueue()
        await gate.acquire(t1)
        await gate.acquire(t2)
        t3 = await gate.try_enqueue()
        acq = asyncio.create_task(gate.acquire(t3))
        await asyncio.sleep(0.05)

        cancelled = await gate.request_cancel(t3)
        assert cancelled is True
        with pytest.raises(UploadCancelledError):
            await acq
        # 排队名额已释放，且后续新请求可以正常入队并拿到名额
        assert gate.stats()["queued"] == 0
        await gate.release(t1)  # 腾出一个执行名额
        t4 = await gate.try_enqueue()
        await asyncio.wait_for(gate.acquire(t4), timeout=1)
        assert gate.stats()["active"] == 2
        await gate.release(t2)
        await gate.release(t4)

    async def test_cancel_after_grant_does_not_release_slot_early(self, gate):
        t1 = await gate.try_enqueue()
        await gate.acquire(t1)
        # 已 ACTIVE：取消只返回 False，执行名额保持占用
        assert await gate.request_cancel(t1) is False
        assert gate.stats()["active"] == 1
        # 显式 release 才归还（模拟 DB commit/rollback + 关连接之后）
        await gate.release(t1)
        assert gate.stats()["active"] == 0

    async def test_cancel_all_queued_keeps_active_intact(self, gate):
        act = []
        for _ in range(2):
            t = await gate.try_enqueue()
            await gate.acquire(t)
            act.append(t)
        queued = [await gate.try_enqueue() for _ in range(3)]
        assert gate.stats()["queued"] == 3

        info = await gate.cancel_all_queued()
        assert info["cancelled_queued"] == 3
        assert info["active_kept"] == 2
        assert gate.stats()["queued"] == 0
        assert gate.stats()["active"] == 2
        # 被取消的排队者 acquire 必须抛取消异常，不会转成执行
        for t in queued:
            with pytest.raises(UploadCancelledError):
                await gate.acquire(t)
        for t in act:
            await gate.release(t)

    async def test_cancel_interleaves_with_grant(self, gate):
        """取消与开始执行交错：已取消的排队者在名额释放后也绝不进入 ACTIVE。"""
        # 占满 2 个执行名额，t3 才是真正的排队者
        t1, t2 = await gate.try_enqueue(), await gate.try_enqueue()
        await gate.acquire(t1)
        await gate.acquire(t2)
        t3 = await gate.try_enqueue()
        acq = asyncio.create_task(gate.acquire(t3))
        await asyncio.sleep(0.05)
        assert not acq.done()

        # 先取消（确定性地先于名额释放），再归还执行名额
        assert await gate.request_cancel(t3) is True
        await gate.release(t1)

        with pytest.raises(UploadCancelledError):
            await asyncio.wait_for(acq, timeout=1)
        # t3 未占用执行名额；释放出的名额可被新请求获得
        assert gate.stats()["active"] == 1
        t4 = await gate.try_enqueue()
        await asyncio.wait_for(gate.acquire(t4), timeout=1)
        assert gate.stats()["active"] == 2
        await gate.release(t2)
        await gate.release(t4)
        assert gate.stats()["inflight"] == 0

    async def test_grant_interleaves_then_cancel_is_ignored(self, gate):
        """交错的另一侧：排队者已获名额进入 ACTIVE 后，取消不生效、不提前释放。"""
        t1, t2 = await gate.try_enqueue(), await gate.try_enqueue()
        await gate.acquire(t1)
        await gate.acquire(t2)
        t3 = await gate.try_enqueue()
        acq = asyncio.create_task(gate.acquire(t3))
        await asyncio.sleep(0.05)
        assert not acq.done()
        await gate.release(t1)
        await asyncio.wait_for(acq, timeout=1)  # t3 拿到名额
        assert await gate.request_cancel(t3) is False
        assert gate.stats()["active"] == 2
        await gate.release(t2)
        await gate.release(t3)
        assert gate.stats()["active"] == 0

    async def test_release_wakes_next_waiter_within_second(self, gate):
        # 先占满 2 个执行名额，后面 3 个才真正排队（总容量 5）
        holders = []
        for _ in range(2):
            t = await gate.try_enqueue()
            await gate.acquire(t)
            holders.append(t)
        waiters = []
        for _ in range(3):
            t = await gate.try_enqueue()
            waiters.append((t, asyncio.create_task(gate.acquire(t))))
        await asyncio.sleep(0.05)
        loop = asyncio.get_running_loop()
        start = loop.time()
        await gate.release(holders[0])
        await asyncio.wait_for(waiters[0][1], timeout=1)
        assert loop.time() - start < 1.0
        # 清理：先取消仍在排队的两个，再归还全部执行名额
        for t, task in waiters[1:]:
            await gate.request_cancel(t)
            with pytest.raises(UploadCancelledError):
                await asyncio.wait_for(task, timeout=1)
        await gate.release(waiters[0][0])
        await gate.release(holders[1])
        assert gate.stats()["inflight"] == 0
