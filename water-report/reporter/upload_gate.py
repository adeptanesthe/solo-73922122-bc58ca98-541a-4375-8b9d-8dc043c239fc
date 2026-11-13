"""上传建批的有界并发闸门：有界执行名额 + 有界排队 + 可取消。

并发模型（全部在 FastAPI 事件循环内协作式调度，无线程阻塞）：

- ``max_active`` 个执行名额：只有拿到执行名额的请求才会被提交到专用上传线程池
  开始文件/数据库操作；
- ``max_queued`` 个排队名额：执行名额满时在队列里等待，执行 + 排队总额满时
  ``try_enqueue`` 立即抛 :class:`GateFullError`，由路由返回 503；
- 排队中的请求可以被取消（客户端断开连接、演示取消接口）：取消后立即失败，
  保证**不会再建批**；
- 已经拿到执行名额的请求视为"数据库操作已开始"，取消只打标记、不打断执行，
  必须等其完成提交或回滚并关闭连接后，由路由调用 :meth:`release` 归还名额，
  取消不会提前释放执行名额。
"""
from __future__ import annotations

import asyncio
import enum
import itertools
from dataclasses import dataclass, field


class TokenState(str, enum.Enum):
    QUEUED = "QUEUED"        # 排队中，占用排队名额
    ACTIVE = "ACTIVE"        # 已拿执行名额，数据库操作进行中（不可取消）
    DONE = "DONE"            # 正常结束并归还名额
    CANCELLED = "CANCELLED"  # 排队阶段被取消，未进入执行


class GateFullError(RuntimeError):
    """执行名额与排队名额均已满，路由应返回 503。"""


class UploadCancelledError(RuntimeError):
    """请求在排队阶段被取消（客户端断开或管理取消），绝不允许建批。"""

    def __init__(self, seq: int):
        super().__init__(f"排队中的上传请求 #{seq} 已取消")
        self.seq = seq


@dataclass
class _Token:
    seq: int
    state: TokenState = TokenState.QUEUED
    queued: bool = True
    active: bool = False
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)


# 等待条件变量的唤醒上限：即便通知丢失，取消/授名额也最多滞后该时长，
# 远小于页面 1 秒响应要求。
_WAIT_TICK_SECONDS = 0.25


class BoundedUploadGate:
    def __init__(self, max_active: int, max_queued: int):
        if max_active < 1:
            raise ValueError("max_active 至少为 1")
        if max_queued < 0:
            raise ValueError("max_queued 不能为负")
        self.max_active = max_active
        self.max_queued = max_queued
        self._active = 0
        self._queued = 0
        self._cond = asyncio.Condition()
        self._counter = itertools.count(1)
        self._tokens: dict[int, _Token] = {}

    # ---------- 观测 ----------
    def stats(self) -> dict:
        """同步快照（事件循环单线程内读写，无需加锁）。"""
        return {
            "max_active": self.max_active,
            "max_queued": self.max_queued,
            "active": self._active,
            "queued": self._queued,
            "inflight": self._active + self._queued,
            "capacity": self.max_active + self.max_queued,
        }

    # ---------- 入队 ----------
    async def try_enqueue(self) -> _Token:
        """登记一个上传请求；执行+排队满额时抛 GateFullError（不阻塞）。"""
        async with self._cond:
            if self._active + self._queued >= self.max_active + self.max_queued:
                raise GateFullError(
                    f"上传容量已满（执行 {self._active}/{self.max_active}，"
                    f"排队 {self._queued}/{self.max_queued}）"
                )
            token = _Token(seq=next(self._counter))
            self._tokens[token.seq] = token
            self._queued += 1
            self._cond.notify_all()
            return token

    # ---------- 排队等执行名额 ----------
    async def acquire(self, token: _Token) -> None:
        """排队等待执行名额。

        排队期间被取消 -> :class:`UploadCancelledError`，调用方不得建批；
        一旦成功返回，token 进入 ACTIVE，后续取消不再生效。
        """
        async with self._cond:
            while True:
                if token.state is TokenState.CANCELLED:
                    raise UploadCancelledError(token.seq)
                if token.state is TokenState.QUEUED and self._active < self.max_active:
                    # 释放排队名额、占用执行名额（总额度占用数不变）
                    token.state = TokenState.ACTIVE
                    token.queued = False
                    token.active = True
                    self._queued -= 1
                    self._active += 1
                    self._cond.notify_all()
                    return
                try:
                    await asyncio.wait_for(
                        self._cond.wait(), timeout=_WAIT_TICK_SECONDS)
                except asyncio.TimeoutError:
                    pass

    # ---------- 归还 ----------
    async def release(self, token: _Token) -> None:
        """数据库操作走完提交/回滚并关闭连接后才调用，归还执行名额。"""
        async with self._cond:
            if token.active:
                token.active = False
                token.state = TokenState.DONE
                self._active -= 1
                self._tokens.pop(token.seq, None)
                self._cond.notify_all()

    # ---------- 取消 ----------
    async def request_cancel(self, token: _Token) -> bool:
        """取消单个请求。

        :return: True=排队中已取消（立即释放排队名额）；
                 False=已开始执行，仅打标记，不打断、不提前释放执行名额。

        取消的 token 保留在登记表里（状态为 CANCELLED）：任何后进入等待的
        acquire 都会先看到取消状态而失败，杜绝"取消与授名额交错"时被误授名额。
        """
        async with self._cond:
            token.cancel_event.set()
            if token.state is TokenState.QUEUED:
                token.state = TokenState.CANCELLED
                token.queued = False
                self._queued -= 1
                self._cond.notify_all()
                return True
            return False

    async def cancel_all_queued(self) -> dict:
        """取消全部排队请求；已在执行的请求不受影响、名额不提前释放。"""
        async with self._cond:
            queued = [t for t in self._tokens.values()
                      if t.state is TokenState.QUEUED]
            for t in queued:
                t.state = TokenState.CANCELLED
                t.queued = False
                t.cancel_event.set()
            n = len(queued)
            self._queued -= n
            self._cond.notify_all()
            return {
                "cancelled_queued": n,
                "active_kept": self._active,
                **self.stats(),
            }
