"""HTTP 级并发实测：持锁窗口内 40 份并发上传的响应性、容量、取消与锁超时。

启动**真实 uvicorn 服务**（而非 TestClient 同步门户），用线程池 + httpx
打真实并发流量，才能准确测量事件循环不被阻塞时的页面响应延迟。
默认快速；设 RUN_SLOW_LOCK_TEST=1 时额外跑超过 30 秒忙等的锁超时用例。
"""
from __future__ import annotations

import os
import socket
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
import uvicorn

from reporter import db  # noqa: E402

# 每个测试函数使用独立临时数据目录，避免批次累积与状态残留
_ROOT_TMP = Path(__file__).resolve().parents[1] / "data" / "_concurrency_tmp"
_ROOT_TMP.mkdir(parents=True, exist_ok=True)


def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@pytest.fixture()
def server(tmp_path):
    # 在导入 app 前注入本用例专属路径与容量配置
    db_path = tmp_path / "reporter.db"
    upload_dir = tmp_path / "uploads"
    shot_dir = tmp_path / "screenshots"
    upload_dir.mkdir(parents=True, exist_ok=True)
    shot_dir.mkdir(parents=True, exist_ok=True)
    os.environ["REPORTER_DB"] = str(db_path)
    os.environ["UPLOAD_DIR"] = str(upload_dir)
    os.environ["SCREENSHOT_DIR"] = str(shot_dir)
    os.environ["UPLOAD_MAX_ACTIVE"] = "4"
    os.environ["UPLOAD_MAX_QUEUED"] = "16"

    from reporter import db as db_mod
    from reporter.app import app, reset_upload_runtime
    reset_upload_runtime()
    from reporter.app import demo_lock, gate
    # startup 事件只在首次启动触发，这里对新路径显式建表并做崩溃恢复
    db_mod.init_db(os.environ["REPORTER_DB"])
    port = _free_port()
    config = uvicorn.Config(app, host="127.0.0.1", port=port,
                            log_level="warning")
    uv_server = uvicorn.Server(config)
    thread = threading.Thread(target=uv_server.run, daemon=True)
    thread.start()
    base = f"http://127.0.0.1:{port}"
    _wait_ready(base)
    yield base
    uv_server.should_exit = True
    thread.join(timeout=10)
    # 确保没有残留持锁/在途请求影响下一个用例
    assert not demo_lock.snapshot()["lock_held"]
    assert gate.stats()["inflight"] == 0


def _wait_ready(base: str) -> None:
    """确认 startup 完成（建表结束）：统计接口可用且首页 200。"""
    deadline = time.monotonic() + 15
    with httpx.Client(timeout=5) as c:
        while time.monotonic() < deadline:
            try:
                if c.get(base + "/").status_code == 200 and \
                        c.get(base + "/__demo/stats").status_code == 200:
                    return
            except httpx.HTTPError:
                pass
            time.sleep(0.1)
    raise RuntimeError("服务未就绪")


def _csv(idx: int) -> tuple[str, bytes]:
    name = f"conc-{idx}-{time.time_ns()}.csv"
    body = (
        "样本号,项目,结果\n"
        f"C{idx:04d}-1,pH,7.{idx % 10}\n"
        f"C{idx:04d}-2,COD,{idx % 50}.0\n"
    ).encode()
    return name, body


def _post_upload(base: str, idx: int) -> tuple[int, float]:
    name, body = _csv(idx)
    with httpx.Client(timeout=60) as c:
        t0 = time.monotonic()
        resp = c.post(base + "/batches",
                      files={"file": (name, body, "text/csv")},
                      follow_redirects=False)
        return resp.status_code, time.monotonic() - t0


def _batch_count() -> int:
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        conn = sqlite3.connect(os.environ["REPORTER_DB"])
        try:
            return conn.execute(
                "SELECT COUNT(*) FROM batches").fetchone()[0]
        except sqlite3.OperationalError:
            time.sleep(0.1)  # startup 的 init_db 尚未建表
        finally:
            conn.close()
    raise RuntimeError("batches 表在 10 秒内未就绪")


def _wait_unlock(base: str, timeout: float = 12) -> None:
    deadline = time.monotonic() + timeout
    with httpx.Client(timeout=5) as c:
        while time.monotonic() < deadline:
            info = c.get(base + "/__demo/stats").json()
            if not info["lock"]["lock_held"]:
                return
            time.sleep(0.1)


def _wait_gate_inflight(base: str, expected: int, timeout: float = 20) -> dict:
    deadline = time.monotonic() + timeout
    last = {}
    with httpx.Client(timeout=5) as c:
        while time.monotonic() < deadline:
            payload = c.get(base + "/__demo/stats").json()
            last = payload["gate"]
            if last["inflight"] >= expected:
                return last
            if not payload["lock"]["lock_held"]:
                raise AssertionError(
                    f"持锁已提前释放，闸门仅到 {last}（期望在途 {expected}）")
            time.sleep(0.1)
        raise AssertionError(f"闸门在 {timeout}s 内未达到在途 {expected}：{last}")


class TestUploadConcurrency:
    def test_full_capacity_503_and_queued_cancel_creates_no_batch(
            self, server):
        base = server
        before = _batch_count()
        with httpx.Client(timeout=30) as c:
            r = c.post(base + "/__demo/hold-lock", data={"seconds": 25})
            assert r.json()["lock_held"] is True

        with ThreadPoolExecutor(max_workers=20) as pool:
            futures = [pool.submit(_post_upload, base, i)
                       for i in range(20)]
            stats = _wait_gate_inflight(base, 20)  # 4 执行 + 16 排队
            assert stats["active"] == 4 and stats["queued"] == 16
            # 确认整个测量期间持锁仍在（统计接口给足超时，事件循环偶发繁忙）
            lock_payload = httpx.get(base + "/__demo/stats",
                                     timeout=15).json()
            assert lock_payload["lock"]["lock_held"]
            # 总容量 20 已满：第 21 个请求立即 503
            code, elapsed = _post_upload(base, 999)
            assert code == 503 and elapsed < 1.0

            # 取消接口即时返回
            with httpx.Client(timeout=10) as c:
                t0 = time.monotonic()
                info = c.post(base + "/__demo/cancel-queued").json()
                cancel_call = time.monotonic() - t0
            assert info["cancelled_queued"] == 16
            assert cancel_call < 1.0

            # 被取消的 16 个排队请求应在 1 秒级内全部收到 503
            t_cancel = time.monotonic()
            done_cancel = 0
            deadline = time.monotonic() + 3
            while time.monotonic() < deadline:
                done_cancel = sum(1 for f in futures if f.done())
                if done_cancel >= 16:
                    break
                time.sleep(0.05)
            cancel_elapsed = time.monotonic() - t_cancel
            assert done_cancel >= 16
            assert cancel_elapsed < 1.5
        # 4 个执行请求在锁释放后才能完成：先等锁释放，再收结果
        _wait_unlock(base, timeout=30)
        results = [f.result(timeout=15) for f in futures]
        codes = [c for c, _ in results]
        assert codes.count(503) == 16   # 排队被取消
        assert codes.count(303) == 4    # 执行中者锁释放后建批
        assert _batch_count() - before == 4

    def test_40_locked_uploads_pages_progress_stop_respond_within_1s(
            self, server):
        base = server
        # 先建一个批次用于进度/停止测量
        code, _ = _post_upload(base, 700)
        assert code == 303
        existing_id = _batch_count()  # 最新批次 id 即当前总数

        with httpx.Client(timeout=30) as c:
            c.post(base + "/__demo/hold-lock", data={"seconds": 25})
        try:
            with ThreadPoolExecutor(max_workers=40) as pool:
                futures = [pool.submit(_post_upload, base, 800 + i)
                           for i in range(40)]
                # 等到闸门占满（4 执行 + 16 排队），其余 20 个已快速 503
                _wait_gate_inflight(base, 20)
                time.sleep(0.5)  # 让满额拒绝也全部返回

                latencies = []
                with httpx.Client(timeout=10) as c:
                    for _ in range(5):
                        t0 = time.monotonic()
                        r1 = c.get(base + "/")
                        latencies.append(("index", time.monotonic() - t0,
                                          r1.status_code))
                        t0 = time.monotonic()
                        r2 = c.get(base + f"/api/batches/{existing_id}")
                        latencies.append(("progress", time.monotonic() - t0,
                                          r2.status_code))
                        t0 = time.monotonic()
                        r3 = c.post(base + f"/batches/{existing_id}/stop")
                        latencies.append(("stop", time.monotonic() - t0,
                                          r3.status_code))
                results = [f.result(timeout=40) for f in futures]
        finally:
            _wait_unlock(base)

        for label, latency, code in latencies:
            assert code in (200, 303), (label, code)
            assert latency < 1.0, (label, latency)
        print("\n持锁 40 并发期间各请求最大延迟:")
        for label in ("index", "progress", "stop"):
            mx = max(l for n, l, _ in latencies if n == label)
            print(f"  {label:8s} max={mx * 1000:.0f}ms")

        codes = sorted(c for c, _ in results)
        assert codes.count(303) == 20   # 4 执行 + 16 排队，锁释放后成功
        assert codes.count(503) == 20   # 满额立即拒绝
        # 503 必须可重试：锁释放后新上传成功
        code, _ = _post_upload(base, int(time.time()) % 1000)
        assert code == 303

    def test_client_disconnect_while_queued_creates_no_batch(self, server):
        """客户端在排队阶段断开：服务端检测到后自动取消，绝不建批。"""
        import socket as socket_mod

        base = server
        host, port = base.replace("http://", "").split(":")
        port = int(port)
        before = _batch_count()
        with httpx.Client(timeout=30) as c:
            c.post(base + "/__demo/hold-lock", data={"seconds": 20})

        # 占满 4 个执行名额，后续请求才会真正排队
        holders = []
        with ThreadPoolExecutor(max_workers=4) as pool:
            for i in range(4):
                holders.append(pool.submit(_post_upload, base, 600 + i))
            _wait_gate_inflight(base, 4)

            # 用原生 socket 发一个上传请求，入队后直接关闭连接模拟客户端断开
            name, body = _csv(666)
            boundary = "----disconnect-demo"
            payload = (
                f"--{boundary}\r\n"
                f'Content-Disposition: form-data; name="file"; '
                f'filename="{name}"\r\n'
                "Content-Type: text/csv\r\n\r\n"
            ).encode() + body + f"\r\n--{boundary}--\r\n".encode()
            request = (
                "POST /batches HTTP/1.1\r\n"
                f"Host: {host}:{port}\r\n"
                f"Content-Type: multipart/form-data; boundary={boundary}\r\n"
                f"Content-Length: {len(payload)}\r\n"
                "Connection: close\r\n\r\n"
            ).encode() + payload

            sock = socket_mod.create_connection((host, port), timeout=10)
            sock.sendall(request)
            # 等服务端把该请求放入排队
            deadline = time.monotonic() + 10
            with httpx.Client(timeout=5) as c:
                while time.monotonic() < deadline:
                    g = c.get(base + "/__demo/stats").json()["gate"]
                    if g["queued"] >= 1:
                        break
                    time.sleep(0.1)
                queued_before = c.get(base + "/__demo/stats").json()["gate"]
                assert queued_before["queued"] == 1
            # 客户端断开：不调用任何管理取消接口，只关 socket
            sock.close()

            # 服务端 _watch_disconnect 应自动取消排队，名额释放
            deadline = time.monotonic() + 10
            with httpx.Client(timeout=5) as c:
                while time.monotonic() < deadline:
                    g = c.get(base + "/__demo/stats").json()["gate"]
                    if g["queued"] == 0:
                        break
                    time.sleep(0.1)
                g = c.get(base + "/__demo/stats").json()["gate"]
                assert g["queued"] == 0
                assert g["active"] == 4  # 执行者不受影响
            [f.result(timeout=30) for f in holders]
        _wait_unlock(base, timeout=30)
        # 断开的排队请求未建批：仅 4 个执行者登记
        assert _batch_count() - before == 4

    def test_lock_timeout_returns_retryable_503_and_registers_no_batch(
            self, server):
        if not os.environ.get("RUN_SLOW_LOCK_TEST"):
            pytest.skip("设 RUN_SLOW_LOCK_TEST=1 启用 30 秒以上的慢用例")
        base = server
        before = _batch_count()
        with httpx.Client(timeout=30) as c:
            c.post(base + "/__demo/hold-lock", data={"seconds": 35})
        code, elapsed = _post_upload(base, 900)
        assert code == 503
        assert 30.0 <= elapsed <= 34.0
        assert _batch_count() == before
        _wait_unlock(base, timeout=40)

    def test_retry_after_unlock_succeeds_with_pending_rows(self, server):
        base = server
        idx = int(time.time()) % 1000
        name, body = _csv(idx)
        with httpx.Client(timeout=30) as c:
            resp = c.post(base + "/batches",
                          files={"file": (name, body, "text/csv")},
                          follow_redirects=False)
        assert resp.status_code == 303
        bid = int(resp.headers["location"].rstrip("/").split("/")[-1])
        detail = httpx.get(base + f"/api/batches/{bid}",
                           timeout=10).json()
        assert len(detail["rows"]) == 2
        assert all(r["status"] == db.PENDING for r in detail["rows"])
