"""上传建批的阻塞工作部分（运行在专用有界线程池，绝不占用事件循环）。

约定：
- 路由拿到闸门的执行名额后才把任务提交到这里，线程池大小与执行名额一致，
  因此线程池本身不会再形成隐性排队；
- 任务一旦开始数据库操作，就必须走完 commit/rollback 并关闭连接才返回，
  取消不会打断它，执行名额也不会提前释放；
- SQLite 连接保留 30 秒 busy_timeout 忙等待（见 db.connect），
  超时由 db.LockTimeout 表达，路由转成可重试的 503 且不登记批次。
"""
from __future__ import annotations

import asyncio
import functools
import os
import sqlite3
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

from . import db
from .csvkit import CsvRow


@dataclass
class BatchResult:
    batch_id: int
    saved_name: str


def _atomic_save(upload_dir: Path, filename: str,
                 content: bytes) -> Path:
    """独占式落盘，避免并发同名文件互相覆盖。"""
    upload_dir.mkdir(parents=True, exist_ok=True)
    base = Path(filename).name or "upload.csv"
    stem, suffix = Path(base).stem, Path(base).suffix
    candidate = upload_dir / base
    n = 1
    while True:
        try:
            fd = os.open(str(candidate),
                         os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
        except FileExistsError:
            candidate = upload_dir / f"{stem}-{n}{suffix}"
            n += 1
            continue
        try:
            with os.fdopen(fd, "wb") as fh:
                fh.write(content)
        except Exception:
            candidate.unlink(missing_ok=True)
            raise
        return candidate


def persist_batch(db_path: str, upload_dir: Path, filename: str,
                  content: bytes, csv_rows: list[CsvRow]) -> BatchResult:
    """线程池任务：落盘 + 单事务建批。

    无论成功、锁超时还是其他异常，都保证事务有 commit/rollback、
    连接被关闭；锁超时时删除未落库的上传文件，避免孤儿文件。
    """
    saved_path = _atomic_save(upload_dir, filename, content)
    conn = db.connect(db_path)
    try:
        batch_id = db.create_batch_with_rows(conn, saved_path.name, csv_rows)
        return BatchResult(batch_id=batch_id, saved_name=saved_path.name)
    except db.LockTimeout:
        # 未登记批次：把刚落盘的文件清掉，保持数据目录与库一致
        saved_path.unlink(missing_ok=True)
        raise
    except Exception:
        try:
            conn.rollback()
        except sqlite3.Error:
            pass
        saved_path.unlink(missing_ok=True)
        raise
    finally:
        conn.close()


class UploadPool:
    """与闸门执行名额等大的专用线程池，页面/停止等请求走默认 anyio 线程池。"""

    def __init__(self, max_active: int):
        self._executor = ThreadPoolExecutor(
            max_workers=max_active,
            thread_name_prefix="upload-db",
        )

    def submit(self, fn, *args, **kwargs):
        return self._executor.submit(fn, *args, **kwargs)

    async def run_async(self, fn, *args, **kwargs):
        """把阻塞任务放进专用线程池并 await 其完成（不阻塞事件循环）。"""
        loop = asyncio.get_running_loop()
        future = self._executor.submit(
            functools.partial(fn, *args, **kwargs))
        return await loop.run_in_executor(None, future.result)

    def shutdown(self) -> None:
        self._executor.shutdown(wait=False, cancel_futures=True)


class DemoLockHolder:
    """演示用：在独立守护线程里持有 SQLite 写锁指定秒数。

    只用于制造可控的"建批遇锁"场景，与上传线程池完全隔离，
    不占用任何上传执行/排队名额。
    """

    def __init__(self, db_path: str):
        self._db_path = db_path
        self._lock = threading.Lock()
        self._conn: sqlite3.Connection | None = None
        self._thread: threading.Thread | None = None
        self._until = 0.0
        self._started_at = 0.0
        self._seconds = 0.0

    def hold(self, seconds: float) -> dict:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return self.snapshot(held=True,
                                     detail="已有持锁演示进行中")
            seconds = max(1.0, min(float(seconds), 90.0))
            try:
                conn = db.connect(self._db_path)
                # WAL 下立即取得保留写锁，其它写事务进入 busy_timeout 忙等
                conn.execute("BEGIN IMMEDIATE")
            except sqlite3.OperationalError as exc:
                return self.snapshot(
                    held=False,
                    detail=f"取锁失败（可能已有写事务）：{exc}")
            self._conn = conn
            self._started_at = time.monotonic()
            self._seconds = seconds
            self._until = self._started_at + seconds
            t = threading.Thread(target=self._hold_then_release,
                                 name="demo-lock-holder", daemon=True)
            self._thread = t
            t.start()
            return self.snapshot(held=True,
                                 detail=f"写锁将持有 {seconds:g} 秒")

    def _hold_then_release(self) -> None:
        try:
            while time.monotonic() < self._until:
                time.sleep(min(0.2, max(0.0, self._until - time.monotonic())))
        finally:
            with self._lock:
                conn, self._conn = self._conn, None
            if conn is not None:
                try:
                    conn.rollback()
                except sqlite3.Error:
                    pass
                finally:
                    conn.close()

    def snapshot(self, held: bool | None = None,
                 detail: str = "") -> dict:
        alive = bool(self._thread and self._thread.is_alive())
        if held is None:
            held = alive
        remaining = max(0.0, self._until - time.monotonic()) if alive else 0.0
        return {
            "lock_held": held,
            "detail": detail,
            "hold_seconds": self._seconds,
            "remaining_seconds": round(remaining, 2),
        }
