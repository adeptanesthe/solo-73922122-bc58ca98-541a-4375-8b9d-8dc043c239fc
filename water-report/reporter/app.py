"""报送后台 Web 应用（FastAPI）。

职责：上传 CSV 建批、启动/停止/续跑浏览器报送、展示逐行状态与远端记录编号。
对旧站的所有操作都封装在 browser_worker 里，经真实浏览器完成。

并发设计（修复写锁等待卡死页面）：
- 建批是阻塞型 SQLite 写操作，遇锁时按 SQLite 原 30 秒 busy_timeout 忙等；
- 建批路由为 async，阻塞工作全部丢进**专用有界上传线程池**，事件循环不被占用，
  因此首页、批次进度轮询、停止操作在任何持锁期间都能即时响应；
- :mod:`reporter.upload_gate` 提供有界执行名额与排队容量，满额直接返回 503；
- 排队请求可取消（客户端断开或演示取消接口），取消后绝不建批；
  已开始的数据库操作必须完成提交或回滚并关闭连接才归还执行名额。
"""
from __future__ import annotations

import asyncio
import os
from pathlib import Path

from fastapi import FastAPI, File, Form, Request, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from . import db
from .browser_worker import ReportingWorker, WorkerManager
from .csvkit import CsvError, parse_csv
from .upload_gate import (
    BoundedUploadGate,
    GateFullError,
    UploadCancelledError,
)
from .upload_work import DemoLockHolder, UploadPool, persist_batch

DB_PATH = os.environ.get("REPORTER_DB", "/data/reporter.db")
UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", "/data/uploads"))
SHOT_DIR = Path(os.environ.get("SCREENSHOT_DIR", "/data/screenshots"))
LEGACY_URL = os.environ.get("LEGACY_URL", "http://legacy:8000")
HEADLESS = os.environ.get("HEADLESS", "1") != "0"
SLOW_MO = int(os.environ.get("SLOW_MO_MS", "0"))
BROWSER_CHANNEL = os.environ.get("BROWSER_CHANNEL") or None

# 上传建批的有界执行/排队容量（专用线程池大小与执行名额一致）
UPLOAD_MAX_ACTIVE = int(os.environ.get("UPLOAD_MAX_ACTIVE", "4"))
UPLOAD_MAX_QUEUED = int(os.environ.get("UPLOAD_MAX_QUEUED", "16"))

BASE_DIR = Path(__file__).parent
templates = Jinja2Templates(directory=str(BASE_DIR / "templates"))

# 必须在挂载 StaticFiles 前建好目录，否则全新数据卷首启会抛异常
UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
SHOT_DIR.mkdir(parents=True, exist_ok=True)

app = FastAPI(title="water-reporting-console")
app.mount("/screenshots", StaticFiles(directory=str(SHOT_DIR)),
          name="screenshots")

manager = WorkerManager()
gate = BoundedUploadGate(max_active=UPLOAD_MAX_ACTIVE,
                         max_queued=UPLOAD_MAX_QUEUED)
upload_pool = UploadPool(max_active=UPLOAD_MAX_ACTIVE)
demo_lock = DemoLockHolder(DB_PATH)


def reset_upload_runtime() -> None:
    """重建上传闸门/线程池/演示持锁器（主要供测试隔离使用）。

    同时重新读取环境变量中的路径与容量，使进程内切换数据目录生效。
    """
    global gate, upload_pool, demo_lock, DB_PATH, UPLOAD_DIR, SHOT_DIR
    global UPLOAD_MAX_ACTIVE, UPLOAD_MAX_QUEUED
    upload_pool.shutdown()
    DB_PATH = os.environ.get("REPORTER_DB", DB_PATH)
    UPLOAD_DIR = Path(os.environ.get("UPLOAD_DIR", str(UPLOAD_DIR)))
    SHOT_DIR = Path(os.environ.get("SCREENSHOT_DIR", str(SHOT_DIR)))
    UPLOAD_MAX_ACTIVE = int(os.environ.get("UPLOAD_MAX_ACTIVE",
                                           str(UPLOAD_MAX_ACTIVE)))
    UPLOAD_MAX_QUEUED = int(os.environ.get("UPLOAD_MAX_QUEUED",
                                           str(UPLOAD_MAX_QUEUED)))
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    SHOT_DIR.mkdir(parents=True, exist_ok=True)
    gate = BoundedUploadGate(max_active=UPLOAD_MAX_ACTIVE,
                             max_queued=UPLOAD_MAX_QUEUED)
    upload_pool = UploadPool(max_active=UPLOAD_MAX_ACTIVE)
    demo_lock = DemoLockHolder(DB_PATH)


def make_worker() -> ReportingWorker:
    return ReportingWorker(
        base_url=LEGACY_URL,
        db_path=DB_PATH,
        screenshot_dir=str(SHOT_DIR),
        headless=HEADLESS,
        slow_mo_ms=SLOW_MO,
        channel=BROWSER_CHANNEL,
    )


@app.on_event("startup")
def _startup() -> None:
    db.init_db(DB_PATH)
    conn = db.connect(DB_PATH)
    try:
        recovered = db.recover_stale_runs(conn)
        if recovered:
            print(f"[startup] 恢复上次异常退出：{recovered}（SAVING 行已转待核实）")
    finally:
        conn.close()


@app.on_event("shutdown")
def _shutdown() -> None:
    # 已在执行的建批任务由各自连接走完提交/回滚；进程退出不影响 SQLite 事务安全
    upload_pool.shutdown()


def _retryable_503(request: Request, message: str):
    """可重试的服务繁忙：浏览器展示错误页，机器可读 Retry-After。"""
    resp = templates.TemplateResponse(
        request, "error.html",
        {"message": f"{message}（可稍后重试）"}, status_code=503)
    resp.headers["Retry-After"] = "5"
    return resp


def _row_view(conn, row) -> dict:
    return {
        **dict(row),
        "status_label": db.ROW_STATUS_LABELS.get(row["status"], row["status"]),
    }


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    conn = db.connect(DB_PATH)
    try:
        batches = []
        for b in db.list_batches(conn):
            counts = db.row_counts(conn, b["id"])
            batches.append({
                **dict(b),
                "counts": counts,
                "total": sum(counts.values()),
                "busy": manager.busy and manager.worker
                and manager.worker.snapshot().get("batch_id") == b["id"],
            })
        return templates.TemplateResponse(
            request, "index.html",
            {"batches": batches, "busy": manager.busy,
             "legacy_url": LEGACY_URL,
             "status_labels": db.ROW_STATUS_LABELS,
             "gate": gate.stats(), "demo": demo_lock.snapshot()},
        )
    finally:
        conn.close()


@app.post("/batches")
async def create_batch(request: Request, file: UploadFile = File(...)):
    # 读取与 CSV 校验在事件循环外影响小且需要快速返回 400，保持原语义
    content = await file.read()
    if not content:
        return templates.TemplateResponse(
            request, "error.html", {"message": "上传文件为空"}, status_code=400)
    try:
        csv_rows = parse_csv(content)
    except CsvError as exc:
        return templates.TemplateResponse(
            request, "error.html",
            {"message": f"CSV 校验失败：{exc}"}, status_code=400)

    # 有界容量登记：执行+排队满额立即 503，不等待
    try:
        token = await gate.try_enqueue()
    except GateFullError as exc:
        return _retryable_503(request, f"上传容量已满：{exc}")

    # 客户端在排队等待期间断开 -> 取消排队，绝不建批；
    # 进入执行阶段后 watcher 随即被 cancel，断开不再影响在途数据库操作
    async def _watch_disconnect() -> None:
        while True:
            if await request.is_disconnected():
                await gate.request_cancel(token)
                return
            await asyncio.sleep(0.2)

    watcher = asyncio.create_task(_watch_disconnect())
    try:
        try:
            # 排队等执行名额；排队中被取消（断开/管理取消）抛 UploadCancelledError
            await gate.acquire(token)
        except UploadCancelledError:
            return _retryable_503(
                request, "上传请求在排队阶段被取消，未创建批次")

        # 已拿到执行名额：数据库操作即将/已经开始，取消不再生效，
        # 必须等任务完成 commit/rollback 并关闭连接后才 release。
        filename = Path(file.filename or "upload.csv").name
        try:
            result = await upload_pool.run_async(
                persist_batch, DB_PATH, UPLOAD_DIR, filename,
                content, csv_rows)
        except db.LockTimeout as exc:
            # 30 秒忙等仍未拿到写锁：未登记批次，返回可重试 503
            return _retryable_503(
                request, f"建批等待数据库写锁超时：{exc}")
        return RedirectResponse(url=f"/batches/{result.batch_id}",
                                status_code=303)
    finally:
        watcher.cancel()
        # 排队取消时 token 已不在 ACTIVE；ACTIVE 任务在此归还执行名额
        await gate.release(token)


def _get_batch_or_none(conn, batch_id: int):
    batch = db.get_batch(conn, batch_id)
    if batch is None:
        return None, None
    rows = [_row_view(conn, r) for r in db.get_rows(conn, batch_id)]
    return batch, rows


@app.get("/batches/{batch_id}", response_class=HTMLResponse)
def batch_detail(request: Request, batch_id: int):
    conn = db.connect(DB_PATH)
    try:
        batch, rows = _get_batch_or_none(conn, batch_id)
        if batch is None:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"批次 {batch_id} 不存在"}, status_code=404)
        lock = db.current_lock(conn)
        return templates.TemplateResponse(
            request, "batch.html",
            {
                "b": batch, "rows": rows,
                "counts": db.row_counts(conn, batch_id),
                "status_labels": db.ROW_STATUS_LABELS,
                "busy": manager.busy,
                "lock_batch": lock["batch_id"] if lock else None,
                "actionable": db.ACTIONABLE,
            },
        )
    finally:
        conn.close()


@app.post("/batches/{batch_id}/start")
def start_batch(request: Request, batch_id: int, resume: str = Form("0")):
    do_resume = resume == "1"
    conn = db.connect(DB_PATH)
    try:
        batch = db.get_batch(conn, batch_id)
        if batch is None:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"批次 {batch_id} 不存在"}, status_code=404)
        if manager.busy or db.current_lock(conn):
            return templates.TemplateResponse(
                request, "error.html",
                {"message": "已有报送任务在运行，每批同一时刻只允许一份；"
                            "请停止当前任务后再操作。"}, status_code=409)
        if batch["status"] == db.BATCH_DONE and not do_resume:
            return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)
        if do_resume:
            pending_rows = [r for r in db.get_rows(conn, batch_id)
                            if r["status"] in db.ACTIONABLE]
            if not pending_rows:
                return RedirectResponse(url=f"/batches/{batch_id}",
                                        status_code=303)
    finally:
        conn.close()

    try:
        manager.start(make_worker(), batch_id, do_resume)
    except RuntimeError as exc:
        return templates.TemplateResponse(
            request, "error.html", {"message": str(exc)}, status_code=409)
    return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)


@app.post("/batches/{batch_id}/stop")
def stop_batch(batch_id: int):
    # 纯内存操作（置停止事件），不触碰 SQLite，持锁期间同样即时响应
    manager.stop_current()
    return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)


@app.post("/batches/{batch_id}/rows/{row_id}/recheck")
def recheck_row(request: Request, batch_id: int, row_id: int):
    """人工在旧站处理完冲突后，把行重置为待核实，续跑时仍会先查询再决定。"""
    conn = db.connect(DB_PATH)
    try:
        row = db.get_row(conn, row_id)
        if row is None or row["batch_id"] != batch_id:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": "行记录不存在"}, status_code=404)
        if manager.busy or db.current_lock(conn):
            return templates.TemplateResponse(
                request, "error.html",
                {"message": "报送任务运行中，请先停止再重置行状态。"},
                status_code=409)
        if row["status"] != db.CONFLICT:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": f"仅冲突行可重置，当前状态 {row['status']}"},
                status_code=400)
        db.update_row(conn, row_id, status=db.UNVERIFIED, remote_id=None,
                      screenshot=None,
                      message="人工处理后重置为待核实，续跑将先查询核实")
        db.add_event(conn, row_id, "RESET_FOR_RECHECK",
                     "冲突行经人工处理，重置为待核实")
        db.set_batch_status(
            conn, batch_id, db.BATCH_STOPPED,
            note=f"[{db.now()}] 行 {row['seq']} 重置待核实，可续跑；")
    finally:
        conn.close()
    return RedirectResponse(url=f"/batches/{batch_id}", status_code=303)


@app.get("/api/batches/{batch_id}")
def api_batch(batch_id: int):
    conn = db.connect(DB_PATH)
    try:
        batch, rows = _get_batch_or_none(conn, batch_id)
        if batch is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        snap = manager.worker.snapshot() if manager.worker else {}
        return JSONResponse({
            "batch": dict(batch),
            "busy": manager.busy,
            "current_seq": snap.get("row_seq") if manager.busy else None,
            "counts": db.row_counts(conn, batch_id),
            "rows": rows,
        })
    finally:
        conn.close()


@app.get("/batches/{batch_id}/rows/{row_id}/events", response_class=HTMLResponse)
def row_events(request: Request, batch_id: int, row_id: int):
    conn = db.connect(DB_PATH)
    try:
        row = db.get_row(conn, row_id)
        if row is None or row["batch_id"] != batch_id:
            return templates.TemplateResponse(
                request, "error.html",
                {"message": "行记录不存在"}, status_code=404)
        events = db.get_events(conn, row_id)
        return templates.TemplateResponse(
            request, "events.html",
            {"b": db.get_batch(conn, batch_id), "row": row,
             "events": events,
             "status_label": db.ROW_STATUS_LABELS.get(row["status"],
                                                     row["status"])},
        )
    finally:
        conn.close()


# --------------------------------------------------------------------------
# 演示控制接口（不参与报送业务，仅用于可控演练持锁/取消/执行交错）
# --------------------------------------------------------------------------
@app.post("/__demo/hold-lock")
def demo_hold_lock(request: Request, seconds: float = Form(8.0)):
    info = demo_lock.hold(seconds)
    return JSONResponse({**info, "gate": gate.stats()})


@app.post("/__demo/cancel-queued")
async def demo_cancel_queued(request: Request):
    info = await gate.cancel_all_queued()
    return JSONResponse(info)


@app.get("/__demo/stats")
def demo_stats(request: Request):
    return JSONResponse({"gate": gate.stats(), "lock": demo_lock.snapshot()})
