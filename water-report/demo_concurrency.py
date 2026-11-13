#!/usr/bin/env python3
"""上传并发/锁等待/取消的可控演示脚本（仅用标准库，针对运行中的服务）。

用法：
    python demo_concurrency.py [REPORTER_BASE_URL]
默认 http://127.0.0.1:8010（run-local.sh 的控制台端口）。

演示阶段：
  1. 正常上传一份，确认基线可用；
  2. 打开持锁窗口（默认 12 秒），并发发起 24 份上传：
     - 前 UPLOAD_MAX_ACTIVE 份进入执行并在 SQLite 30 秒忙等内等锁；
     - 接下来 UPLOAD_MAX_QUEUED 份排队；
     - 超出总容量的请求立即收到可重试 503；
  3. 持锁期间持续测量首页 / 进度接口 / 停止操作的响应延迟（要求 <1s）；
  4. 在持锁窗口内"取消全部排队请求"，验证排队者快速 503 且不登记批次；
  5. 锁释放后，执行中的请求完成建批（取消未提前释放名额的闭环）；
  6. 打印批次增量与闸门最终状态。
"""
from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor

BASE = sys.argv[1].rstrip("/") if len(sys.argv) > 1 else "http://127.0.0.1:8010"


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *a, **k):
        return None  # 不自动跟随，保留 303 状态码供判断


_OPENER = urllib.request.build_opener(_NoRedirect)


def _request(method: str, path: str, data: bytes | None = None,
             content_type: str | None = None, timeout: float = 40):
    req = urllib.request.Request(BASE + path, data=data, method=method)
    if content_type:
        req.add_header("Content-Type", content_type)
    try:
        with _OPENER.open(req, timeout=timeout) as resp:
            return resp.status, resp.read(), dict(resp.headers)
    except urllib.error.HTTPError as exc:
        return exc.code, exc.read(), dict(exc.headers)


def stats() -> dict:
    return json.loads(_request("GET", "/__demo/stats", timeout=10)[1])


def hold_lock(seconds: float) -> dict:
    body = f"seconds={seconds}".encode()
    return json.loads(_request(
        "POST", "/__demo/hold-lock", body,
        "application/x-www-form-urlencoded")[1])


def cancel_queued() -> dict:
    return json.loads(_request("POST", "/__demo/cancel-queued", b"",
                               "application/x-www-form-urlencoded")[1])


def upload(idx: int) -> tuple[int, float]:
    boundary = f"----demo{idx}-{time.time_ns()}"
    content = (
        "样本号,项目,结果\n"
        f"D{idx:04d}-1,pH,7.{idx % 10}\n"
        f"D{idx:04d}-2,COD,{idx % 40}.0\n"
    ).encode()
    body = (
        f"--{boundary}\r\n"
        f'Content-Disposition: form-data; name="file"; '
        f'filename="demo-{idx}.csv"\r\n'
        "Content-Type: text/csv\r\n\r\n"
    ).encode() + content + f"\r\n--{boundary}--\r\n".encode()
    t0 = time.monotonic()
    code, _, headers = _request(
        "POST", "/batches", body,
        f"multipart/form-data; boundary={boundary}")
    retry_after = headers.get("Retry-After") or headers.get("retry-after")
    return code, time.monotonic() - t0, retry_after


def main() -> int:
    print(f"== 目标服务 {BASE} ==")
    initial = stats()
    g0 = initial["gate"]
    max_active, max_queued = g0["max_active"], g0["max_queued"]
    capacity = g0["capacity"]
    print(f"容量配置：执行 {max_active}，排队 {max_queued}，总容量 {capacity}")

    print("\n== 阶段 1：基线正常上传 ==")
    code, latency, _ = upload(1)
    print(f"  基线上传 -> HTTP {code}，耗时 {latency*1000:.0f}ms")
    if code != 303:
        print("  基线上传失败，服务可能未就绪，请先启动控制台。")
        return 1

    print("\n== 阶段 2：打开持锁窗口并发起 24 份并发上传 ==")
    hold_seconds = 14
    info = hold_lock(hold_seconds)
    print(f"  持锁：{info['detail']}")

    n_uploads = capacity + 4  # 总容量之外再多发 4 个，验证满额 503
    results = []

    def worker(i):
        return upload(100 + i)

    with ThreadPoolExecutor(max_workers=n_uploads) as pool:
        futures = [pool.submit(worker, i) for i in range(n_uploads)]

        # 等闸门占满
        deadline = time.monotonic() + 10
        while time.monotonic() < deadline:
            g = stats()["gate"]
            if g["inflight"] >= capacity:
                break
            time.sleep(0.1)
        g = stats()
        print(f"  闸门占满：执行 {g['gate']['active']}/{max_active}，"
              f"排队 {g['gate']['queued']}/{max_queued}，持锁="
              f"{g['lock']['lock_held']}")

        print("\n== 阶段 3：持锁期间测量页面/进度/停止响应延迟 ==")
        latencies = {"首页": [], "进度API": [], "停止": []}
        for _ in range(6):
            t0 = time.monotonic()
            _request("GET", "/", timeout=10)
            latencies["首页"].append(time.monotonic() - t0)
            t0 = time.monotonic()
            _request("GET", "/api/batches/1", timeout=10)
            latencies["进度API"].append(time.monotonic() - t0)
            t0 = time.monotonic()
            _request("POST", "/batches/1/stop", b"",
                     "application/x-www-form-urlencoded", timeout=10)
            latencies["停止"].append(time.monotonic() - t0)
        for name, vals in latencies.items():
            print(f"  {name:8s} 最大 {max(vals)*1000:6.1f}ms "
                  f"平均 {sum(vals)/len(vals)*1000:6.1f}ms")
        worst = max(max(v) for v in latencies.values())
        print(f"  全部 <1s：{'是' if worst < 1 else '否'}")

        print("\n== 阶段 4：持锁窗口内取消全部排队请求 ==")
        before_cancel = stats()["gate"]
        t0 = time.monotonic()
        cancelled = cancel_queued()
        cancel_call = time.monotonic() - t0
        print(f"  取消接口 {cancel_call*1000:.0f}ms 返回，"
              f"取消排队 {cancelled['cancelled_queued']} 个，"
              f"执行中保留 {cancelled['active_kept']} 个（名额未提前释放）")

        # 等被取消的排队请求收到 503
        time.sleep(1.5)
        results = [f.result(timeout=30) for f in futures]

    print("\n== 阶段 5：等待锁释放，执行中的请求完成建批 ==")
    deadline = time.monotonic() + hold_seconds + 20
    while time.monotonic() < deadline:
        s = stats()
        if not s["lock"]["lock_held"] and s["gate"]["inflight"] == 0:
            break
        time.sleep(0.3)

    codes = sorted(r[0] for r in results)
    n_503 = codes.count(503)
    n_303 = codes.count(303)
    print(f"  {n_uploads} 份上传结果：303 建批 {n_303} 个，"
          f"503（排队取消/满额）{n_503} 个")
    retry_after = {r[2] for r in results if r[0] == 503}
    print(f"  503 均带 Retry-After：{retry_after}")

    print("\n== 阶段 6：锁释放后重新上传（可重试闭环） ==")
    code, latency, _ = upload(999)
    print(f"  解锁后上传 -> HTTP {code}，耗时 {latency*1000:.0f}ms")
    final = stats()
    print(f"  最终闸门状态：{final['gate']}，持锁={final['lock']['lock_held']}")

    ok = (n_503 == (max_queued + 4) and n_303 == max_active
          and code == 303 and worst < 1.0 and cancel_call < 1.0)
    print("\n==== 演示结论："
          + ("符合预期 ✔" if ok else "存在不符项 ✘"))
    return 0 if ok else 2


if __name__ == "__main__":
    sys.exit(main())
