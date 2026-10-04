#!/usr/bin/env python3
"""Compose 验收脚本（verify 服务入口）。

流程：
1. 等待页面与健康响应可用；
2. 运行覆盖幂等重传、冲突隔离的代码测试（unittest）；
3. API/HTTP 冒烟：健康响应、并发封签唯一性、重启恢复。

全部通过退出码 0，任一失败退出码非 0，完成即退出。
仅依赖 Python 标准库；目标地址由环境变量配置：
* BASE_URL（默认 http://web:8080）
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from typing import Any

BASE_URL = os.environ.get("BASE_URL", "http://web:8080").rstrip("/")
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

PASS = "\033[32mPASS\033[0m"
FAIL = "\033[31mFAIL\033[0m"
STEP = "\033[36mSTEP\033[0m"


def log(kind: str, msg: str) -> None:
    print("[%s] %s" % (kind, msg), flush=True)


def http(method: str, path: str,
         body: dict[str, Any] | None = None,
         timeout: float = 10.0) -> tuple[int, Any]:
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE_URL + path, data=data, headers=headers,
                                 method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            return resp.status, json.loads(raw) if raw else None
    except urllib.error.HTTPError as exc:
        raw = exc.read()
        return exc.code, json.loads(raw) if raw else None


def wait_ready(timeout_s: float = 60.0) -> None:
    log(STEP, "等待页面与健康响应可用：%s" % BASE_URL)
    deadline = time.time() + timeout_s
    last_err = ""
    while time.time() < deadline:
        try:
            status, body = http("GET", "/healthz")
            if status == 200 and body and body.get("status") == "ok":
                with urllib.request.urlopen(BASE_URL + "/",
                                            timeout=5) as resp:
                    html = resp.read().decode("utf-8")
                if "批次封签控制台" in html:
                    log(PASS, "页面与健康响应可用")
                    return
                last_err = "页面内容不符"
            else:
                last_err = "healthz 状态 %s" % status
        except OSError as exc:
            last_err = str(exc)
        time.sleep(1.0)
    log(FAIL, "服务在 %ss 内未就绪：%s" % (int(timeout_s), last_err))
    sys.exit(1)


def run_code_tests() -> None:
    log(STEP, "运行代码测试（幂等重传、冲突隔离、并发封签、恢复）")
    proc = subprocess.run(
        [sys.executable, "-m", "unittest", "discover", "-s", "tests",
         "-p", "test_*.py", "-v"],
        cwd=ROOT)
    if proc.returncode != 0:
        log(FAIL, "代码测试失败，退出码 %d" % proc.returncode)
        sys.exit(proc.returncode or 1)
    log(PASS, "代码测试全部通过（含页面/接口可用性）")


def smoke_health() -> None:
    log(STEP, "冒烟：健康响应与可操作拒绝反馈")
    status, body = http("GET", "/healthz")
    assert status == 200 and body["status"] == "ok", body
    status, body = http("POST", "/api/batches",
                        {"batch_id": "bad batch", "stations": [], "threshold": 0})
    assert status == 400 and body.get("code") and body.get("hint"), body
    log(PASS, "健康响应 200；非法配置返回可操作错误（%s）" % body["code"])


def smoke_idempotent_and_conflict() -> None:
    log(STEP, "冒烟：幂等重传不增票 + 冲突票隔离")
    bid = "SMOKE-%d" % int(time.time())
    stations = ["S-A", "S-B", "S-C"]
    status, _ = http("POST", "/api/batches",
                     {"batch_id": bid, "stations": stations, "threshold": 3})
    assert status == 201, "创建批次失败"
    status, r = http("POST", "/api/batches/%s/votes" % bid,
                     {"station": "S-A", "vote_id": "k1", "summary": "h-A"})
    assert status == 201 and r["state"]["approve_count"] == 1, r
    for _ in range(3):
        status, r = http("POST", "/api/batches/%s/votes" % bid,
                         {"station": "S-A", "vote_id": "k1", "summary": "h-A"})
        assert status == 202 and r["replayed"], r
        assert r["state"]["approve_count"] == 1, r
    # vote_id 改绑内容 → 冲突。
    status, r = http("POST", "/api/batches/%s/votes" % bid,
                     {"station": "S-A", "vote_id": "k1", "summary": "h-X"})
    assert r["decision"] == "CONFLICT" and r["reason"] == \
        "VOTE_ID_CONTENT_CHANGED", r
    # 不同摘要 → 冲突，阈值仍不满足。
    status, r = http("POST", "/api/batches/%s/votes" % bid,
                     {"station": "S-B", "vote_id": "k2", "summary": "h-B"})
    assert r["decision"] == "CONFLICT" and \
        r["state"]["approve_count"] == 1, r
    log(PASS, "重传回放不增票；两类冲突均被隔离")


def smoke_concurrent_seal() -> None:
    log(STEP, "冒烟：多站并发达到阈值只生成一份证书")
    bid = "RACE-%d" % int(time.time())
    stations = ["S%d" % i for i in range(8)]
    status, _ = http("POST", "/api/batches",
                     {"batch_id": bid, "stations": stations, "threshold": 5})
    assert status == 201
    results: list[tuple[int, Any]] = []
    lock = threading.Lock()
    barrier = threading.Barrier(len(stations))

    def vote(station: str) -> None:
        barrier.wait()
        out = http("POST", "/api/batches/%s/votes" % bid,
                   {"station": station, "vote_id": "v-" + station,
                    "summary": "h-A"})
        with lock:
            results.append(out)

    threads = [threading.Thread(target=vote, args=(s,)) for s in stations]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    sealed = [r for _, r in results if r and r.get("sealed_now")]
    assert len(sealed) == 1, "期望恰好 1 次封签，实际 %d 次" % len(sealed)
    status, state = http("GET", "/api/batches/%s" % bid)
    assert state["status"] == "SEALED", state
    assert state["approve_count"] == 5, state
    assert state["certificate"] and state["certificate"]["immutable"], state
    late = [r for s, r in results if s == 409 and r["code"] == "LATE_VOTE"]
    assert len(late) == 3, late
    log(PASS, "8 站并发、阈值 5：恰好 1 份不可变证书，3 张迟到票被拒")


def _free_port() -> int:
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def _wait_local(url: str, timeout_s: float = 30.0) -> None:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(url + "/healthz", timeout=3) as r:
                if r.status == 200:
                    return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError("本地验收服务未就绪：%s" % url)


def smoke_restart_recovery() -> None:
    """进程崩溃/中断后用同一持久 DB 重启：状态恢复、封存不回退。"""
    log(STEP, "冒烟：写入途中异常中断后的持久化恢复")
    port = _free_port()
    url = "http://127.0.0.1:%d" % port
    data_dir = tempfile.mkdtemp(prefix="cal-recover-")
    db_path = os.path.join(data_dir, "cal.db")
    env = dict(os.environ, HOST="127.0.0.1", PORT=str(port), DB_PATH=db_path,
               QUIET="1")

    proc = subprocess.Popen(
        [sys.executable, "-m", "app.server"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        _wait_local(url)
        def lh(method: str, path: str, body=None):
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(
                url + path, data=data,
                headers={"Content-Type": "application/json"}, method=method)
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status, json.loads(resp.read())
            except urllib.error.HTTPError as exc:
                return exc.code, json.loads(exc.read())

        lh("POST", "/api/batches",
           {"batch_id": "REC", "stations": ["S1", "S2", "S3"], "threshold": 3})
        lh("POST", "/api/batches/REC/votes",
           {"station": "S1", "vote_id": "v1", "summary": "h-A"})
        lh("POST", "/api/batches/REC/votes",
           {"station": "S2", "vote_id": "v2", "summary": "h-A"})
    finally:
        # 模拟异常中断：SIGKILL，不给优雅停机机会。
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)

    # 用同一持久 DB 重启。
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.server"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        _wait_local(url)
        def lh2(method: str, path: str, body=None):
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(
                url + path, data=data,
                headers={"Content-Type": "application/json"}, method=method)
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status, json.loads(resp.read())
            except urllib.error.HTTPError as exc:
                return exc.code, json.loads(exc.read())

        status, state = lh2("GET", "/api/batches/REC")
        assert status == 200, state
        assert state["status"] == "COLLECTING" and \
            state["approve_count"] == 2 and \
            state["frozen_summary"] == "h-A", state
        status, r = lh2("POST", "/api/batches/REC/votes",
                        {"station": "S3", "vote_id": "v3", "summary": "h-A"})
        assert status == 201 and r["sealed_now"], r
    finally:
        proc.send_signal(signal.SIGKILL)
        proc.wait(timeout=10)

    # 再次重启：已封存批次不得回到收集中，迟到票不得改写证书。
    proc = subprocess.Popen(
        [sys.executable, "-m", "app.server"], cwd=ROOT, env=env,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    try:
        _wait_local(url)
        def lh3(method: str, path: str, body=None):
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(
                url + path, data=data,
                headers={"Content-Type": "application/json"}, method=method)
            try:
                with urllib.request.urlopen(req, timeout=5) as resp:
                    return resp.status, json.loads(resp.read())
            except urllib.error.HTTPError as exc:
                return exc.code, json.loads(exc.read())

        _, state = lh3("GET", "/api/batches/REC")
        assert state["status"] == "SEALED" and state["certificate"], state
        cert_hash = state["certificate"]["hash"]
        status, r = lh3("POST", "/api/batches/REC/votes",
                        {"station": "S3", "vote_id": "v9", "summary": "h-A"})
        assert status == 409 and r["code"] == "LATE_VOTE", (status, r)
        _, state2 = lh3("GET", "/api/batches/REC")
        assert state2["certificate"]["hash"] == cert_hash, "证书被迟到票改写"
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(timeout=10)
    log(PASS, "SIGKILL 中断后同库重启：票数/冻结配置恢复，封签不回退、证书不变")


def main() -> int:
    print("=" * 64)
    print("深空辐照标定 · Compose 验收 (verify) -> %s" % BASE_URL)
    print("=" * 64, flush=True)
    wait_ready()
    run_code_tests()
    smoke_health()
    smoke_idempotent_and_conflict()
    smoke_concurrent_seal()
    smoke_restart_recovery()
    status, body = http("GET", "/api/batches")
    assert status == 200 and isinstance(body.get("batches"), list), body
    print("=" * 64)
    log(PASS, "全部验收通过：页面/健康、幂等重传、冲突隔离、"
              "并发封签唯一、异常中断后恢复")
    print("=" * 64)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except AssertionError as exc:
        log(FAIL, "冒烟断言失败：%r" % (exc,))
        sys.exit(2)
    except Exception as exc:  # noqa: BLE001
        log(FAIL, "验收异常：%r" % (exc,))
        sys.exit(3)
