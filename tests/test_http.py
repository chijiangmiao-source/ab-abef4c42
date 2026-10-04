"""HTTP 层测试：页面、健康检查、幂等重传、冲突隔离、并发封签与重启恢复。"""

from __future__ import annotations

import json
import os
import tempfile
import threading
import time
import unittest
import urllib.error
import urllib.request
from typing import Any

from app.server import build_server


def _free_port() -> int:
    import socket
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


class HttpTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cal.db")
        self.port = _free_port()
        self.httpd = build_server("127.0.0.1", self.port, self.db)
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)
        self.thread.start()
        self.base = "http://127.0.0.1:%d" % self.port
        # 等待端口就绪。
        for _ in range(50):
            try:
                self.request("GET", "/healthz")
                break
            except OSError:
                time.sleep(0.05)

    def tearDown(self) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd.store.close()
        self.thread.join(timeout=5)
        self.tmp.cleanup()

    def request(self, method: str, path: str,
                body: dict[str, Any] | None = None) -> tuple[int, Any]:
        data = None
        headers = {}
        if body is not None:
            data = json.dumps(body).encode("utf-8")
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(self.base + path, data=data,
                                     headers=headers, method=method)
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                raw = resp.read()
                return resp.status, json.loads(raw) if raw else None
        except urllib.error.HTTPError as exc:
            raw = exc.read()
            return exc.code, json.loads(raw) if raw else None

    def test_health_and_page(self) -> None:
        status, body = self.request("GET", "/healthz")
        self.assertEqual(status, 200)
        self.assertEqual(body["status"], "ok")
        # 页面“构建可用”：HTML 与静态资源均可取到。
        with urllib.request.urlopen(self.base + "/", timeout=5) as resp:
            html = resp.read().decode("utf-8")
        self.assertIn("批次封签控制台", html)
        self.assertIn("/static/app.js", html)
        with urllib.request.urlopen(self.base + "/static/app.js",
                                    timeout=5) as resp:
            js = resp.read().decode("utf-8")
        self.assertIn("acceptState", js)
        with urllib.request.urlopen(self.base + "/static/styles.css",
                                    timeout=5) as resp:
            self.assertEqual(resp.status, 200)

    def test_config_validation_rejected_actionably(self) -> None:
        status, body = self.request("POST", "/api/batches",
                                    {"batch_id": "bad id", "stations": [],
                                     "threshold": 0})
        self.assertEqual(status, 400)
        self.assertTrue(body["code"])
        self.assertTrue(body["message"])
        self.assertIn("hint", body)

    def test_idempotent_replay_over_http(self) -> None:
        self.request("POST", "/api/batches",
                     {"batch_id": "B1", "stations": ["S1", "S2", "S3"],
                      "threshold": 3})
        s1, r1 = self.request("POST", "/api/batches/B1/votes",
                              {"station": "S1", "vote_id": "stable-1",
                               "summary": "digest-A"})
        self.assertEqual(s1, 201)
        self.assertFalse(r1["replayed"])
        self.assertEqual(r1["state"]["approve_count"], 1)
        for _ in range(3):
            s2, r2 = self.request("POST", "/api/batches/B1/votes",
                                  {"station": "S1", "vote_id": "stable-1",
                                   "summary": "digest-A"})
            self.assertEqual(s2, 202)
            self.assertTrue(r2["replayed"])
            self.assertEqual(r2["state"]["approve_count"], 1)
            self.assertIsNone(r2["vote"]["reason"])

    def test_conflict_isolated_over_http(self) -> None:
        self.request("POST", "/api/batches",
                     {"batch_id": "B2", "stations": ["S1", "S2"],
                      "threshold": 2})
        self.request("POST", "/api/batches/B2/votes",
                     {"station": "S1", "vote_id": "v1", "summary": "digest-A"})
        # 不同摘要：冲突隔离。
        s, r = self.request("POST", "/api/batches/B2/votes",
                            {"station": "S2", "vote_id": "v2",
                             "summary": "digest-B"})
        self.assertEqual(s, 201)
        self.assertEqual(r["decision"], "CONFLICT")
        self.assertEqual(r["state"]["approve_count"], 1)
        self.assertNotEqual(r["state"]["status"], "SEALED")
        self.assertIn("S2", r["state"]["conflict_stations"])
        # 同一 vote_id 改绑内容：冲突。
        s, r = self.request("POST", "/api/batches/B2/votes",
                            {"station": "S1", "vote_id": "v1",
                             "summary": "digest-FORGED"})
        self.assertEqual(r["decision"], "CONFLICT")
        self.assertEqual(r["reason"], "VOTE_ID_CONTENT_CHANGED")
        # 名单外站点：400 可操作拒绝。
        s, r = self.request("POST", "/api/batches/B2/votes",
                            {"station": "SX", "vote_id": "vx",
                             "summary": "digest-A"})
        self.assertEqual(s, 400)
        self.assertEqual(r["code"], "STATION_NOT_IN_LIST")

    def test_concurrent_seal_uniqueness_over_http(self) -> None:
        stations = ["S%d" % i for i in range(6)]
        self.request("POST", "/api/batches",
                     {"batch_id": "RACE", "stations": stations, "threshold": 4})
        results: list[tuple[int, Any]] = []
        lock = threading.Lock()
        barrier = threading.Barrier(len(stations))

        def vote(station: str) -> None:
            barrier.wait()
            out = self.request("POST", "/api/batches/RACE/votes",
                               {"station": station, "vote_id": "v-" + station,
                                "summary": "digest-A"})
            with lock:
                results.append(out)

        threads = [threading.Thread(target=vote, args=(s,)) for s in stations]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        sealed = [r for _, r in results if r and r.get("sealed_now")]
        self.assertEqual(len(sealed), 1)
        status, state = self.request("GET", "/api/batches/RACE")
        self.assertEqual(state["status"], "SEALED")
        self.assertEqual(state["approve_count"], 4)
        self.assertIsNotNone(state["certificate"])
        late = [r for s, r in results if s == 409]
        self.assertEqual(len(late), 2)

    def test_restart_recovery_over_http(self) -> None:
        self.request("POST", "/api/batches",
                     {"batch_id": "B3", "stations": ["S1", "S2", "S3"],
                      "threshold": 3})
        self.request("POST", "/api/batches/B3/votes",
                     {"station": "S1", "vote_id": "v1", "summary": "digest-A"})
        self.request("POST", "/api/batches/B3/votes",
                     {"station": "S2", "vote_id": "v2", "summary": "digest-A"})
        # 模拟重启：关掉服务，用同一 DB 重建。
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd.store.close()
        self.thread.join(timeout=5)

        self.httpd = build_server("127.0.0.1", self.port, self.db)
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)
        self.thread.start()
        status, state = self.request("GET", "/api/batches/B3")
        self.assertEqual(status, 200)
        self.assertEqual(state["approve_count"], 2)
        self.assertEqual(state["frozen_summary"], "digest-A")
        # 恢复后完成封签。
        s, r = self.request("POST", "/api/batches/B3/votes",
                            {"station": "S3", "vote_id": "v3",
                             "summary": "digest-A"})
        self.assertEqual(s, 201)
        self.assertTrue(r["sealed_now"])
        # 再重启：已封存不可回到收集中，迟到票仍被拒绝。
        self.httpd.shutdown()
        self.httpd.server_close()
        self.httpd.store.close()
        self.thread.join(timeout=5)
        self.httpd = build_server("127.0.0.1", self.port, self.db)
        self.thread = threading.Thread(target=self.httpd.serve_forever,
                                       daemon=True)
        self.thread.start()
        _, state = self.request("GET", "/api/batches/B3")
        self.assertEqual(state["status"], "SEALED")
        s, r = self.request("POST", "/api/batches/B3/votes",
                            {"station": "S3", "vote_id": "v9",
                             "summary": "digest-A"})
        self.assertEqual(s, 409)
        self.assertEqual(r["code"], "LATE_VOTE")


if __name__ == "__main__":
    unittest.main()
