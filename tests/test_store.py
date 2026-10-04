"""领域逻辑测试：冻结、幂等重传、冲突隔离、并发封签唯一性、重启恢复。"""

from __future__ import annotations

import os
import tempfile
import threading
import unittest

from app.store import (DECISION_APPROVE, DECISION_CONFLICT, STATUS_COLLECTING,
                       STATUS_SEALED, DomainError, Store)


def _cfg(batch_id="B1", stations=None, threshold=2):
    return {
        "batch_id": batch_id,
        "stations": stations or ["S1", "S2", "S3"],
        "threshold": threshold,
    }


class StoreTest(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.db = os.path.join(self.tmp.name, "cal.db")
        self.store = Store(self.db)

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def vote(self, batch="B1", station="S1", vote_id="v1",
             summary="digest-A"):
        return self.store.submit_vote(batch, {
            "station": station, "vote_id": vote_id, "summary": summary})

    # ---- 配置校验 -----------------------------------------------------
    def test_invalid_config_is_actionable(self) -> None:
        for bad in [
            {},
            {"batch_id": "bad id!", "stations": ["S1"], "threshold": 1},
            {"batch_id": "B", "stations": [], "threshold": 1},
            {"batch_id": "B", "stations": ["S1", "S1"], "threshold": 1},
            {"batch_id": "B", "stations": ["S1"], "threshold": 2},
            {"batch_id": "B", "stations": ["S1"], "threshold": 0},
            {"batch_id": "B", "stations": ["S1"], "threshold": "1"},
        ]:
            with self.assertRaises(DomainError) as ctx:
                self.store.create_batch(bad)
            self.assertTrue(ctx.exception.code)
            self.assertEqual(ctx.exception.http_status, 400)

    def test_duplicate_batch_rejected(self) -> None:
        self.store.create_batch(_cfg())
        with self.assertRaises(DomainError) as ctx:
            self.store.create_batch(_cfg())
        self.assertEqual(ctx.exception.code, "BATCH_EXISTS")

    def test_station_not_in_list_rejected(self) -> None:
        self.store.create_batch(_cfg())
        with self.assertRaises(DomainError) as ctx:
            self.vote(station="SX")
        self.assertEqual(ctx.exception.code, "STATION_NOT_IN_LIST")

    # ---- 原子冻结 -----------------------------------------------------
    def test_first_valid_vote_freezes_config(self) -> None:
        self.store.create_batch(_cfg(threshold=3))
        r = self.vote(station="S1", vote_id="v1", summary="digest-A")
        self.assertEqual(r.decision, DECISION_APPROVE)
        st = r.state
        self.assertEqual(st["status"], STATUS_COLLECTING)
        self.assertEqual(st["frozen_summary"], "digest-A")
        self.assertIsNotNone(st["frozen_at"])
        self.assertEqual(st["threshold"], 3)
        self.assertEqual(st["stations"], ["S1", "S2", "S3"])

    # ---- 幂等重传 -----------------------------------------------------
    def test_same_vote_id_same_summary_replays_without_counting(self) -> None:
        self.store.create_batch(_cfg(threshold=3))
        first = self.vote("B1", "S1", "stable-1", "digest-A")
        self.assertFalse(first.replayed)
        self.assertEqual(first.state["approve_count"], 1)
        for _ in range(5):
            again = self.vote("B1", "S1", "stable-1", "digest-A")
            self.assertTrue(again.replayed)
            self.assertEqual(again.decision, DECISION_APPROVE)
        st = self.store.get_state("B1")
        self.assertEqual(st["approve_count"], 1)
        self.assertEqual(len([v for v in st["votes"]
                              if v["decision"] == DECISION_APPROVE]), 1)

    # ---- 冲突隔离 -----------------------------------------------------
    def test_vote_id_rebound_to_different_content_is_conflict(self) -> None:
        self.store.create_batch(_cfg(threshold=2))
        self.vote("B1", "S1", "stable-1", "digest-A")
        r = self.vote("B1", "S1", "stable-1", "digest-FORGED")
        self.assertEqual(r.decision, DECISION_CONFLICT)
        self.assertFalse(r.replayed)
        self.assertEqual(r.reason, "VOTE_ID_CONTENT_CHANGED")
        st = self.store.get_state("B1")
        self.assertEqual(st["approve_count"], 1)
        self.assertIn("S1", st["conflict_stations"])

    def test_different_summary_is_conflict_and_does_not_seal(self) -> None:
        self.store.create_batch(_cfg(threshold=2))
        self.vote("B1", "S1", "v1", "digest-A")
        r = self.vote("B1", "S2", "v2", "digest-B")
        self.assertEqual(r.decision, DECISION_CONFLICT)
        self.assertEqual(r.reason, "SUMMARY_MISMATCH")
        st = self.store.get_state("B1")
        self.assertEqual(st["approve_count"], 1)
        self.assertNotEqual(st["status"], STATUS_SEALED)
        self.assertIn("S2", st["conflict_stations"])

    def test_station_cannot_vote_twice_with_other_id(self) -> None:
        self.store.create_batch(_cfg(threshold=2))
        self.vote("B1", "S1", "v1", "digest-A")
        r = self.vote("B1", "S1", "v2", "digest-A")
        self.assertEqual(r.decision, DECISION_CONFLICT)
        self.assertEqual(r.reason, "STATION_ALREADY_VOTED")
        self.assertEqual(self.store.get_state("B1")["approve_count"], 1)

    def test_conflicts_never_reach_threshold(self) -> None:
        # 阈值 1：冲突票也绝不能触发封签。
        self.store.create_batch(
            _cfg(threshold=1, stations=["S1", "S2"]))
        # 先制造冻结（S1 赞成，阈值 1 即封签，所以换阈值 2 场景）
        self.store.create_batch(
            _cfg("B2", threshold=2, stations=["S1", "S2"]))
        self.vote("B2", "S1", "v1", "digest-A")
        for i in range(10):
            r = self.vote("B2", "S2", "c%d" % i,
                          "digest-B" if i % 2 else "digest-A" + "x")
            self.assertEqual(r.decision, DECISION_CONFLICT)
        st = self.store.get_state("B2")
        self.assertEqual(st["approve_count"], 1)
        self.assertNotEqual(st["status"], STATUS_SEALED)

    # ---- 封签 ---------------------------------------------------------
    def test_threshold_seals_once_with_immutable_certificate(self) -> None:
        self.store.create_batch(_cfg(threshold=2))
        self.vote("B1", "S1", "v1", "digest-A")
        r = self.vote("B1", "S2", "v2", "digest-A")
        self.assertTrue(r.sealed_now)
        st = self.store.get_state("B1")
        self.assertEqual(st["status"], STATUS_SEALED)
        cert = st["certificate"]
        self.assertIsNotNone(cert)
        self.assertTrue(cert["immutable"])
        self.assertEqual(cert["approving_stations"], ["S1", "S2"])
        self.assertTrue(self.store.verify_certificate(cert))

        # 证书内容篡改可被指纹识破。
        tampered = dict(cert)
        tampered["summary"] = "digest-EVIL"
        self.assertFalse(self.store.verify_certificate(tampered))

    def test_late_vote_after_seal_rejected(self) -> None:
        self.store.create_batch(_cfg(threshold=2))
        self.vote("B1", "S1", "v1", "digest-A")
        self.vote("B1", "S2", "v2", "digest-A")
        with self.assertRaises(DomainError) as ctx:
            self.vote("B1", "S3", "v3", "digest-A")
        self.assertEqual(ctx.exception.code, "LATE_VOTE")
        # 全新的投票标识、任何摘要都不能改变封存事实。
        with self.assertRaises(DomainError):
            self.vote("B1", "S3", "v4", "digest-OTHER")
        # 但完全相同的重传仍可回放。
        replay = self.vote("B1", "S1", "v1", "digest-A")
        self.assertTrue(replay.replayed)
        cert_after = self.store.get_state("B1")["certificate"]
        self.assertEqual(cert_after["hash"],
                         self.store.get_state("B1")["certificate"]["hash"])

    def test_concurrent_threshold_creates_exactly_one_certificate(self) -> None:
        stations = ["S%d" % i for i in range(8)]
        self.store.create_batch(
            _cfg("RACE", stations=stations, threshold=5))
        barrier = threading.Barrier(len(stations))
        outcomes: list[tuple[str, object]] = []
        lock = threading.Lock()

        def worker(station: str) -> None:
            barrier.wait()
            try:
                r = self.store.submit_vote("RACE", {
                    "station": station, "vote_id": "v-" + station,
                    "summary": "digest-A"})
                with lock:
                    outcomes.append((station, r))
            except DomainError as exc:
                with lock:
                    outcomes.append((station, exc))

        threads = [threading.Thread(target=worker, args=(s,))
                   for s in stations]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        sealed_now = [o for o in outcomes
                      if hasattr(o[1], "sealed_now") and o[1].sealed_now]
        self.assertEqual(len(sealed_now), 1)
        st = self.store.get_state("RACE")
        self.assertEqual(st["status"], STATUS_SEALED)
        self.assertEqual(st["approve_count"], 5)
        self.assertIsNotNone(st["certificate"])
        self.assertTrue(self.store.verify_certificate(st["certificate"]))
        late = [o for o in outcomes if isinstance(o[1], DomainError)]
        self.assertEqual(len(late), 3)
        self.assertTrue(all(o[1].code == "LATE_VOTE" for o in late))

    def test_concurrent_first_vote_freezes_exactly_one_summary(self) -> None:
        stations = ["S1", "S2", "S3"]
        self.store.create_batch(
            _cfg("FREEZE", stations=stations, threshold=3))
        barrier = threading.Barrier(3)
        results: list[object] = []
        lock = threading.Lock()

        def worker(station: str, summary: str) -> None:
            barrier.wait()
            r = self.store.submit_vote("FREEZE", {
                "station": station, "vote_id": "v-" + station,
                "summary": summary})
            with lock:
                results.append(r)

        threads = [threading.Thread(target=worker, args=(s, "digest-" + s))
                   for s in stations]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        approves = [r for r in results if r.decision == DECISION_APPROVE]
        conflicts = [r for r in results if r.decision == DECISION_CONFLICT]
        self.assertEqual(len(approves), 1)
        self.assertEqual(len(conflicts), 2)
        st = self.store.get_state("FREEZE")
        self.assertEqual(st["approve_count"], 1)
        self.assertNotEqual(st["status"], STATUS_SEALED)
        self.assertEqual(st["frozen_summary"],
                         "digest-" + approves[0].vote["station"])

    # ---- 重启恢复 -----------------------------------------------------
    def test_recovery_after_restart(self) -> None:
        self.store.create_batch(_cfg(threshold=3))
        self.vote("B1", "S1", "v1", "digest-A")
        self.vote("B1", "S2", "v2", "digest-A")
        self.store.close()

        reopened = Store(self.db)
        st = reopened.get_state("B1")
        self.assertEqual(st["status"], STATUS_COLLECTING)
        self.assertEqual(st["approve_count"], 2)
        self.assertEqual(st["frozen_summary"], "digest-A")
        # 恢复后继续：第三票封签。
        r = reopened.submit_vote("B1", {
            "station": "S3", "vote_id": "v3", "summary": "digest-A"})
        self.assertTrue(r.sealed_now)
        reopened.close()

        sealed_store = Store(self.db)
        st = sealed_store.get_state("B1")
        self.assertEqual(st["status"], STATUS_SEALED)
        self.assertTrue(sealed_store.verify_certificate(st["certificate"]))
        with self.assertRaises(DomainError) as ctx:
            sealed_store.submit_vote("B1", {
                "station": "S3", "vote_id": "v9", "summary": "digest-A"})
        self.assertEqual(ctx.exception.code, "LATE_VOTE")
        sealed_store.close()


if __name__ == "__main__":
    unittest.main()
