"""标定批次的持久化与领域逻辑。

仅依赖 Python 标准库（sqlite3）。所有写操作在单个
``BEGIN IMMEDIATE`` 事务内完成，SQLite 的写串行化保证：

* 首次有效投票原子冻结名单 / 阈值 / 摘要；
* (站点, 稳定投票标识) 的重传只回放首次结果，不增票；
* 不同摘要或同一标识绑定内容改变一律记为冲突，不参与封签；
* 达到阈值的当次事务内唯一生成不可变证书；
* 提交要么完整生效要么不生效，重启后从同一数据库恢复。
"""

from __future__ import annotations

import json
import os
import re
import sqlite3
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from typing import Any, Iterator

BATCH_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$")
MAX_STATIONS = 100
SUMMARY_MAX = 8192
VOTE_ID_MAX = 128
STATION_MAX = 64

STATUS_AWAITING = "AWAITING_FIRST_VOTE"
STATUS_COLLECTING = "COLLECTING"
STATUS_SEALED = "SEALED"

DECISION_APPROVE = "APPROVE"
DECISION_CONFLICT = "CONFLICT"

# 状态先后次序，用于防止旧响应覆盖新状态。
STATUS_RANK = {
    STATUS_AWAITING: 0,
    STATUS_COLLECTING: 1,
    STATUS_SEALED: 2,
}


class DomainError(Exception):
    """可操作的业务拒绝。http_status 供 HTTP 层使用。"""

    def __init__(self, code: str, message: str, http_status: int = 400,
                 hint: str | None = None) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.http_status = http_status
        self.hint = hint

    def to_dict(self) -> dict[str, Any]:
        body: dict[str, Any] = {"code": self.code, "message": self.message}
        if self.hint:
            body["hint"] = self.hint
        return body


def _utcnow() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def _canonical_hash(payload: dict[str, Any]) -> str:
    blob = json.dumps(payload, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"))
    return sha256(blob.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class VoteResult:
    """提交投票后的结论。"""

    decision: str                      # APPROVE / CONFLICT
    replayed: bool                     # 是否为重传回放
    reason: str | None
    vote: dict[str, Any]
    sealed_now: bool                   # 本次提交是否触发了封签
    state: dict[str, Any]


class Store:
    def __init__(self, path: str) -> None:
        self.path = path
        if path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(
            path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=FULL")
        self._conn.execute("PRAGMA foreign_keys=ON")
        self._init_schema()

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    @contextmanager
    def _tx(self) -> Iterator[sqlite3.Cursor]:
        """串行化的写事务，提交/回滚都在锁内完成。"""
        with self._lock:
            cur = self._conn.cursor()
            cur.execute("BEGIN IMMEDIATE")
            try:
                yield cur
                cur.execute("COMMIT")
            except Exception:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    def _init_schema(self) -> None:
        with self._tx() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS batches (
                    batch_id       TEXT PRIMARY KEY,
                    stations_json  TEXT NOT NULL,
                    threshold      INTEGER NOT NULL,
                    status         TEXT NOT NULL,
                    frozen_summary TEXT,
                    frozen_at      TEXT,
                    sealed_at      TEXT,
                    version        INTEGER NOT NULL DEFAULT 0,
                    created_at     TEXT NOT NULL
                )
                """)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS votes (
                    id         INTEGER PRIMARY KEY AUTOINCREMENT,
                    batch_id   TEXT NOT NULL REFERENCES batches(batch_id),
                    station    TEXT NOT NULL,
                    vote_id    TEXT NOT NULL,
                    summary    TEXT NOT NULL,
                    decision   TEXT NOT NULL,
                    reason     TEXT,
                    created_at TEXT NOT NULL
                )
                """)
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS certificates (
                    batch_id        TEXT PRIMARY KEY REFERENCES batches(batch_id),
                    cert_id         TEXT NOT NULL,
                    summary         TEXT NOT NULL,
                    threshold       INTEGER NOT NULL,
                    stations_json   TEXT NOT NULL,
                    approving_json  TEXT NOT NULL,
                    frozen_at       TEXT NOT NULL,
                    sealed_at       TEXT NOT NULL,
                    hash            TEXT NOT NULL
                )
                """)
            # 每个站点至多一张赞成票（冲突票不受限，便于审计）。
            cur.execute(
                """
                CREATE UNIQUE INDEX IF NOT EXISTS idx_one_approve_per_station
                ON votes(batch_id, station)
                WHERE decision = 'APPROVE'
                """)
            cur.execute(
                "CREATE INDEX IF NOT EXISTS idx_votes_batch ON votes(batch_id, id)")

    # ------------------------------------------------------------------
    # 批次配置
    # ------------------------------------------------------------------
    @staticmethod
    def validate_config(payload: Any) -> tuple[str, list[str], int]:
        if not isinstance(payload, dict):
            raise DomainError("INVALID_BODY", "请求体必须是 JSON 对象", 400)

        batch_id = payload.get("batch_id")
        if not isinstance(batch_id, str) or not BATCH_ID_RE.match(batch_id):
            raise DomainError(
                "INVALID_BATCH_ID",
                "批次标识须为 1-64 位字母、数字或 ._-，且以字母数字开头",
                400, hint="例如：DSCOVR-2026-001")

        raw_stations = payload.get("stations")
        if not isinstance(raw_stations, list) or not raw_stations:
            raise DomainError(
                "INVALID_STATIONS", "站点名单必须是非空数组", 400,
                hint="提交 1-%d 个互不重复的站点标识" % MAX_STATIONS)
        stations: list[str] = []
        for item in raw_stations:
            if not isinstance(item, str):
                raise DomainError("INVALID_STATIONS",
                                  "站点标识必须是字符串", 400)
            s = item.strip()
            if not s or len(s) > STATION_MAX:
                raise DomainError(
                    "INVALID_STATIONS",
                    "站点标识须为 1-%d 个非空字符" % STATION_MAX, 400)
            if s in stations:
                raise DomainError(
                    "INVALID_STATIONS", "站点名单存在重复站点：%s" % s, 400,
                    hint="固定站点名单中每个站点只能出现一次")
            stations.append(s)
        if len(stations) > MAX_STATIONS:
            raise DomainError(
                "INVALID_STATIONS",
                "站点名单数量上限为 %d" % MAX_STATIONS, 400)

        threshold = payload.get("threshold")
        if isinstance(threshold, bool) or not isinstance(threshold, int):
            raise DomainError(
                "INVALID_THRESHOLD", "阈值必须是正整数", 400,
                hint="取值范围 1-%d（站点总数）" % len(stations))
        if not 1 <= threshold <= len(stations):
            raise DomainError(
                "INVALID_THRESHOLD",
                "阈值必须在 1 到站点总数(%d)之间" % len(stations), 400)
        return batch_id, stations, threshold

    def create_batch(self, payload: Any) -> dict[str, Any]:
        batch_id, stations, threshold = self.validate_config(payload)
        now = _utcnow()
        with self._tx() as cur:
            exists = cur.execute(
                "SELECT 1 FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
            if exists:
                raise DomainError(
                    "BATCH_EXISTS", "批次 %s 已存在，不能重复配置" % batch_id,
                    409, hint="请改用新的批次标识，或直接查看既有批次")
            cur.execute(
                """INSERT INTO batches(batch_id, stations_json, threshold,
                                       status, created_at, version)
                   VALUES(?,?,?,?,?,0)""",
                (batch_id, json.dumps(stations, ensure_ascii=False), threshold,
                 STATUS_AWAITING, now))
        return self.get_state(batch_id)

    # ------------------------------------------------------------------
    # 投票
    # ------------------------------------------------------------------
    @staticmethod
    def _validate_vote_payload(payload: Any) -> tuple[str, str, str]:
        if not isinstance(payload, dict):
            raise DomainError("INVALID_BODY", "请求体必须是 JSON 对象", 400)
        station = payload.get("station")
        summary = payload.get("summary")
        vote_id = payload.get("vote_id")
        if not isinstance(station, str) or not station.strip() \
                or len(station) > STATION_MAX:
            raise DomainError(
                "INVALID_STATION",
                "station 缺失或无效（须为 1-%d 字符）" % STATION_MAX, 400)
        if not isinstance(summary, str) or not summary.strip() \
                or len(summary) > SUMMARY_MAX:
            raise DomainError(
                "INVALID_SUMMARY",
                "summary 缺失或为空（最长 %d 字符）" % SUMMARY_MAX, 400,
                hint="摘要为该站点提交的标定读数指纹/文本")
        if not isinstance(vote_id, str) or not vote_id.strip() \
                or len(vote_id) > VOTE_ID_MAX:
            raise DomainError(
                "INVALID_VOTE_ID",
                "vote_id 缺失或无效（稳定投票标识，1-%d 字符）" % VOTE_ID_MAX,
                400, hint="重传时必须使用与首次完全相同的 vote_id")
        return station.strip(), summary, vote_id.strip()

    def _fetch_batch(self, cur: sqlite3.Cursor, batch_id: str) -> sqlite3.Row:
        row = cur.execute(
            "SELECT * FROM batches WHERE batch_id=?", (batch_id,)).fetchone()
        if row is None:
            raise DomainError("BATCH_NOT_FOUND",
                              "批次 %s 不存在" % batch_id, 404,
                              hint="请先配置该批次再提交投票")
        return row

    def submit_vote(self, batch_id: str, payload: Any) -> VoteResult:
        station, summary, vote_id = self._validate_vote_payload(payload)
        with self._tx() as cur:
            row = self._fetch_batch(cur, batch_id)
            stations: list[str] = json.loads(row["stations_json"])
            if station not in stations:
                raise DomainError(
                    "STATION_NOT_IN_LIST",
                    "站点 %s 不在批次 %s 的固定名单内" % (station, batch_id),
                    400, hint="仅允许名单内站点投票：%s" % "、".join(stations))

            # 1) 同站 + 同一稳定投票标识：只回放首次结果。
            prior = cur.execute(
                """SELECT * FROM votes
                   WHERE batch_id=? AND station=? AND vote_id=?
                   ORDER BY id ASC LIMIT 1""",
                (batch_id, station, vote_id)).fetchone()
            if prior is not None:
                if prior["summary"] != summary:
                    # 同一投票标识绑定了不同内容：记冲突，绝不回放成赞成。
                    conflict = self._insert_conflict(
                        cur, batch_id, station, vote_id, summary,
                        "VOTE_ID_CONTENT_CHANGED")
                    state = self._state_from_row(
                        cur, self._fetch_batch(cur, batch_id))
                    return VoteResult(DECISION_CONFLICT, False,
                                      conflict["reason"], conflict, False, state)
                state = self._state_from_row(
                    cur, cur.execute(
                        "SELECT * FROM batches WHERE batch_id=?",
                        (batch_id,)).fetchone())
                return VoteResult(prior["decision"], True, prior["reason"],
                                  self._vote_dict(prior), False, state)

            # 2) 已封存批次的“新”投票一律拒绝，不得改写证书。
            if row["status"] == STATUS_SEALED:
                raise DomainError(
                    "LATE_VOTE",
                    "批次 %s 已封存，迟到投票不再记录" % batch_id, 409,
                    hint="如需重新标定请使用新批次标识；完全相同的重传仍可回放")

            # 3) 已冻结且摘要不同 → 冲突隔离。
            if row["status"] == STATUS_COLLECTING \
                    and summary != row["frozen_summary"]:
                conflict = self._insert_conflict(
                    cur, batch_id, station, vote_id, summary, "SUMMARY_MISMATCH")
                return VoteResult(
                    DECISION_CONFLICT, False, conflict["reason"], conflict,
                    False, self._state_from_row(
                        cur, cur.execute(
                            "SELECT * FROM batches WHERE batch_id=?",
                            (batch_id,)).fetchone()))

            # 4) 本站已用别的投票标识投过赞成票 → 冲突，不增票。
            already = cur.execute(
                """SELECT id FROM votes
                   WHERE batch_id=? AND station=? AND decision='APPROVE'""",
                (batch_id, station)).fetchone()
            if already is not None:
                conflict = self._insert_conflict(
                    cur, batch_id, station, vote_id, summary,
                    "STATION_ALREADY_VOTED")
                return VoteResult(
                    DECISION_CONFLICT, False, conflict["reason"], conflict,
                    False, self._state_from_row(
                        cur, cur.execute(
                            "SELECT * FROM batches WHERE batch_id=?",
                            (batch_id,)).fetchone()))

            # 5) 首次有效投票：与赞成票写入同事务原子冻结。
            sealed_now = False
            if row["status"] == STATUS_AWAITING:
                frozen_at = _utcnow()
                cur.execute(
                    """UPDATE batches
                       SET status=?, frozen_summary=?, frozen_at=?,
                           version=version+1
                       WHERE batch_id=? AND status=?""",
                    (STATUS_COLLECTING, summary, frozen_at,
                     batch_id, STATUS_AWAITING))
                if cur.rowcount == 0:
                    # 并发下被另一站点抢先冻结：按其冻结摘要重新判定。
                    row = self._fetch_batch(cur, batch_id)
                    if summary != row["frozen_summary"]:
                        conflict = self._insert_conflict(
                            cur, batch_id, station, vote_id, summary,
                            "SUMMARY_MISMATCH")
                        return VoteResult(
                            DECISION_CONFLICT, False, conflict["reason"],
                            conflict, False, self._state_from_row(cur, row))

            # 6) 记赞成票（唯一索引兜底防并发重复）。
            now = _utcnow()
            try:
                cur.execute(
                    """INSERT INTO votes(batch_id, station, vote_id, summary,
                                         decision, reason, created_at)
                       VALUES(?,?,?,?,'APPROVE',NULL,?)""",
                    (batch_id, station, vote_id, summary, now))
            except sqlite3.IntegrityError:
                conflict = self._insert_conflict(
                    cur, batch_id, station, vote_id, summary,
                    "STATION_ALREADY_VOTED")
                return VoteResult(
                    DECISION_CONFLICT, False, conflict["reason"], conflict,
                    False, self._state_from_row(
                        cur, self._fetch_batch(cur, batch_id)))

            # 7) 达到阈值：同一事务内封签，条件更新保证只出一份证书。
            approves = cur.execute(
                """SELECT station FROM votes
                   WHERE batch_id=? AND decision='APPROVE'
                   ORDER BY id ASC""", (batch_id,)).fetchall()
            approve_stations = [r["station"] for r in approves]
            latest = self._fetch_batch(cur, batch_id)
            if len(approve_stations) >= latest["threshold"] \
                    and latest["status"] == STATUS_COLLECTING:
                sealed_at = _utcnow()
                cur.execute(
                    """UPDATE batches SET status=?, sealed_at=?,
                           version=version+1
                       WHERE batch_id=? AND status=?""",
                    (STATUS_SEALED, sealed_at, batch_id, STATUS_COLLECTING))
                if cur.rowcount == 1:
                    self._insert_certificate(
                        cur, latest, approve_stations, sealed_at)
                    sealed_now = True

            final_row = self._fetch_batch(cur, batch_id)
            vote_row = cur.execute(
                """SELECT * FROM votes
                   WHERE batch_id=? AND station=? AND decision='APPROVE'
                   ORDER BY id DESC LIMIT 1""",
                (batch_id, station)).fetchone()
            return VoteResult(
                DECISION_APPROVE, False, None, self._vote_dict(vote_row),
                sealed_now, self._state_from_row(cur, final_row))

    def _insert_conflict(self, cur: sqlite3.Cursor, batch_id: str,
                         station: str, vote_id: str, summary: str,
                         reason: str) -> dict[str, Any]:
        now = _utcnow()
        cur.execute(
            """INSERT INTO votes(batch_id, station, vote_id, summary,
                                 decision, reason, created_at)
               VALUES(?,?,?,?,'CONFLICT',?,?)""",
            (batch_id, station, vote_id, summary, reason, now))
        cur.execute(
            "UPDATE batches SET version=version+1 WHERE batch_id=?",
            (batch_id,))
        return {
            "batch_id": batch_id, "station": station, "vote_id": vote_id,
            "summary": summary, "decision": DECISION_CONFLICT,
            "reason": reason, "created_at": now,
        }

    # ------------------------------------------------------------------
    # 封签
    # ------------------------------------------------------------------
    def _insert_certificate(self, cur: sqlite3.Cursor, batch: sqlite3.Row,
                            approving_stations: list[str],
                            sealed_at: str) -> None:
        stations = json.loads(batch["stations_json"])
        payload = {
            "batch_id": batch["batch_id"],
            "summary": batch["frozen_summary"],
            "threshold": batch["threshold"],
            "stations": stations,
            "approving_stations": approving_stations,
            "frozen_at": batch["frozen_at"],
            "sealed_at": sealed_at,
        }
        digest = _canonical_hash(payload)
        cert_id = "SEAL-" + digest[:16]
        cur.execute(
            """INSERT INTO certificates(batch_id, cert_id, summary, threshold,
                                        stations_json, approving_json,
                                        frozen_at, sealed_at, hash)
               VALUES(?,?,?,?,?,?,?,?,?)""",
            (batch["batch_id"], cert_id, batch["frozen_summary"],
             batch["threshold"], json.dumps(stations, ensure_ascii=False),
             json.dumps(approving_stations, ensure_ascii=False),
             batch["frozen_at"], sealed_at, digest))

    def verify_certificate(self, cert: dict[str, Any]) -> bool:
        """重新计算证书指纹，核验证书内容未被篡改。"""
        payload = {
            "batch_id": cert["batch_id"],
            "summary": cert["summary"],
            "threshold": cert["threshold"],
            "stations": cert["stations"],
            "approving_stations": cert["approving_stations"],
            "frozen_at": cert["frozen_at"],
            "sealed_at": cert["sealed_at"],
        }
        return _canonical_hash(payload) == cert["hash"] \
            and cert["cert_id"] == "SEAL-" + cert["hash"][:16]

    # ------------------------------------------------------------------
    # 状态投影
    # ------------------------------------------------------------------
    @staticmethod
    def _vote_dict(row: sqlite3.Row) -> dict[str, Any]:
        return {
            "id": row["id"], "batch_id": row["batch_id"],
            "station": row["station"], "vote_id": row["vote_id"],
            "summary": row["summary"], "decision": row["decision"],
            "reason": row["reason"], "created_at": row["created_at"],
        }

    def _state_from_row(self, cur: sqlite3.Cursor,
                        row: sqlite3.Row) -> dict[str, Any]:
        vote_rows = cur.execute(
            "SELECT * FROM votes WHERE batch_id=? ORDER BY id ASC",
            (row["batch_id"],)).fetchall()
        votes = [self._vote_dict(r) for r in vote_rows]
        approving = [v["station"] for v in votes
                     if v["decision"] == DECISION_APPROVE]
        conflicts = [v for v in votes if v["decision"] == DECISION_CONFLICT]
        cert_row = cur.execute(
            "SELECT * FROM certificates WHERE batch_id=?",
            (row["batch_id"],)).fetchone()
        cert = None
        if cert_row is not None:
            cert = {
                "batch_id": cert_row["batch_id"],
                "cert_id": cert_row["cert_id"],
                "summary": cert_row["summary"],
                "threshold": cert_row["threshold"],
                "stations": json.loads(cert_row["stations_json"]),
                "approving_stations": json.loads(cert_row["approving_json"]),
                "frozen_at": cert_row["frozen_at"],
                "sealed_at": cert_row["sealed_at"],
                "hash": cert_row["hash"],
                "immutable": True,
            }
        return {
            "batch_id": row["batch_id"],
            "status": row["status"],
            "stations": json.loads(row["stations_json"]),
            "threshold": row["threshold"],
            "frozen_summary": row["frozen_summary"],
            "frozen_at": row["frozen_at"],
            "sealed_at": row["sealed_at"],
            "version": row["version"],
            "approving_stations": approving,
            "approve_count": len(approving),
            "conflicts": conflicts,
            "conflict_stations": sorted({v["station"] for v in conflicts}),
            "certificate": cert,
            "votes": votes,
        }

    def get_state(self, batch_id: str) -> dict[str, Any]:
        with self._tx() as cur:
            return self._state_from_row(cur, self._fetch_batch(cur, batch_id))

    def list_batches(self) -> list[dict[str, Any]]:
        with self._tx() as cur:
            rows = cur.execute(
                "SELECT batch_id, status, version FROM batches ORDER BY batch_id"
            ).fetchall()
            return [dict(r) for r in rows]
