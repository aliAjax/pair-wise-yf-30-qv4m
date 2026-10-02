#!/usr/bin/env python3
"""Pharmacovigilance case intake service using only the Python standard library."""
from __future__ import annotations

import argparse
import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import date, datetime, timedelta, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

PORT = 8201
ROLES = {"reporter", "regional_lead", "medical_reviewer", "global_admin"}


class ApiError(Exception):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(message)
        self.status = status
        self.code = code
        self.message = message


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


def iso(value: datetime | None = None) -> str:
    current = value or utcnow()
    return current.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_time(value: str | None, default: datetime | None = None) -> datetime:
    if not value:
        if default is None:
            raise ApiError(400, "missing_time", "必须提供 ISO 8601 时间")
        return default
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ApiError(400, "invalid_time", f"时间格式错误: {value}") from exc
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def report_deadline(received_at: datetime, serious: bool, fatal: bool) -> datetime:
    if serious:
        return received_at + timedelta(days=7 if fatal else 15)
    return received_at + timedelta(days=90)


class Repository:
    def __init__(self, db_path: str | Path):
        self.db_path = str(db_path)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA busy_timeout=5000")
        self.init_schema()

    @contextmanager
    def tx(self):
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            yield self.conn
            self.conn.execute("COMMIT")
        except Exception:
            self.conn.execute("ROLLBACK")
            raise

    @contextmanager
    def fresh_tx(self):
        """Independent connection + IMMEDIATE transaction.

        A long-lived connection that read a case before waiting on another
        writer can keep a stale WAL read snapshot inside the next write
        transaction. Concurrency-critical backfills run on a brand-new
        connection so they always observe the latest committed revision.
        """
        conn = sqlite3.connect(self.db_path, check_same_thread=False, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA foreign_keys=ON")
        conn.execute("PRAGMA busy_timeout=5000")
        try:
            conn.execute("BEGIN IMMEDIATE")
            yield conn
            conn.execute("COMMIT")
        except Exception:
            conn.execute("ROLLBACK")
            raise
        finally:
            conn.close()

    def init_schema(self) -> None:
        self.conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS cases (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_no TEXT NOT NULL UNIQUE,
                patient_ref TEXT NOT NULL,
                region TEXT NOT NULL,
                product TEXT NOT NULL,
                event_term TEXT NOT NULL,
                onset_at TEXT,
                received_at TEXT NOT NULL,
                serious INTEGER NOT NULL DEFAULT 0,
                fatal INTEGER NOT NULL DEFAULT 0,
                causality TEXT,
                report_due_at TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'open',
                revision INTEGER NOT NULL DEFAULT 1,
                merged_into INTEGER REFERENCES cases(id),
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS intakes (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER REFERENCES cases(id),
                source TEXT NOT NULL,
                dedupe_key TEXT NOT NULL UNIQUE,
                payload_json TEXT NOT NULL,
                received_at TEXT NOT NULL,
                case_revision INTEGER NOT NULL DEFAULT 1,
                batch_id TEXT,
                serious INTEGER,
                fatal INTEGER,
                status TEXT NOT NULL DEFAULT 'applied',
                superseded_by TEXT,
                processed_at TEXT,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS followups (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL REFERENCES cases(id),
                content TEXT NOT NULL,
                source TEXT NOT NULL,
                received_at TEXT NOT NULL,
                revision INTEGER NOT NULL,
                serious INTEGER,
                fatal INTEGER,
                created_by TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(case_id, revision)
            );
            CREATE TABLE IF NOT EXISTS reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL REFERENCES cases(id),
                country TEXT NOT NULL,
                version INTEGER NOT NULL DEFAULT 1,
                supersedes_report_id INTEGER REFERENCES reports(id),
                due_at TEXT NOT NULL,
                basis_revision INTEGER NOT NULL DEFAULT 1,
                basis_kind TEXT,
                basis_intake_id INTEGER,
                basis_received_at TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                submitted_at TEXT,
                submitted_by TEXT,
                late INTEGER NOT NULL DEFAULT 0,
                archived INTEGER NOT NULL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS medical_reviews (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER NOT NULL REFERENCES cases(id),
                case_revision INTEGER NOT NULL,
                serious INTEGER NOT NULL,
                fatal INTEGER NOT NULL,
                causality TEXT NOT NULL,
                rationale TEXT NOT NULL,
                received_at TEXT,
                reviewer TEXT NOT NULL,
                created_at TEXT NOT NULL,
                UNIQUE(case_id, case_revision)
            );
            CREATE TABLE IF NOT EXISTS audit_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                case_id INTEGER,
                actor TEXT NOT NULL,
                role TEXT NOT NULL,
                action TEXT NOT NULL,
                detail_json TEXT NOT NULL,
                created_at TEXT NOT NULL
            );
            """
        )
        self._migrate_schema()

    def _migrate_schema(self) -> None:
        """Add columns introduced after the initial prototype; rebuild reports if it
        still carries the legacy UNIQUE(case_id,country) constraint so that
        archived originals and new report versions can coexist."""
        columns = {
            "intakes": {
                "case_revision": "INTEGER NOT NULL DEFAULT 1",
                "batch_id": "TEXT",
                "serious": "INTEGER",
                "fatal": "INTEGER",
                "status": "TEXT NOT NULL DEFAULT 'applied'",
                "superseded_by": "TEXT",
                "processed_at": "TEXT",
            },
            "followups": {"serious": "INTEGER", "fatal": "INTEGER"},
            "medical_reviews": {"received_at": "TEXT"},
            "reports": {
                "version": "INTEGER NOT NULL DEFAULT 1",
                "supersedes_report_id": "INTEGER",
                "basis_revision": "INTEGER NOT NULL DEFAULT 1",
                "basis_kind": "TEXT",
                "basis_intake_id": "INTEGER",
                "basis_received_at": "TEXT",
                "archived": "INTEGER NOT NULL DEFAULT 0",
            },
        }
        for table, cols in columns.items():
            existing = {row["name"] for row in self.conn.execute(f"PRAGMA table_info({table})")}
            for name, ddl in cols.items():
                if name not in existing:
                    self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")
        ddl = self.conn.execute(
            "SELECT sql FROM sqlite_master WHERE type='table' AND name='reports'"
        ).fetchone()
        if ddl and "unique(case_id,country)" in ddl["sql"].replace(" ", "").lower():
            self.conn.executescript(
                """
                ALTER TABLE reports RENAME TO reports_legacy;
                CREATE TABLE reports (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    case_id INTEGER NOT NULL REFERENCES cases(id),
                    country TEXT NOT NULL,
                    version INTEGER NOT NULL DEFAULT 1,
                    supersedes_report_id INTEGER REFERENCES reports(id),
                    due_at TEXT NOT NULL,
                    basis_revision INTEGER NOT NULL DEFAULT 1,
                    basis_kind TEXT,
                    basis_intake_id INTEGER,
                    basis_received_at TEXT,
                    status TEXT NOT NULL DEFAULT 'pending',
                    submitted_at TEXT,
                    submitted_by TEXT,
                    late INTEGER NOT NULL DEFAULT 0,
                    archived INTEGER NOT NULL DEFAULT 0
                );
                INSERT INTO reports(id,case_id,country,version,due_at,basis_revision,status,submitted_at,
                                    submitted_by,late,archived)
                    SELECT id,case_id,country,1,due_at,basis_revision,status,submitted_at,submitted_by,late,0
                    FROM reports_legacy;
                DROP TABLE reports_legacy;
                CREATE UNIQUE INDEX reports_active_country_uk
                    ON reports(case_id, country) WHERE archived=0;
                """
            )
        else:
            self.conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS reports_active_country_uk "
                "ON reports(case_id, country) WHERE archived=0"
            )

    @staticmethod
    def audit(conn: sqlite3.Connection, case_id: int | None, actor: str, role: str, action: str, detail: dict[str, Any]) -> None:
        conn.execute(
            "INSERT INTO audit_log(case_id,actor,role,action,detail_json,created_at) VALUES(?,?,?,?,?,?)",
            (case_id, actor, role, action, json.dumps(detail, ensure_ascii=False, sort_keys=True), iso()),
        )

    @staticmethod
    def row(row: sqlite3.Row | None) -> dict[str, Any] | None:
        return dict(row) if row is not None else None


# Process-wide per-case lock registry, keyed by (database path, case id).
# Multiple service instances against the same file (e.g. worker threads with
# their own connection) must share the same lock for a case.
_CASE_LOCKS: dict[tuple[str, int], threading.Lock] = {}
_CASE_LOCKS_GUARD = threading.Lock()


class PharmacovigilanceService:
    def __init__(self, db_path: str | Path):
        self.repo = Repository(db_path)

    def _case_lock(self, case_id: int) -> threading.Lock:
        key = (self.repo.db_path, case_id)
        with _CASE_LOCKS_GUARD:
            lock = _CASE_LOCKS.get(key)
            if lock is None:
                lock = threading.Lock()
                _CASE_LOCKS[key] = lock
            return lock

    @contextmanager
    def _case_tx(self, case_id: int):
        """Hold the per-case mutex across the whole DB transaction so a
        concurrent mutator re-reads the committed revision instead of racing."""
        lock = self._case_lock(case_id)
        lock.acquire()
        try:
            with self.repo.fresh_tx() as conn:
                yield conn
        finally:
            lock.release()

    @staticmethod
    def identity(headers: Any) -> tuple[str, str, str]:
        actor = headers.get("X-User-Id", "").strip()
        role = headers.get("X-Role", "").strip()
        region = headers.get("X-Region", "").strip()
        if not actor or role not in ROLES:
            raise ApiError(401, "unauthorized", "需要 X-User-Id 和有效的 X-Role")
        if role in {"reporter", "regional_lead"} and not region:
            raise ApiError(401, "region_required", "该角色必须提供 X-Region")
        return actor, role, region

    @staticmethod
    def can_access(case: dict[str, Any], role: str, region: str) -> bool:
        return role in {"medical_reviewer", "global_admin"} or case["region"] == region

    def _case(self, conn: sqlite3.Connection, case_id: int) -> sqlite3.Row:
        row = conn.execute("SELECT * FROM cases WHERE id=?", (case_id,)).fetchone()
        if not row:
            raise ApiError(404, "case_not_found", "案例不存在")
        return row

    def create_case(self, actor: str, role: str, region: str, body: dict[str, Any]) -> dict[str, Any]:
        required = ("patient_ref", "region", "product", "event_term", "source", "dedupe_key")
        missing = [key for key in required if not str(body.get(key, "")).strip()]
        if missing:
            raise ApiError(400, "missing_fields", f"缺少字段: {', '.join(missing)}")
        if role == "reporter" and body["region"] != region:
            raise ApiError(403, "region_forbidden", "只能录入本区域案例")
        if role == "medical_reviewer" and body["region"] not in {"", region}:
            raise ApiError(403, "reviewer_region_forbidden", "医学审核员不能代表区域录入案例")
        received = parse_time(body.get("received_at"), utcnow())
        serious = bool(body.get("serious", False))
        fatal = bool(body.get("fatal", False))
        due = report_deadline(received, serious, fatal)
        now = iso()
        with self.repo.tx() as conn:
            duplicate = conn.execute("SELECT * FROM intakes WHERE dedupe_key=?", (body["dedupe_key"],)).fetchone()
            if duplicate:
                case = self._case(conn, duplicate["case_id"])
                Repository.audit(conn, case["id"], actor, role, "intake_deduplicated", {"dedupe_key": body["dedupe_key"], "source": body["source"]})
                return {"deduplicated": True, "case": dict(case), "intake_id": duplicate["id"]}
            count = conn.execute("SELECT COUNT(*) FROM cases").fetchone()[0] + 1
            case_no = body.get("case_no") or f"PV-{received.year}-{count:06d}"
            try:
                cursor = conn.execute(
                    """INSERT INTO cases(case_no,patient_ref,region,product,event_term,onset_at,received_at,
                       serious,fatal,causality,report_due_at,status,revision,created_by,created_at,updated_at)
                       VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                    (case_no, body["patient_ref"], body["region"], body["product"], body["event_term"],
                     body.get("onset_at"), iso(received), int(serious), int(fatal), body.get("causality"),
                     iso(due), "open", 1, actor, now, now),
                )
            except sqlite3.IntegrityError as exc:
                raise ApiError(409, "case_number_conflict", "案例编号已存在") from exc
            case_id = cursor.lastrowid
            conn.execute(
                """INSERT INTO intakes(case_id,source,dedupe_key,payload_json,received_at,case_revision,
                   serious,fatal,status,processed_at,created_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?,'applied',?,?,?)""",
                (case_id, body["source"], body["dedupe_key"], json.dumps(body, ensure_ascii=False, sort_keys=True),
                 iso(received), 1, int(serious), int(fatal), iso(received), actor, now),
            )
            Repository.audit(conn, case_id, actor, role, "case_created", {"case_no": case_no, "source": body["source"]})
            case = self._case(conn, case_id)
            return {"deduplicated": False, "case": dict(case)}

    def get_case(self, case_id: int, role: str, region: str) -> dict[str, Any]:
        case = self._case(self.repo.conn, case_id)
        if not self.can_access(case, role, region):
            raise ApiError(403, "case_forbidden", "无权查看该区域案例")
        conn = self.repo.conn
        return {
            "case": dict(case),
            "intakes": [dict(r) for r in conn.execute(
                "SELECT id,source,dedupe_key,received_at,case_revision,batch_id,serious,fatal,status,"
                "superseded_by,processed_at,created_by,created_at FROM intakes WHERE case_id=? ORDER BY id",
                (case_id,))],
            "followups": [dict(r) for r in conn.execute("SELECT * FROM followups WHERE case_id=? ORDER BY revision", (case_id,))],
            "reports": [dict(r) for r in conn.execute("SELECT * FROM reports WHERE case_id=? ORDER BY country", (case_id,))],
            "reviews": [dict(r) for r in conn.execute("SELECT * FROM medical_reviews WHERE case_id=? ORDER BY id", (case_id,))],
            "audit": [dict(r) for r in conn.execute("SELECT actor,role,action,detail_json,created_at FROM audit_log WHERE case_id=? ORDER BY id", (case_id,))] if role in {"medical_reviewer", "global_admin"} else [],
        }

    def list_cases(self, role: str, region: str, query: dict[str, list[str]]) -> list[dict[str, Any]]:
        sql = "SELECT * FROM cases WHERE status!='merged'"
        args: list[Any] = []
        if role not in {"medical_reviewer", "global_admin"}:
            sql += " AND region=?"
            args.append(region)
        if query.get("status"):
            sql += " AND status=?"
            args.append(query["status"][0])
        sql += " ORDER BY received_at DESC,id DESC"
        return [dict(r) for r in self.repo.conn.execute(sql, args)]

    def add_followup(self, case_id: int, actor: str, role: str, region: str, body: dict[str, Any]) -> dict[str, Any]:
        content = str(body.get("content", "")).strip()
        source = str(body.get("source", "")).strip()
        if not content or not source:
            raise ApiError(400, "missing_fields", "content 和 source 必填")
        expected = body.get("expected_revision")
        if not isinstance(expected, int):
            raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        serious = body.get("serious")
        fatal = body.get("fatal", False)
        if serious is not None and (not isinstance(serious, bool) or not isinstance(fatal, bool)):
            raise ApiError(400, "invalid_severity", "serious/fatal 必须是布尔值")
        if fatal and serious is not None and not serious:
            raise ApiError(400, "invalid_severity", "死亡案例必须标记为严重")
        with self.repo.tx() as conn:
            case = self._case(conn, case_id)
            if not self.can_access(case, role, region) or role in {"medical_reviewer"}:
                raise ApiError(403, "followup_forbidden", "当前角色不能提交随访")
            if case["status"] == "merged":
                raise ApiError(409, "case_merged", "已合并案例不能再更新")
            if case["revision"] != expected:
                raise ApiError(409, "revision_conflict", "案例已被其他人员更新，请重新读取")
            revision = case["revision"] + 1
            received = parse_time(body.get("received_at"), utcnow())
            new_serious = int(serious) if serious is not None else case["serious"]
            new_fatal = int(fatal) if serious is not None else case["fatal"]
            due = report_deadline(received, bool(new_serious), bool(new_fatal))
            conn.execute(
                """INSERT INTO followups(case_id,content,source,received_at,revision,serious,fatal,created_by,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (case_id, content, source, iso(received), revision,
                 int(serious) if serious is not None else None,
                 int(fatal) if serious is not None else None, actor, iso()),
            )
            conn.execute(
                "UPDATE cases SET revision=?,received_at=?,report_due_at=?,updated_at=? WHERE id=?",
                (revision, iso(received), iso(due), iso(), case_id),
            )
            if serious is not None:
                conn.execute("UPDATE cases SET serious=?,fatal=? WHERE id=?", (new_serious, new_fatal, case_id))
            # Every follow-up restarts the reporting clock, even when it does not
            # change severity, so case and report deadlines stay reconcilable.
            self._refresh_report_basis(
                conn, case_id, revision, "followup", None, received,
            )
            self._reclassify_sources(conn, case_id)
            Repository.audit(conn, case_id, actor, role, "followup_added",
                             {"revision": revision, "source": source,
                              "serious": serious if serious is not None else "unchanged"})
            return {"case": dict(self._case(conn, case_id)), "revision": revision}

    def medical_review(self, case_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "medical_reviewer":
            raise ApiError(403, "medical_reviewer_required", "只有医学审核员可以裁定严重性")
        expected = body.get("expected_revision")
        if not isinstance(expected, int):
            raise ApiError(400, "revision_required", "expected_revision 必须是整数")
        serious = body.get("serious")
        fatal = body.get("fatal")
        causality = str(body.get("causality", "")).strip()
        rationale = str(body.get("rationale", "")).strip()
        if not isinstance(serious, bool) or not isinstance(fatal, bool) or not causality or not rationale:
            raise ApiError(400, "invalid_review", "serious/fatal 必须是布尔值，causality 和 rationale 必填")
        if fatal and not serious:
            raise ApiError(400, "invalid_severity", "死亡案例必须标记为严重")
        received = parse_time(body.get("received_at"))
        due = report_deadline(received, serious, fatal)
        with self.repo.tx() as conn:
            case = self._case(conn, case_id)
            if case["status"] == "merged":
                raise ApiError(409, "case_merged", "已合并案例不能审核")
            if case["revision"] != expected:
                raise ApiError(409, "revision_conflict", "案例版本已变化")
            revision = expected + 1
            conn.execute(
                """UPDATE cases SET serious=?,fatal=?,causality=?,received_at=?,report_due_at=?,revision=?,updated_at=? WHERE id=?""",
                (int(serious), int(fatal), causality, iso(received), iso(due), revision, iso(), case_id),
            )
            conn.execute(
                """INSERT INTO medical_reviews(case_id,case_revision,serious,fatal,causality,rationale,
                   received_at,reviewer,created_at)
                   VALUES(?,?,?,?,?,?,?,?,?)""",
                (case_id, expected, int(serious), int(fatal), causality, rationale, iso(received), actor, iso()),
            )
            self._refresh_report_basis(
                conn, case_id, revision, "medical_review", None, received,
            )
            self._reclassify_sources(conn, case_id)
            Repository.audit(conn, case_id, actor, role, "medical_reviewed", {"from_revision": expected, "serious": serious, "fatal": fatal, "causality": causality})
            return {"case": dict(self._case(conn, case_id)), "reviewed_revision": expected}

    def create_report(self, case_id: int, actor: str, role: str, region: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"regional_lead", "global_admin"}:
            raise ApiError(403, "report_forbidden", "只有区域负责人或全局管理员可以生成报告")
        country = str(body.get("country", "")).strip().upper()
        if not country:
            raise ApiError(400, "country_required", "country 必填")
        with self.repo.tx() as conn:
            case = self._case(conn, case_id)
            if not self.can_access(case, role, region):
                raise ApiError(403, "region_forbidden", "不能为本区域之外案例生成报告")
            effective = self._effective_state(conn, case_id)
            existing = conn.execute(
                "SELECT id,status,archived FROM reports WHERE case_id=? AND country=? AND archived=0",
                (case_id, country),
            ).fetchone()
            if existing:
                raise ApiError(409, "report_exists", "该国家报告已经存在")
            clock = parse_time(effective["basis_received_at"])
            due = report_deadline(clock, bool(effective["serious"]), bool(effective["fatal"]))
            cur = conn.execute(
                """INSERT INTO reports(case_id,country,version,due_at,basis_revision,basis_kind,basis_intake_id,
                   basis_received_at,status)
                   VALUES(?,?,1,?,?,?,?,?,?)""",
                (case_id, country, iso(due), effective["revision"], effective["kind"],
                 effective.get("intake_id"), effective["basis_received_at"], "pending"),
            )
            Repository.audit(conn, case_id, actor, role, "report_created", {"report_id": cur.lastrowid, "country": country})
            return dict(conn.execute("SELECT * FROM reports WHERE id=?", (cur.lastrowid,)).fetchone())

    def submit_report(self, report_id: int, actor: str, role: str, region: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"regional_lead", "global_admin"}:
            raise ApiError(403, "submit_forbidden", "当前角色不能提交监管报告")
        with self.repo.tx() as conn:
            row = conn.execute("SELECT r.*,c.region FROM reports r JOIN cases c ON c.id=r.case_id WHERE r.id=?", (report_id,)).fetchone()
            if not row:
                raise ApiError(404, "report_not_found", "报告不存在")
            if not self.can_access(dict(row), role, region):
                raise ApiError(403, "region_forbidden", "无权提交其他区域报告")
            if row["archived"]:
                raise ApiError(409, "report_archived", "该原稿已被新依据取代，只能提交待重报版本")
            if row["status"] == "submitted":
                return {"report": dict(row), "idempotent": True}
            now = parse_time(body.get("submitted_at"), utcnow())
            late = int(now > parse_time(row["due_at"]))
            conn.execute(
                "UPDATE reports SET status='submitted',submitted_at=?,submitted_by=?,late=? WHERE id=?",
                (iso(now), actor, late, report_id),
            )
            Repository.audit(conn, row["case_id"], actor, role, "report_submitted", {"report_id": report_id, "country": row["country"], "late": bool(late)})
            return {"report": dict(conn.execute("SELECT * FROM reports WHERE id=?", (report_id,)).fetchone()), "idempotent": False}

    def merge_cases(self, source_id: int, actor: str, role: str, body: dict[str, Any]) -> dict[str, Any]:
        if role != "global_admin":
            raise ApiError(403, "merge_forbidden", "只有全局管理员可以合并案例")
        target_id = body.get("target_case_id")
        if not isinstance(target_id, int) or source_id == target_id:
            raise ApiError(400, "invalid_target", "target_case_id 必须指向不同案例")
        with self.repo.tx() as conn:
            source = self._case(conn, source_id)
            target = self._case(conn, target_id)
            if source["status"] == "merged":
                return {"case": dict(source), "idempotent": True}
            if target["status"] == "merged" or source["product"].casefold() != target["product"].casefold():
                raise ApiError(409, "merge_conflict", "目标案例不可用，或产品与来源案例不一致")
            conn.execute("UPDATE cases SET status='merged',merged_into=?,revision=revision+1,updated_at=? WHERE id=?", (target_id, iso(), source_id))
            conn.execute("UPDATE intakes SET case_id=? WHERE case_id=?", (target_id, source_id))
            Repository.audit(conn, target_id, actor, role, "case_merged_in", {"source_case_id": source_id})
            Repository.audit(conn, source_id, actor, role, "case_merged_into", {"target_case_id": target_id})
            return {"case": dict(self._case(conn, source_id)), "idempotent": False}

    def overdue(self, role: str, region: str) -> list[dict[str, Any]]:
        sql = "SELECT * FROM reports WHERE archived=0 AND status!='submitted' AND due_at < ?"
        args: list[Any] = [iso()]
        if role not in {"medical_reviewer", "global_admin"}:
            sql += " AND case_id IN (SELECT id FROM cases WHERE region=?)"
            args.append(region)
        return [dict(r) for r in self.repo.conn.execute(sql, args)]

    def escalate_overdue(self, actor: str, role: str, region: str) -> dict[str, Any]:
        if role not in {"regional_lead", "global_admin"}:
            raise ApiError(403, "escalation_forbidden", "当前角色不能执行逾期升级")
        rows = self.overdue(role, region)
        with self.repo.tx() as conn:
            for row in rows:
                conn.execute(
                    "UPDATE reports SET status='overdue' WHERE id=? AND status IN ('pending','resubmission_required')",
                    (row["id"],))
                Repository.audit(conn, row["case_id"], actor, role, "report_overdue_escalated", {"report_id": row["id"], "country": row["country"]})
        return {"escalated": len(rows)}

    # ------------------------------------------------------------------
    # Source-history reconciliation
    # ------------------------------------------------------------------

    def _gather_events(self, conn: sqlite3.Connection, case_id: int) -> list[dict[str, Any]]:
        """All severity-bearing events on a case, ordered as they occurred.

        Rank encodes precedence at equal revision/time: medical adjudication
        outranks follow-up, which outranks an intake channel source. A follow-up
        that did not itself state a severity still anchors the reporting clock;
        after ordering it inherits the severity then in force.
        """
        case_row = conn.execute("SELECT serious,fatal FROM cases WHERE id=?", (case_id,)).fetchone()
        seed_serious = bool(case_row["serious"]) if case_row else False
        seed_fatal = bool(case_row["fatal"]) if case_row else False
        events: list[dict[str, Any]] = []
        for row in conn.execute("SELECT * FROM intakes WHERE case_id=? AND serious IS NOT NULL", (case_id,)):
            # Superseded channel sources are retained for audit but never provide
            # the severity that later follow-ups inherit.
            if row["status"] == "superseded":
                continue
            events.append({
                "kind": "intake", "id": row["id"], "rev": row["case_revision"],
                "received_at": parse_time(row["received_at"]),
                "rank": 1, "serious": bool(row["serious"]), "fatal": bool(row["fatal"] or 0),
                "intake_id": row["id"], "explicit": True,
            })
        for row in conn.execute("SELECT * FROM followups WHERE case_id=? ORDER BY revision", (case_id,)):
            events.append({
                "kind": "followup", "id": row["id"], "rev": row["revision"],
                "received_at": parse_time(row["received_at"]),
                "rank": 2,
                "serious": bool(row["serious"]) if row["serious"] is not None else None,
                "fatal": bool(row["fatal"]) if row["serious"] is not None else None,
                "intake_id": None, "explicit": row["serious"] is not None,
            })
        for row in conn.execute("SELECT * FROM medical_reviews WHERE case_id=? ORDER BY id", (case_id,)):
            # Rows written before received_at was tracked fall back to creation time.
            received_at = parse_time(row["received_at"], parse_time(row["created_at"]))
            events.append({
                "kind": "medical_review", "id": row["id"], "rev": row["case_revision"] + 1,
                "received_at": received_at,
                "rank": 3, "serious": bool(row["serious"]), "fatal": bool(row["fatal"]),
                "intake_id": None, "explicit": True, "causality": row["causality"],
            })
        # Walk the ordered timeline; follow-ups without a stated severity inherit
        # the severity last explicitly in force (defaulting to the case seed).
        events.sort(key=lambda e: (e["rev"], e["received_at"], e["rank"], e["id"]))
        last_serious: bool | None = None
        last_fatal: bool | None = None
        for event in events:
            if event["explicit"]:
                last_serious, last_fatal = event["serious"], event["fatal"]
            else:
                event["serious"] = last_serious if last_serious is not None else seed_serious
                event["fatal"] = last_fatal if last_fatal is not None else seed_fatal
        return events

    def _effective_state(self, conn: sqlite3.Connection, case_id: int) -> dict[str, Any]:
        """Locate the severity that applies to the case by revision + time.

        A late channel source can never override a follow-up or medical
        adjudication that already sits on the timeline at a later revision
        (or later within the same revision).
        """
        events = self._gather_events(conn, case_id)
        if not events:
            case = self._case(conn, case_id)
            return {"revision": case["revision"], "kind": "case", "id": None, "intake_id": None,
                    "serious": bool(case["serious"]), "fatal": bool(case["fatal"]),
                    "basis_received_at": case["received_at"]}
        winner = events[-1]
        case = self._case(conn, case_id)
        return {"revision": case["revision"], "kind": winner["kind"], "id": winner["id"],
                "intake_id": winner.get("intake_id"),
                "serious": winner["serious"], "fatal": winner["fatal"],
                "causality": winner.get("causality"),
                "basis_received_at": iso(winner["received_at"])}

    @staticmethod
    def _superseded_by(events: list[dict[str, Any]], intake: sqlite3.Row) -> str | None:
        """Return the authority that supersedes an intake source, if any.

        Only follow-ups and medical adjudication can supersede a channel
        source; later intakes do not void earlier ones (both stay visible).
        """
        anchor = parse_time(intake["received_at"])
        for event in events:
            if event["kind"] == "intake":
                continue
            after = event["rev"] > intake["case_revision"] or (
                event["rev"] == intake["case_revision"]
                and (event["received_at"] > anchor
                     or (event["received_at"] == anchor and event["id"] > intake["id"]))
            )
            if after:
                return f"{event['kind']}:{event['id']}"
        return None

    def _reclassify_sources(self, conn: sqlite3.Connection, case_id: int) -> None:
        """Mark every severity-bearing source applied or superseded against the
        authoritative timeline. Must run after any follow-up, medical review or
        backfill changes the timeline so source history stays reconcilable."""
        events = self._gather_events(conn, case_id)
        for row in conn.execute("SELECT * FROM intakes WHERE case_id=? AND serious IS NOT NULL", (case_id,)):
            marker = self._superseded_by(events, row)
            status = "superseded" if marker else "applied"
            if row["status"] != status or row["superseded_by"] != marker:
                conn.execute("UPDATE intakes SET status=?,superseded_by=? WHERE id=?",
                             (status, marker, row["id"]))

    def _refresh_report_basis(self, conn: sqlite3.Connection, case_id: int, basis_revision: int,
                              basis_kind: str, basis_intake_id: int | None,
                              basis_received: datetime) -> dict[str, Any]:
        """Recompute deadlines against the current effective basis.

        Active reports not yet submitted get their deadline recomputed
        immediately; a submitted active report keeps its original draft row
        (archived, untouched) and spawns a new resubmission-required version.
        """
        case = conn.execute("SELECT * FROM cases WHERE id=?", (case_id,)).fetchone()
        if not case:
            raise ApiError(404, "case_not_found", "案例不存在")
        due = report_deadline(basis_received, bool(case["serious"]), bool(case["fatal"]))
        conn.execute("UPDATE cases SET report_due_at=? WHERE id=?", (iso(due), case_id))
        recomputed: list[int] = []
        resubmitted: list[dict[str, Any]] = []
        active = conn.execute(
            "SELECT * FROM reports WHERE case_id=? AND archived=0 ORDER BY id", (case_id,)
        ).fetchall()
        for report in active:
            basis = (basis_revision, basis_kind, basis_intake_id, iso(basis_received))
            if report["status"] == "submitted":
                # Keep the submitted original intact; open a new version against the new basis.
                conn.execute("UPDATE reports SET archived=1 WHERE id=?", (report["id"],))
                cur = conn.execute(
                    """INSERT INTO reports(case_id,country,version,supersedes_report_id,due_at,basis_revision,
                       basis_kind,basis_intake_id,basis_received_at,status,archived)
                       VALUES(?,?,?,?,?,?,?,?,?, 'resubmission_required',0)""",
                    (case_id, report["country"], report["version"] + 1, report["id"], iso(due), *basis),
                )
                resubmitted.append({"new_report_id": cur.lastrowid, "country": report["country"],
                                    "supersedes_report_id": report["id"], "version": report["version"] + 1})
            else:
                conn.execute(
                    """UPDATE reports SET due_at=?,basis_revision=?,basis_kind=?,basis_intake_id=?,
                       basis_received_at=? WHERE id=?""",
                    (iso(due), *basis, report["id"]),
                )
                recomputed.append(report["id"])
        return {"due_at": iso(due), "recomputed": recomputed, "resubmitted": resubmitted}

    @staticmethod
    def _validate_source_item(body: dict[str, Any]) -> dict[str, Any]:
        source = str(body.get("source", "")).strip()
        dedupe_key = str(body.get("dedupe_key", "")).strip()
        if not source or not dedupe_key:
            raise ApiError(400, "missing_fields", "source 和 dedupe_key 必填")
        expected = body.get("expected_revision")
        if not isinstance(expected, int):
            raise ApiError(400, "revision_required", "expected_revision 必须是案例修订号（整数）")
        received = parse_time(body.get("received_at"))
        serious = body.get("serious")
        fatal = body.get("fatal", False)
        if not isinstance(serious, bool) or not isinstance(fatal, bool):
            raise ApiError(400, "invalid_severity", "serious/fatal 必须是布尔值")
        if fatal and not serious:
            raise ApiError(400, "invalid_severity", "死亡案例必须标记为严重")
        return {"source": source, "dedupe_key": dedupe_key, "expected_revision": expected,
                "received": received, "serious": serious, "fatal": fatal,
                "batch_id": str(body.get("batch_id", "")).strip() or None,
                "payload": body.get("payload") if isinstance(body.get("payload"), dict) else {}}

    def _apply_source_item(self, conn: sqlite3.Connection, case_id: int, actor: str, role: str,
                           item: dict[str, Any], now: str) -> dict[str, Any]:
        case = self._case(conn, case_id)
        if case["status"] == "merged":
            raise ApiError(409, "case_merged", "已合并案例不能再补录来源")
        if item["expected_revision"] > case["revision"]:
            raise ApiError(409, "revision_conflict",
                           f"修订号不匹配：案例当前为 {case['revision']}，补录基于 {item['expected_revision']}，请重新读取")
        cursor = conn.execute(
            """INSERT INTO intakes(case_id,source,dedupe_key,payload_json,received_at,case_revision,batch_id,
               serious,fatal,status,processed_at,created_by,created_at)
               VALUES(?,?,?,?,?,?,?,?,?,'applied',?,?,?)""",
            (case_id, item["source"], item["dedupe_key"],
             json.dumps(item["payload"], ensure_ascii=False, sort_keys=True),
             iso(item["received"]), item["expected_revision"], item["batch_id"],
             int(item["serious"]), int(item["fatal"]), now, actor, now),
        )
        intake_id = cursor.lastrowid

        # Re-classify every source against the authoritative timeline first, then
        # recompute the effective basis using the resulting applied/superseded
        # markers (a newly superseded source must not drive inheritance).
        self._reclassify_sources(conn, case_id)
        effective = self._effective_state(conn, case_id)

        intake = conn.execute("SELECT * FROM intakes WHERE id=?", (intake_id,)).fetchone()
        changed = False
        impact: dict[str, Any] | None = None
        current = conn.execute("SELECT * FROM cases WHERE id=?", (case_id,)).fetchone()
        is_winner = effective["kind"] == "intake" and effective["intake_id"] == intake_id
        if is_winner and item["expected_revision"] != current["revision"]:
            # Another concurrent backfill already bumped the case off the
            # expected revision: conflict and roll this insert back.
            raise ApiError(409, "revision_conflict",
                           f"修订号不匹配：案例当前为 {current['revision']}，补录基于 {item['expected_revision']}，"
                           "另一名补录人员已基于该修订号生效，请重新读取")
        if not is_winner and effective["kind"] == "intake" and current["revision"] != item["expected_revision"]:
            # A concurrent intake on the same expected revision won first; the
            # loser must not be silently recorded.
            raise ApiError(409, "revision_conflict",
                           f"另一名补录人员的来源已基于同一修订号生效，案例当前修订号为 {current['revision']}，请重新读取最新案例")
        if is_winner:
            # Pre-commit optimistic lock: bump the revision only if the case is
            # still on the revision this source was anchored at. A concurrent
            # backfill on the same revision commits first; the loser then matches
            # zero rows, rolls back and leaves no source or audit behind.
            cur_update = conn.execute(
                "UPDATE cases SET serious=?,fatal=?,received_at=?,revision=?,updated_at=? WHERE id=? AND revision=?",
                (int(item["serious"]), int(item["fatal"]), iso(item["received"]),
                 item["expected_revision"] + 1, now, case_id, item["expected_revision"]),
            )
            if cur_update.rowcount == 0:
                raise ApiError(409, "revision_conflict",
                               "案例修订号已被其他补录人员改变，请重新读取最新版本")
            before = {"serious": current["serious"], "fatal": current["fatal"],
                      "received_at": current["received_at"], "due": current["report_due_at"]}
            changed = (before["serious"] != int(item["serious"])
                       or before["fatal"] != int(item["fatal"])
                       or before["received_at"] != iso(item["received"]))
            new_revision = item["expected_revision"] + 1
            impact = self._refresh_report_basis(
                conn, case_id, new_revision, "intake", intake_id, item["received"],
            )
            conn.execute("UPDATE intakes SET case_revision=? WHERE id=?", (new_revision, intake_id))
            effective["revision"] = new_revision
            Repository.audit(conn, case_id, actor, role, "source_backfill_applied",
                             {"intake_id": intake_id, "dedupe_key": item["dedupe_key"],
                              "source": item["source"], "original_received_at": iso(item["received"]),
                              "based_on_revision": item["expected_revision"], "revision": new_revision,
                              "changed": changed, "report_impact": impact})
        elif intake["status"] == "superseded":
            # An old source that follow-up or medical adjudication already moved past:
            # kept in history for audit, but it never touches the current conclusion.
            Repository.audit(conn, case_id, actor, role, "source_backfill_superseded",
                             {"intake_id": intake_id, "dedupe_key": item["dedupe_key"],
                              "source": item["source"], "original_received_at": iso(item["received"]),
                              "based_on_revision": item["expected_revision"],
                              "superseded_by": intake["superseded_by"]})
        else:
            # Applied to the history but not the current winner: counted once,
            # kept visible, yet it does not move the current conclusion.
            Repository.audit(conn, case_id, actor, role, "source_backfill_recorded",
                             {"intake_id": intake_id, "dedupe_key": item["dedupe_key"],
                              "source": item["source"], "original_received_at": iso(item["received"]),
                              "based_on_revision": item["expected_revision"],
                              "current_basis": f"{effective['kind']}:{effective['id']}"})
        return {"intake_id": intake_id, "status": intake["status"],
                "superseded_by": intake["superseded_by"], "changed": changed,
                "revision": effective["revision"], "report_impact": impact}

    def backfill_source(self, case_id: int, actor: str, role: str, region: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"reporter", "regional_lead", "global_admin"}:
            raise ApiError(403, "source_forbidden", "当前角色不能补录渠道来源")
        item = self._validate_source_item(body)
        with self._case_tx(case_id) as conn:
            case = self._case(conn, case_id)
            if not self.can_access(case, role, region):
                raise ApiError(403, "region_forbidden", "不能为本区域之外案例补录来源")
            existing = conn.execute(
                "SELECT * FROM intakes WHERE dedupe_key=?", (item["dedupe_key"],)
            ).fetchone()
            if existing:
                return {"idempotent": True, "intake_id": existing["id"], "case_id": existing["case_id"],
                        "status": existing["status"], "superseded_by": existing["superseded_by"],
                        "revision": case["revision"]}
            result = self._apply_source_item(conn, case_id, actor, role, item, iso())
            result["idempotent"] = False
            result["case_id"] = case_id
            return result

    def backfill_batch(self, actor: str, role: str, region: str, body: dict[str, Any]) -> dict[str, Any]:
        if role not in {"reporter", "regional_lead", "global_admin"}:
            raise ApiError(403, "source_forbidden", "当前角色不能补录渠道来源")
        raw_items = body.get("items")
        if not isinstance(raw_items, list) or not raw_items:
            raise ApiError(400, "items_required", "items 必须是非空数组")
        batch_id = str(body.get("batch_id", "")).strip() or f"batch-{iso()}"
        items: list[dict[str, Any]] = []
        for index, raw in enumerate(raw_items):
            if not isinstance(raw, dict):
                raise ApiError(400, "invalid_item", f"第 {index} 项不是 JSON 对象")
            if not isinstance(raw.get("case_id"), int) or raw["case_id"] <= 0:
                raise ApiError(400, "case_id_required", f"第 {index} 项 case_id 必须是整数")
            try:
                items.append(self._validate_source_item(raw))
            except ApiError as exc:
                raise ApiError(exc.status, exc.code, f"第 {index} 项: {exc.message}") from exc

        results: list[dict[str, Any]] = []
        applied = superseded = duplicated = conflicted = failed = 0
        # Each item commits independently so a partial batch can be retried;
        # only sources never processed are inserted (dedupe key guards retries).
        for index, item in enumerate(items):
            item.setdefault("batch_id", batch_id)
            cid = raw_items[index]["case_id"]
            try:
                with self._case_tx(cid) as conn:
                    case = self._case(conn, cid)
                    if not self.can_access(case, role, region):
                        raise ApiError(403, "region_forbidden", "不能为本区域之外案例补录来源")
                    existing = conn.execute(
                        "SELECT * FROM intakes WHERE dedupe_key=?", (item["dedupe_key"],)
                    ).fetchone()
                    if existing:
                        outcome = {"index": index, "ok": True, "idempotent": True,
                                   "intake_id": existing["id"], "case_id": existing["case_id"],
                                   "status": existing["status"], "superseded_by": existing["superseded_by"]}
                        duplicated += 1
                    else:
                        applied_result = self._apply_source_item(conn, cid, actor, role, item, iso())
                        outcome = {"index": index, "ok": True, "idempotent": False,
                                   "case_id": cid, **applied_result}
                        if applied_result["status"] == "superseded":
                            superseded += 1
                        else:
                            applied += 1
            except ApiError as exc:
                outcome = {"index": index, "ok": False, "error": exc.code, "message": exc.message}
                if exc.code == "revision_conflict":
                    conflicted += 1
                else:
                    failed += 1
            results.append(outcome)
        return {"batch_id": batch_id, "processed": len(results), "applied": applied,
                "superseded": superseded, "duplicated": duplicated, "conflicted": conflicted,
                "failed": failed, "items": results}

    def reconcile_case(self, case_id: int, role: str, region: str) -> dict[str, Any]:
        """Cross-check case, source history and each report version.

        Only when all three line up can the valid report be identified.
        """
        case = self._case(self.repo.conn, case_id)
        if not self.can_access(case, role, region):
            raise ApiError(403, "case_forbidden", "无权查看该区域案例")
        conn = self.repo.conn
        effective = self._effective_state(conn, case_id)
        expected_due = report_deadline(
            parse_time(effective["basis_received_at"]), effective["serious"], effective["fatal"]
        )
        mismatches: list[str] = []
        if bool(case["serious"]) != effective["serious"] or bool(case["fatal"]) != effective["fatal"]:
            mismatches.append(
                f"case.severity={case['serious']}/{case['fatal']} 与有效依据 "
                f"{effective['kind']}:{effective['id']} 的严重性不一致")
        if case["received_at"] != effective["basis_received_at"]:
            mismatches.append("案例接收时间与有效依据时间不一致")
        if case["report_due_at"] != iso(expected_due):
            mismatches.append("案例期限与有效依据重算期限不一致")

        sources = [dict(r) for r in conn.execute(
            "SELECT id,source,dedupe_key,received_at,case_revision,batch_id,serious,fatal,status,"
            "superseded_by,processed_at,created_by,created_at FROM intakes WHERE case_id=? ORDER BY id",
            (case_id,))]
        report_rows = [dict(r) for r in conn.execute(
            "SELECT * FROM reports WHERE case_id=? ORDER BY country,version,id", (case_id,))]
        for report in report_rows:
            if report["archived"]:
                continue
            recomputed = report_deadline(
                parse_time(effective["basis_received_at"]), effective["serious"], effective["fatal"])
            if report["due_at"] != iso(recomputed):
                mismatches.append(f"报告 {report['id']}({report['country']} v{report['version']}) 期限与有效依据不一致")
            if report["basis_kind"] != effective["kind"] or report["basis_revision"] != case["revision"]:
                mismatches.append(f"报告 {report['id']}({report['country']}) 依据版本已过期")
        active = [r for r in report_rows if not r["archived"]]
        for country in {r["country"] for r in active}:
            if len([r for r in active if r["country"] == country]) != 1:
                mismatches.append(f"国家 {country} 存在多个有效报告版本，无法判定哪份有效")
        events = self._gather_events(conn, case_id)
        for source in sources:
            if source["serious"] is None:
                continue
            marker = self._superseded_by(events, conn.execute(
                "SELECT * FROM intakes WHERE id=?", (source["id"],)).fetchone())
            expected_status = "superseded" if marker else "applied"
            if source["status"] != expected_status:
                mismatches.append(f"来源 {source['id']} 状态应为 {expected_status}，实际为 {source['status']}")

        return {
            "case_id": case_id,
            "consistent": not mismatches,
            "mismatches": mismatches,
            "current": {
                "revision": case["revision"],
                "serious": effective["serious"], "fatal": effective["fatal"],
                "basis_kind": effective["kind"], "basis_id": effective["id"],
                "basis_intake_id": effective.get("intake_id"),
                "basis_received_at": effective["basis_received_at"],
                "expected_due_at": iso(expected_due),
                "case_received_at": case["received_at"],
                "case_due_at": case["report_due_at"],
            },
            "sources": {
                "total": len(sources),
                "applied": len([s for s in sources if s["status"] == "applied"]),
                "superseded": len([s for s in sources if s["status"] == "superseded"]),
                "items": sources,
            },
            "reports": {
                "active": [r for r in report_rows if not r["archived"]],
                "archived": [r for r in report_rows if r["archived"]],
            },
        }

    def state(self, role: str, region: str) -> dict[str, Any]:
        cases = self.list_cases(role, region, {})
        return {"cases": cases, "overdue": self.overdue(role, region), "server_time": iso()}


def json_response(handler: BaseHTTPRequestHandler, status: int, payload: Any) -> None:
    raw = json.dumps(payload, ensure_ascii=False, default=str).encode("utf-8")
    handler.send_response(status)
    handler.send_header("Content-Type", "application/json; charset=utf-8")
    handler.send_header("Content-Length", str(len(raw)))
    handler.end_headers()
    handler.wfile.write(raw)


class Handler(BaseHTTPRequestHandler):
    service: PharmacovigilanceService
    web_root: Path

    def log_message(self, fmt: str, *args: Any) -> None:
        print(f"{self.address_string()} - {fmt % args}")

    def _body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0"))
        if not length:
            return {}
        try:
            result = json.loads(self.rfile.read(length))
        except json.JSONDecodeError as exc:
            raise ApiError(400, "invalid_json", "请求体不是有效 JSON") from exc
        if not isinstance(result, dict):
            raise ApiError(400, "invalid_json", "请求体必须是 JSON 对象")
        return result

    def _dispatch_get(self, path: str, query: dict[str, list[str]]) -> Any:
        if path == "/health":
            return 200, {"status": "ok", "service": "pharmacovigilance"}
        actor, role, region = self.service.identity(self.headers)
        if path == "/api/state":
            return 200, self.service.state(role, region)
        if path == "/api/cases":
            return 200, {"cases": self.service.list_cases(role, region, query)}
        if path == "/api/overdue":
            return 200, {"reports": self.service.overdue(role, region)}
        parts = [part for part in path.split("/") if part]
        if len(parts) == 4 and parts[:2] == ["api", "cases"] and parts[2].isdigit() and parts[3] == "reconcile":
            return 200, self.service.reconcile_case(int(parts[2]), role, region)
        if len(parts) == 3 and parts[:2] == ["api", "cases"] and parts[2].isdigit():
            return 200, self.service.get_case(int(parts[2]), role, region)
        raise ApiError(404, "not_found", "接口不存在")

    def _dispatch_post(self, path: str, body: dict[str, Any]) -> Any:
        actor, role, region = self.service.identity(self.headers)
        if path == "/api/cases":
            return 201, self.service.create_case(actor, role, region, body)
        if path == "/api/escalate-overdue":
            return 200, self.service.escalate_overdue(actor, role, region)
        parts = [part for part in path.split("/") if part]
        if len(parts) == 4 and parts[:2] == ["api", "cases"] and parts[2].isdigit():
            case_id, action = int(parts[2]), parts[3]
            if action == "followups":
                return 201, self.service.add_followup(case_id, actor, role, region, body)
            if action == "medical-review":
                return 200, self.service.medical_review(case_id, actor, role, body)
            if action == "reports":
                return 201, self.service.create_report(case_id, actor, role, region, body)
            if action == "merge":
                return 200, self.service.merge_cases(case_id, actor, role, body)
            if action == "sources":
                return 201, self.service.backfill_source(case_id, actor, role, region, body)
        if path == "/api/sources/batch":
            return 200, self.service.backfill_batch(actor, role, region, body)
        if len(parts) == 4 and parts[:2] == ["api", "reports"] and parts[2].isdigit() and parts[3] == "submit":
            return 200, self.service.submit_report(int(parts[2]), actor, role, region, body)
        raise ApiError(404, "not_found", "接口不存在")

    def _handle(self, method: str) -> None:
        parsed = urlparse(self.path)
        try:
            if method == "GET" and parsed.path == "/":
                page = (self.web_root / "index.html").read_bytes()
                self.send_response(200)
                self.send_header("Content-Type", "text/html; charset=utf-8")
                self.send_header("Content-Length", str(len(page)))
                self.end_headers()
                self.wfile.write(page)
                return
            if method == "GET":
                status, payload = self._dispatch_get(parsed.path, parse_qs(parsed.query))
            else:
                status, payload = self._dispatch_post(parsed.path, self._body())
            json_response(self, status, payload)
        except ApiError as exc:
            json_response(self, exc.status, {"error": exc.code, "message": exc.message})
        except Exception as exc:
            print(f"unhandled error: {exc!r}")
            json_response(self, 500, {"error": "internal_error", "message": str(exc)})

    def do_GET(self) -> None:
        self._handle("GET")

    def do_POST(self) -> None:
        self._handle("POST")


def create_server(db_path: str | Path, host: str = "127.0.0.1", port: int = PORT) -> ThreadingHTTPServer:
    service = PharmacovigilanceService(db_path)
    web_root = Path(__file__).resolve().parent / "static"
    handler = type("PharmacovigilanceHandler", (Handler,), {"service": service, "web_root": web_root})
    return ThreadingHTTPServer((host, port), handler)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=PORT)
    parser.add_argument("--db", default=os.environ.get("PV_DB", "pharmacovigilance.db"))
    args = parser.parse_args()
    server = create_server(args.db, args.host, args.port)
    print(f"pharmacovigilance listening on http://{args.host}:{args.port}")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()


if __name__ == "__main__":
    main()
