import json
import tempfile
import unittest
from datetime import timedelta
from pathlib import Path

from app import ApiError, PharmacovigilanceService, iso, parse_time, utcnow


class SourceReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = PharmacovigilanceService(Path(self.tmp.name) / "test.db")

    def tearDown(self):
        self.tmp.cleanup()

    def create(self, serious=False, fatal=False):
        return self.svc.create_case(
            "reporter-a", "reporter", "CN",
            {"patient_ref": "P-1", "region": "CN", "product": "DrugA", "event_term": "肝损伤",
             "source": "email", "dedupe_key": "intake-1", "received_at": iso(utcnow()),
             "serious": serious, "fatal": fatal},
        )["case"]

    def test_backfill_records_source_and_reconciles(self):
        case = self.create()
        base = parse_time(case["received_at"])
        res = self.svc.backfill_sources(
            case["id"], "reporter-a", "reporter", "CN",
            {"batch_id": "b1", "expected_revision": case["revision"],
             "sources": [{"source": "fax", "received_at": iso(base + timedelta(days=1)),
                          "serious": False, "fatal": False}]},
        )
        self.assertFalse(res["idempotent"])
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["sources"]), 1)
        src = detail["sources"][0]
        self.assertEqual(src["source"], "fax")
        self.assertEqual(src["effective"], 1)
        self.assertIn("severity_at_time", src)
        rec = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(rec["reconciled"])

    def test_late_source_changes_severity_recomputes_pending(self):
        case = self.create(serious=False)
        base = parse_time(case["received_at"])
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        self.assertEqual(report["due_at"], iso(base + timedelta(days=90)))
        res = self.svc.backfill_sources(
            case["id"], "reporter-a", "reporter", "CN",
            {"batch_id": "b1", "expected_revision": case["revision"],
             "sources": [{"source": "hotline", "received_at": iso(base + timedelta(days=1)),
                          "serious": True, "fatal": False}]},
        )
        self.assertTrue(res["severity_changed"])
        self.assertEqual(res["case"]["serious"], 1)
        rec = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(rec["reconciled"])
        updated = [r for r in rec["reports"] if r["id"] == report["id"]][0]
        self.assertEqual(updated["due_at"], iso(base + timedelta(days=15)))
        self.assertEqual(updated["status"], "pending")

    def test_submitted_report_becomes_pending_resubmission_keeps_original(self):
        case = self.create(serious=False)
        base = parse_time(case["received_at"])
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        submitted = self.svc.submit_report(report["id"], "lead-cn", "regional_lead", "CN", {})
        self.assertEqual(submitted["report"]["status"], "submitted")
        original_due = submitted["report"]["due_at"]
        res = self.svc.backfill_sources(
            case["id"], "reporter-a", "reporter", "CN",
            {"batch_id": "b1", "expected_revision": case["revision"],
             "sources": [{"source": "hotline", "received_at": iso(base + timedelta(days=1)),
                          "serious": True, "fatal": False}]},
        )
        self.assertTrue(res["severity_changed"])
        rec = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(rec["reconciled"])
        updated = [r for r in rec["reports"] if r["id"] == report["id"]][0]
        self.assertEqual(updated["status"], "pending_resubmission")
        self.assertEqual(updated["original_due_at"], original_due)
        self.assertEqual(updated["due_at"], iso(base + timedelta(days=15)))
        self.assertIsNotNone(updated["original_submitted_at"])
        self.assertEqual(updated["resubmission_reason"], "late_source_severity_change")

    def test_old_source_superseded_cannot_override(self):
        case = self.create(serious=False)
        base = parse_time(case["received_at"])
        report = self.svc.create_report(case["id"], "lead-cn", "regional_lead", "CN", {"country": "CN"})
        reviewed = self.svc.medical_review(
            case["id"], "reviewer-1", "medical_reviewer",
            {"expected_revision": case["revision"], "serious": True, "fatal": True,
             "causality": "related", "rationale": "住院和死亡证明已核验", "received_at": iso(utcnow())},
        )
        self.assertEqual(reviewed["case"]["serious"], 1)
        res = self.svc.backfill_sources(
            case["id"], "reporter-a", "reporter", "CN",
            {"batch_id": "b1", "expected_revision": reviewed["case"]["revision"],
             "sources": [{"source": "email-archive", "received_at": iso(base - timedelta(days=1)),
                          "serious": False, "fatal": False}]},
        )
        self.assertFalse(res["severity_changed"])
        self.assertEqual(res["case"]["serious"], 1)
        detail = self.svc.get_case(case["id"], "global_admin", "")
        src = detail["sources"][0]
        self.assertEqual(src["effective"], 0)
        self.assertEqual(src["superseded_reason"], "superseded_by_later_conclusion")
        sev_at = json.loads(src["severity_at_time"])
        self.assertFalse(sev_at["serious"])
        rec = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(rec["reconciled"])
        updated = [r for r in rec["reports"] if r["id"] == report["id"]][0]
        self.assertEqual(updated["due_at"], iso(base + timedelta(days=7)))

    def test_concurrent_backfill_conflict(self):
        case = self.create()
        body_a = {"batch_id": "b-a", "expected_revision": case["revision"],
                  "sources": [{"source": "fax", "received_at": iso(utcnow() + timedelta(days=1))}]}
        body_b = {"batch_id": "b-b", "expected_revision": case["revision"],
                  "sources": [{"source": "phone", "received_at": iso(utcnow() + timedelta(days=2))}]}
        first = self.svc.backfill_sources(case["id"], "reporter-a", "reporter", "CN", body_a)
        self.assertFalse(first["idempotent"])
        with self.assertRaises(ApiError) as ctx:
            self.svc.backfill_sources(case["id"], "reporter-b", "reporter", "CN", body_b)
        self.assertEqual(ctx.exception.code, "revision_conflict")

    def test_batch_retry_idempotent(self):
        case = self.create()
        body = {"batch_id": "b1", "expected_revision": case["revision"],
                "sources": [
                    {"source": "fax", "received_at": iso(utcnow() + timedelta(days=1)), "item_key": "a"},
                    {"source": "phone", "received_at": iso(utcnow() + timedelta(days=2)), "item_key": "b"},
                ]}
        first = self.svc.backfill_sources(case["id"], "reporter-a", "reporter", "CN", body)
        self.assertFalse(first["idempotent"])
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["sources"]), 2)
        retry = self.svc.backfill_sources(case["id"], "reporter-a", "reporter", "CN", body)
        self.assertTrue(retry["idempotent"])
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["sources"]), 2)

    def test_batch_skips_confirmed_adds_new(self):
        case = self.create()
        body1 = {"batch_id": "b1", "expected_revision": case["revision"],
                 "sources": [{"source": "fax", "received_at": iso(utcnow() + timedelta(days=1)), "item_key": "a"}]}
        self.svc.backfill_sources(case["id"], "reporter-a", "reporter", "CN", body1)
        case = self.svc.get_case(case["id"], "global_admin", "")["case"]
        body2 = {"batch_id": "b1", "expected_revision": case["revision"],
                 "sources": [
                     {"source": "fax", "received_at": iso(utcnow() + timedelta(days=1)), "item_key": "a"},
                     {"source": "phone", "received_at": iso(utcnow() + timedelta(days=2)), "item_key": "b"},
                 ]}
        res = self.svc.backfill_sources(case["id"], "reporter-a", "reporter", "CN", body2)
        self.assertFalse(res["idempotent"])
        statuses = {s["item_key"]: s["status"] for s in res["sources"]}
        self.assertEqual(statuses["a"], "already_confirmed")
        self.assertEqual(statuses["b"], "confirmed")
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["sources"]), 2)


if __name__ == "__main__":
    unittest.main()
