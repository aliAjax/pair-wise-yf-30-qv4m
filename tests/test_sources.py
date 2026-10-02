import sys
import tempfile
import threading
import unittest
from pathlib import Path
from datetime import timedelta

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app import ApiError, PharmacovigilanceService, iso, parse_time, report_deadline, utcnow


class SourceReconciliationTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.svc = PharmacovigilanceService(Path(self.tmp.name) / "test.db")
        self.t0 = utcnow() - timedelta(days=1)
        self.lead = ("lead-cn", "regional_lead", "CN")
        self.reporter = ("reporter-a", "reporter", "CN")

    def tearDown(self):
        self.tmp.cleanup()

    def make_case(self, dedupe="src-1", serious=False, fatal=False):
        return self.svc.create_case(
            "reporter-a", "reporter", "CN",
            {"patient_ref": "P-1", "region": "CN", "product": "DrugA", "event_term": "肝损伤",
             "source": "email", "dedupe_key": dedupe, "received_at": iso(self.t0),
             "serious": serious, "fatal": fatal},
        )["case"]

    def backfill(self, case, dedupe, expected, received, serious, fatal=False, source="fax"):
        return self.svc.backfill_source(
            case["id"], "reporter-b", "reporter", "CN",
            {"source": source, "dedupe_key": dedupe, "expected_revision": expected,
             "received_at": iso(received), "serious": serious, "fatal": fatal},
        )

    # 1. 迟到来源锚定旧修订号，不能覆盖随访/医学裁定的当前结论
    def test_late_source_anchored_at_old_revision_cannot_override_followup(self):
        case = self.make_case()
        t1 = self.t0 + timedelta(days=2)
        self.svc.add_followup(
            case["id"], *self.reporter,
            {"content": "住院", "source": "phone", "expected_revision": 1,
             "received_at": iso(t1), "serious": True},
        )
        # 渠道补录晚到：原始接收时间在随访之后，但锚定修订号 1（旧严重性）
        t_late = t1 + timedelta(days=3)
        result = self.backfill(
            self.svc.get_case(case["id"], "global_admin", "")["case"],
            "src-late", expected=1, received=t_late, serious=False)
        self.assertEqual(result["status"], "superseded")
        self.assertTrue(result["superseded_by"].startswith("followup:"))
        self.assertFalse(result["changed"])

        detail = self.svc.get_case(case["id"], "global_admin", "")
        case_after = detail["case"]
        self.assertEqual(case_after["revision"], 2)  # 没有为失效来源抬版本
        self.assertEqual(case_after["serious"], 1)
        self.assertEqual(len(detail["intakes"]), 2)
        self.assertEqual(detail["intakes"][0]["status"], "superseded")  # 初始来源已被随访取代
        self.assertEqual(detail["intakes"][0]["superseded_by"].startswith("followup:"), True)
        self.assertEqual(detail["intakes"][-1]["status"], "superseded")

        recon = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(recon["consistent"], recon["mismatches"])
        self.assertEqual(recon["current"]["basis_kind"], "followup")
        self.assertEqual(recon["sources"]["applied"], 0)
        self.assertEqual(recon["sources"]["superseded"], 2)

    def test_late_source_cannot_override_medical_adjudication(self):
        case = self.make_case(serious=True)
        t1 = self.t0 + timedelta(days=1)
        self.svc.medical_review(
            case["id"], "reviewer-1", "medical_reviewer",
            {"expected_revision": 1, "serious": False, "fatal": False, "causality": "unrelated",
             "rationale": "病史证明与药品无关", "received_at": iso(t1)},
        )
        # 医学裁定降为非严重后，迟到的旧渠道来源仍声称严重，锚定修订号 1
        result = self.backfill(case, "src-late", expected=1,
                               received=t1 + timedelta(days=5), serious=True)
        self.assertEqual(result["status"], "superseded")
        self.assertTrue(result["superseded_by"].startswith("medical_review:"))
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(detail["case"]["serious"], 0)
        recon = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(recon["consistent"], recon["mismatches"])
        self.assertEqual(recon["current"]["basis_kind"], "medical_review")

    # 2. 迟到来源锚定当前修订号、且时间晚于随访 → 改变严重性口径，未交报告立即重算期限
    def test_late_source_changes_severity_pending_report_recomputed(self):
        case = self.make_case()
        t1 = self.t0 + timedelta(days=2)
        self.svc.add_followup(
            case["id"], *self.reporter,
            {"content": "住院", "source": "phone", "expected_revision": 1,
             "received_at": iso(t1), "serious": True},
        )
        report = self.svc.create_report(case["id"], *self.lead, {"country": "CN"})
        self.assertEqual(report["due_at"], iso(report_deadline(t1, True, False)))

        # 修订号 2（当前）+ 更晚的原始接收时间 + 死亡转归 → 死亡口径，7 天
        t2 = t1 + timedelta(days=1)
        result = self.backfill(case, "src-fatal", expected=2, received=t2,
                               serious=True, fatal=True, source="hospital_portal")
        self.assertEqual(result["status"], "applied")
        self.assertTrue(result["changed"])
        self.assertEqual(result["revision"], 3)
        self.assertEqual(result["report_impact"]["recomputed"], [report["id"]])
        self.assertEqual(result["report_impact"]["resubmitted"], [])

        reports = self.svc.reconcile_case(case["id"], "global_admin", "")["reports"]
        self.assertEqual(len(reports["active"]), 1)
        active = reports["active"][0]
        self.assertEqual(active["status"], "pending")
        self.assertEqual(active["version"], 1)
        self.assertEqual(active["due_at"], iso(report_deadline(t2, True, True)))
        self.assertEqual(active["basis_kind"], "intake")
        recon = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(recon["consistent"], recon["mismatches"])
        # 初始非严重来源已被随访取代；随访又被死亡补录取代，当前生效的是补录来源
        self.assertEqual(recon["sources"]["applied"], 1)
        self.assertEqual(recon["sources"]["superseded"], 1)

    # 3. 已交报告：保留原稿，按新依据生成待重报版本
    def test_submitted_report_keeps_original_and_opens_resubmission(self):
        case = self.make_case(serious=True)
        submitted_at = self.t0 + timedelta(days=3)
        report = self.svc.create_report(case["id"], *self.lead, {"country": "CN"})
        original = self.svc.submit_report(report["id"], *self.lead,
                                          {"submitted_at": iso(submitted_at)})["report"]
        self.assertEqual(original["status"], "submitted")

        t1 = self.t0 + timedelta(days=10)
        result = self.backfill(case, "src-fatal", expected=1, received=t1,
                               serious=True, fatal=True, source="hospital_portal")
        self.assertEqual(result["revision"], 2)
        self.assertEqual(len(result["report_impact"]["resubmitted"]), 1)

        recon = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(recon["consistent"], recon["mismatches"])
        archived = recon["reports"]["archived"][0]
        active = recon["reports"]["active"][0]
        # 原稿原样保留
        self.assertEqual(archived["id"], report["id"])
        self.assertEqual(archived["status"], "submitted")
        self.assertEqual(archived["submitted_at"], iso(submitted_at))
        self.assertEqual(archived["version"], 1)
        self.assertEqual(archived["due_at"], iso(report_deadline(self.t0, True, False)))
        # 新依据 → 待重报，7 天死亡期限
        self.assertEqual(active["status"], "resubmission_required")
        self.assertEqual(active["version"], 2)
        self.assertEqual(active["supersedes_report_id"], report["id"])
        self.assertEqual(active["due_at"], iso(report_deadline(t1, True, True)))
        self.assertEqual(active["basis_revision"], 2)

        # 不能直接对原稿再提交
        with self.assertRaises(ApiError) as ctx:
            self.svc.submit_report(report["id"], *self.lead, {})
        self.assertEqual(ctx.exception.code, "report_archived")
        # 待重报版本提交后生效
        resubmitted = self.svc.submit_report(active["id"], *self.lead,
                                             {"submitted_at": iso(t1 + timedelta(days=2))})["report"]
        self.assertEqual(resubmitted["status"], "submitted")
        # 国家维度仍只能有一份有效报告
        with self.assertRaises(ApiError) as ctx:
            self.svc.create_report(case["id"], *self.lead, {"country": "CN"})
        self.assertEqual(ctx.exception.code, "report_exists")

    # 4. 两人同时补录：只有修订号匹配者生效，另一人冲突且不落库
    def test_concurrent_backfill_only_matching_revision_takes_effect(self):
        case = self.make_case()
        db_path = Path(self.tmp.name) / "concurrent.db"
        svc_a = PharmacovigilanceService(db_path)
        svc_b = PharmacovigilanceService(db_path)
        # 在共享库里重建该案例
        case = svc_a.create_case(
            "reporter-a", "reporter", "CN",
            {"patient_ref": "P-1", "region": "CN", "product": "DrugA", "event_term": "肝损伤",
             "source": "email", "dedupe_key": "cc-base", "received_at": iso(self.t0),
             "serious": False},
        )["case"]
        errors = []
        results = []
        barrier = threading.Barrier(2)

        def run(svc, dedupe, received):
            barrier.wait()
            try:
                results.append(svc.backfill_source(
                    case["id"], "reporter-b", "reporter", "CN",
                    {"source": "fax", "dedupe_key": dedupe, "expected_revision": 1,
                     "received_at": iso(received), "serious": True}))
            except ApiError as exc:
                errors.append(exc)

        t_a = self.t0 + timedelta(days=2)
        t_b = self.t0 + timedelta(days=3)
        ta = threading.Thread(target=run, args=(svc_a, "src-a", t_a))
        tb = threading.Thread(target=run, args=(svc_b, "src-b", t_b))
        ta.start(); tb.start(); ta.join(); tb.join()

        self.assertEqual(len(results), 1, f"应只有一个胜出，errors={errors}")
        self.assertEqual(len(errors), 1)
        self.assertEqual(errors[0].code, "revision_conflict")
        self.assertIn("2", errors[0].message)
        # 胜出者由线程调度决定（src-a 或 src-b）；关键是恰好一个新来源落库
        detail = svc_a.get_case(case["id"], "global_admin", "")
        self.assertEqual(detail["case"]["revision"], 2)
        self.assertEqual(len(detail["intakes"]), 2)  # 初始来源 + 胜出补录，冲突者未入库
        new_sources = [i for i in detail["intakes"] if i["dedupe_key"] != "cc-base"]
        self.assertEqual(len(new_sources), 1)
        self.assertIn(new_sources[0]["dedupe_key"], {"src-a", "src-b"})
        self.assertEqual(new_sources[0]["status"], "applied")
        loser_key = "src-b" if new_sources[0]["dedupe_key"] == "src-a" else "src-a"
        present = {i["dedupe_key"] for i in detail["intakes"]}
        self.assertNotIn(loser_key, present)

    # 5. 批次部分失败后重试：只补未处理来源，已确认来源不重复计数
    def test_batch_partial_failure_then_retry_is_idempotent(self):
        case = self.make_case(dedupe="batch-base")
        t1 = self.t0 + timedelta(days=2)
        body = {"batch_id": "batch-001", "items": [
            {"case_id": case["id"], "source": "fax", "dedupe_key": "b-item-1",
             "expected_revision": 1, "received_at": iso(t1), "serious": True},
            {"case_id": case["id"], "source": "portal", "dedupe_key": "b-item-2",
             "expected_revision": 1, "received_at": iso(t1 + timedelta(days=1)),
             "serious": True},
        ]}
        first = self.svc.backfill_batch("reporter-b", "reporter", "CN", body)
        self.assertEqual(first["applied"], 1)
        self.assertEqual(first["conflicted"], 1)
        self.assertTrue(first["items"][0]["ok"])
        self.assertEqual(first["items"][1]["error"], "revision_conflict")

        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["intakes"]), 2)
        current_revision = detail["case"]["revision"]
        self.assertEqual(current_revision, 2)

        # 用同批次重试：第一项已确认（去重不重复计数），第二项用新修订号补成功
        body["items"][1]["expected_revision"] = 2
        second = self.svc.backfill_batch("reporter-b", "reporter", "CN", body)
        self.assertEqual(second["applied"], 1)
        self.assertEqual(second["duplicated"], 1)
        self.assertEqual(second["conflicted"], 0)
        self.assertTrue(second["items"][0]["idempotent"])

        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["intakes"]), 3)  # 无重复行
        recon = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(recon["consistent"], recon["mismatches"])
        self.assertEqual(recon["sources"]["total"], 3)
        self.assertEqual(recon["sources"]["applied"], 3)

    def test_batch_validation_error_writes_nothing(self):
        case = self.make_case(dedupe="bad-base")
        body = {"items": [
            {"case_id": case["id"], "source": "fax", "dedupe_key": "bad-1",
             "expected_revision": 1, "received_at": iso(self.t0), "serious": True},
            {"case_id": case["id"], "source": "", "dedupe_key": "bad-2",
             "expected_revision": 1, "received_at": iso(self.t0), "serious": True},
        ]}
        with self.assertRaises(ApiError) as ctx:
            self.svc.backfill_batch("reporter-b", "reporter", "CN", body)
        self.assertEqual(ctx.exception.status, 400)
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["intakes"]), 1)  # 整体校验失败，零写入

    # 6. 单条补录本身幂等：同一 dedupe_key 重试返回已处理结果
    def test_single_backfill_is_idempotent(self):
        case = self.make_case()
        t1 = self.t0 + timedelta(days=2)
        first = self.backfill(case, "idem-1", expected=1, received=t1, serious=True)
        self.assertFalse(first["idempotent"])
        second = self.backfill(case, "idem-1", expected=2, received=t1, serious=True)
        self.assertTrue(second["idempotent"])
        self.assertEqual(second["intake_id"], first["intake_id"])
        detail = self.svc.get_case(case["id"], "global_admin", "")
        self.assertEqual(len(detail["intakes"]), 2)

    # 7. 权限：医学审核员不能补录渠道来源；跨区域被拒
    def test_backfill_permissions(self):
        case = self.make_case()
        with self.assertRaises(ApiError) as ctx:
            self.svc.backfill_source(
                case["id"], "reviewer-1", "medical_reviewer", "",
                {"source": "fax", "dedupe_key": "perm-1", "expected_revision": 1,
                 "received_at": iso(self.t0), "serious": True})
        self.assertEqual(ctx.exception.status, 403)
        with self.assertRaises(ApiError) as ctx:
            self.svc.backfill_source(
                case["id"], "reporter-us", "reporter", "US",
                {"source": "fax", "dedupe_key": "perm-2", "expected_revision": 1,
                 "received_at": iso(self.t0), "serious": True})
        self.assertEqual(ctx.exception.status, 403)

    # 8. 无报告时补录对账仍一致，且能识别陈旧修订号
    def test_reconcile_clean_case_and_stale_revision(self):
        case = self.make_case()
        recon = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(recon["consistent"])
        with self.assertRaises(ApiError) as ctx:
            self.backfill(case, "stale-1", expected=99, received=self.t0, serious=True)
        self.assertEqual(ctx.exception.code, "revision_conflict")

    # 9. 普通随访（不改严重性）也重置报告时钟，且案例/来源/报告保持对账一致
    def test_plain_followup_resets_clock_and_stays_consistent(self):
        case = self.make_case(serious=True)
        report = self.svc.create_report(case["id"], *self.lead, {"country": "CN"})
        t1 = self.t0 + timedelta(days=3)
        self.svc.add_followup(
            case["id"], *self.reporter,
            {"content": "补充病程记录（严重性不变）", "source": "phone",
             "expected_revision": 1, "received_at": iso(t1)},
        )
        detail = self.svc.get_case(case["id"], "global_admin", "")
        # 案例期限按随访时间、沿用严重口径 15 天重算
        self.assertEqual(detail["case"]["report_due_at"], iso(report_deadline(t1, True, False)))
        self.assertEqual(detail["case"]["serious"], 1)
        active = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(active["consistent"], active["mismatches"])
        self.assertEqual(active["current"]["basis_kind"], "followup")
        self.assertEqual(active["reports"]["active"][0]["due_at"], iso(report_deadline(t1, True, False)))
        self.assertEqual(active["reports"]["active"][0]["id"], report["id"])

        # 锚定旧修订号的迟到来源被该随访（继承严重口径）挡住
        result = self.backfill(case, "late-plain", expected=1,
                               received=t1 + timedelta(days=2), serious=False)
        self.assertEqual(result["status"], "superseded")
        self.assertTrue(result["superseded_by"].startswith("followup:"))
        recon = self.svc.reconcile_case(case["id"], "global_admin", "")
        self.assertTrue(recon["consistent"], recon["mismatches"])

    def test_http_routes_for_backfill_and_reconcile(self):
        import json
        import urllib.error
        import urllib.request
        from app import create_server
        db_path = Path(self.tmp.name) / "http.db"
        server = create_server(db_path, host="127.0.0.1", port=0)
        port = server.server_address[1]
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            def call(method, path, body=None, user="reporter-a", role="reporter", region="CN"):
                data = json.dumps(body).encode() if body is not None else None
                req = urllib.request.Request(f"http://127.0.0.1:{port}{path}", data=data, method=method)
                req.add_header("Content-Type", "application/json")
                req.add_header("X-User-Id", user); req.add_header("X-Role", role); req.add_header("X-Region", region)
                try:
                    with urllib.request.urlopen(req) as r:
                        return r.status, json.loads(r.read())
                except urllib.error.HTTPError as e:
                    return e.code, json.loads(e.read())
            s, c = call("POST", "/api/cases",
                        {"patient_ref": "P", "region": "CN", "product": "D", "event_term": "e",
                         "source": "email", "dedupe_key": "h1", "received_at": iso(self.t0), "serious": False})
            self.assertEqual(s, 201)
            cid = c["case"]["id"]
            s, c = call("POST", f"/api/cases/{cid}/sources",
                        {"source": "fax", "dedupe_key": "h2", "expected_revision": 1,
                         "received_at": iso(self.t0), "serious": True})
            self.assertEqual(s, 201)
            s, c = call("POST", "/api/sources/batch",
                        {"batch_id": "hb", "items": [{"case_id": cid, "source": "fax", "dedupe_key": "h2",
                         "expected_revision": 2, "received_at": iso(self.t0), "serious": True}]})
            self.assertEqual(s, 200)
            self.assertEqual(c["duplicated"], 1)
            s, c = call("GET", f"/api/cases/{cid}/reconcile", None,
                        user="admin", role="global_admin", region="")
            self.assertEqual(s, 200)
            self.assertIn("consistent", c)
            self.assertIn("sources", c)
        finally:
            server.shutdown()


if __name__ == "__main__":
    unittest.main()
