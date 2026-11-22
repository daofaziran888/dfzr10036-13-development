"""快照维护数据视图的领域回归测试（标准库 unittest，无需额外依赖）。

运行：cd backend && .venv/bin/python -m unittest discover -s tests -v
"""
from __future__ import annotations

import unittest

from app.seed import SEED_ROWS
from app.services.snapshot_maintenance import MaintenanceError, maintenance_service
from app.store import store

TABLE = "ads.order_events_iceberg"
F1 = "s3://warehouse/ads/order_events/data/part-00001.parquet"
F2 = "s3://warehouse/ads/order_events/data/part-00002.parquet"
F3 = "s3://warehouse/ads/order_events/data/part-00003.parquet"
LATER = "s3://warehouse/ads/order_events/data/part-00004.parquet"


class SnapshotMaintenanceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        maintenance_service.reset()
        store._tables["iceberg_commit"] = [
            dict(row) for row in SEED_ROWS["iceberg_commit"]]
        store._tables["compaction"] = [
            dict(row) for row in SEED_ROWS["compaction"]]

    def test_candidates_carry_base_snapshot(self) -> None:
        view = maintenance_service.open_view(TABLE)
        self.assertEqual(view["snapshotCursor"], "IC-0004")
        base = {obj["dataFile"]: obj["baseSnapshot"] for obj in view["candidates"]}
        self.assertEqual(base[F1], "IC-0001")
        self.assertEqual(base[F2], "IC-0003")
        self.assertEqual(base[F3], "IC-0004")
        visible = {snap["snapshotNo"] for snap in view["visibleSnapshots"]}
        for obj in view["candidates"]:
            self.assertTrue(set(obj["sourceSnapshots"]) <= visible)

        candidates = maintenance_service.list_candidates(view["viewId"])
        self.assertEqual(candidates["total"], 3)
        self.assertTrue(all(item["baseSnapshot"] for item in candidates["items"]))

    def test_shared_files_listed_once(self) -> None:
        view = maintenance_service.open_view(TABLE)
        paths = [obj["dataFile"] for obj in view["objects"]]
        self.assertEqual(len(paths), len(set(paths)))
        repeated = [obj for obj in view["objects"] if len(obj["sourceSnapshots"]) > 1]
        self.assertEqual(len(repeated), 2)

    def test_new_snapshot_does_not_enter_open_view(self) -> None:
        view = maintenance_service.open_view(TABLE)
        appended = maintenance_service.append_committed_snapshot(TABLE, [LATER])
        after = maintenance_service.get_view(view["viewId"])
        self.assertNotIn(
            appended["snapshotNo"], [obj["baseSnapshot"] for obj in after["objects"]])
        self.assertEqual(after["newFilesSinceOpen"]["paths"], [LATER])

    def test_confirmed_scope_survives_later_changes(self) -> None:
        view = maintenance_service.open_view(TABLE)
        first_two = [item["objectId"] for item in view["candidates"][:2]]
        plan = maintenance_service.confirm_scope(view["viewId"], first_two, batch_size=1)
        maintenance_service.append_committed_snapshot(TABLE, [LATER])

        frozen = maintenance_service.get_plan(plan["planId"])
        self.assertEqual(frozen["confirmedScope"]["count"], 2)
        self.assertIn(LATER, frozen["laterAppended"]["paths"])
        self.assertNotIn(LATER, frozen["confirmedScope"]["paths"])

    def test_concurrent_jobs_claim_each_file_once(self) -> None:
        view_a = maintenance_service.open_view(TABLE)
        view_b = maintenance_service.open_view(TABLE)
        plan_a = maintenance_service.confirm_scope(view_a["viewId"], batch_size=2)
        maintenance_service.confirm_scope(view_b["viewId"], batch_size=2)

        result_a = maintenance_service.submit_maintenance(
            view_a["viewId"], job_id="job-A")
        self.assertEqual(result_a["batch"]["effectiveCount"], 2)
        self.assertEqual(result_a["effectiveScope"]["paths"], [F1, F2])

        result_b = maintenance_service.submit_maintenance(
            view_b["viewId"], job_id="job-B")
        self.assertEqual(result_b["batch"]["effectiveCount"], 0)
        self.assertEqual(result_b["batch"]["conflictCount"], 2)
        self.assertEqual(
            result_a["compactionTask"]["真实生效路径"], f"{F1},{F2}")

        plan_a_view = maintenance_service.get_plan(plan_a["planId"])
        self.assertEqual(plan_a_view["replacedScope"]["count"], 2)
        self.assertEqual(plan_a_view["confirmedScope"]["count"], 3)

    def test_continuation_ignores_later_snapshot(self) -> None:
        view = maintenance_service.open_view(TABLE)
        plan = maintenance_service.confirm_scope(view["viewId"], batch_size=2)
        maintenance_service.submit_maintenance(view["viewId"], job_id="job-1")
        maintenance_service.append_committed_snapshot(TABLE, [LATER])

        result = maintenance_service.submit_maintenance(view["viewId"], job_id="job-1")
        self.assertEqual(result["effectiveScope"]["paths"], [F3])
        self.assertIn(LATER, result["laterAppended"]["paths"])
        plan_view = maintenance_service.get_plan(plan["planId"])
        self.assertNotIn(LATER, plan_view["replacedScope"]["paths"])
        self.assertEqual(plan_view["remainingScope"]["count"], 0)

    def test_compaction_task_reports_attempted_and_effective(self) -> None:
        view = maintenance_service.open_view(TABLE)
        maintenance_service.confirm_scope(view["viewId"], batch_size=2)
        result = maintenance_service.submit_maintenance(view["viewId"], job_id="job-1")
        task = result["compactionTask"]
        self.assertEqual(task["试图替换路径"], f"{F1},{F2}")
        self.assertEqual(task["真实生效路径"], f"{F1},{F2}")
        self.assertEqual(task["维护视图"], f"快照维护数据视图#{view['viewId']}")
        self.assertEqual(task["确认对象数"], 2)
        self.assertTrue(task["基准快照"])

    def test_failed_batch_still_reports_ineffective_paths(self) -> None:
        view_a = maintenance_service.open_view(TABLE)
        view_b = maintenance_service.open_view(TABLE)
        maintenance_service.confirm_scope(view_a["viewId"])
        maintenance_service.confirm_scope(view_b["viewId"])
        maintenance_service.submit_maintenance(view_a["viewId"], job_id="job-A")

        result_b = maintenance_service.submit_maintenance(
            view_b["viewId"], job_id="job-B")
        task = result_b["compactionTask"]
        self.assertEqual(task["真实生效路径"], "")
        self.assertEqual(task["未生效路径"], f"{F1},{F2},{F3}")

    def test_plan_comparison_and_alias_errors(self) -> None:
        view_a = maintenance_service.open_view(TABLE)
        plan_a = maintenance_service.confirm_scope(view_a["viewId"], batch_size=2)
        maintenance_service.submit_maintenance(view_a["viewId"], job_id="job-A")

        view_b = maintenance_service.open_view(TABLE)
        plan_b = maintenance_service.confirm_scope(view_b["viewId"], batch_size=2)
        compare = maintenance_service.compare_plans(plan_a["planId"], plan_b["planId"])
        self.assertTrue(compare["sameTable"])
        self.assertEqual(compare["confirmed"]["rightOnly"], [])
        self.assertEqual(
            compare["stillGovernable"]["left"],
            [F3],
        )
        self.assertEqual(set(compare["replaced"]["leftOnly"]), {F1, F2})

        with self.assertRaises(MaintenanceError):
            maintenance_service.get_view(9999)
        with self.assertRaises(MaintenanceError):
            maintenance_service.submit_maintenance(view_a["viewId"], plan_id=plan_b["planId"])
        with self.assertRaises(MaintenanceError):
            maintenance_service.open_view("no.such.table")

    def test_cursor_limited_view(self) -> None:
        view = maintenance_service.open_view(TABLE, snapshot_id=1)
        self.assertEqual([obj["dataFile"] for obj in view["objects"]], [F1])
        self.assertEqual(view["newFilesSinceOpen"]["count"], 2)
        with self.assertRaises(MaintenanceError):
            maintenance_service.open_view("ods.payment_log_iceberg", snapshot_id=1)


if __name__ == "__main__":
    unittest.main()
