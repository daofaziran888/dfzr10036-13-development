"""快照维护数据视图：按已提交快照圈定小文件合并的确认范围。

视图打开时把当时可见的已提交快照固化成一批维护对象，每个对象都带基准快照
（文件第一次出现的快照）和来源快照。之后同表新增快照只会出现在新视图里，
不会混进旧视图或旧计划；计划确认的内容也会冻结，后续仍可查询。

几条硬约束：
1. 待处理文件必须能追溯到打开视图时已经存在的快照。
2. 同一数据文件的写入占用全局唯一，两个合并作业同时提交时只有一个生效。
3. 计划确认后数据文件条件变化，已确认范围保持可查；续做只看确认范围，
   视图打开后追加的快照文件永远不会被算作“已替换”。
"""
from __future__ import annotations

import math
import re
import threading
from datetime import datetime
from typing import Any, Iterable

from app.services.compaction import CompactionService
from app.store import store

MAX_BATCH_SIZE = 200


class MaintenanceError(Exception):
    """维护视图的领域错误，路由层转成可读的 HTTP 状态。"""

    def __init__(self, message: str, status_code: int = 400) -> None:
        super().__init__(message)
        self.message = message
        self.status_code = status_code


def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _split_files(raw: Any) -> list[str]:
    """数据文件既可能是逗号串，也可能是列表，统一拆成去重后的路径列表。"""
    if raw is None:
        return []
    if isinstance(raw, (list, tuple, set)):
        parts = [str(item) for item in raw]
    else:
        parts = re.split(r"[,;\n]+", str(raw))
    seen: set[str] = set()
    files: list[str] = []
    for part in parts:
        path = part.strip()
        if path and path not in seen:
            seen.add(path)
            files.append(path)
    return files


def _file_size_mb(path: str) -> int:
    """示例数据没有真实文件大小，用文件名稳定生成一个可展示的小文件大小。"""
    match = re.search(r"part-(\d+)", path)
    number = int(match.group(1)) if match else sum(ord(char) for char in path)
    return 12 + (number * 37) % 140


def _batch_size(value: int | None, fallback: int) -> int:
    try:
        size = int(value if value is not None else fallback)
    except (TypeError, ValueError):
        size = fallback
    return max(1, min(MAX_BATCH_SIZE, size))


class SnapshotMaintenanceService:
    """快照维护视图的领域服务（内存实现，与示例数据仓库同生命周期）。"""

    def __init__(self) -> None:
        self._lock = threading.RLock()
        self._views: dict[int, dict[str, Any]] = {}
        self._plans: dict[int, dict[str, Any]] = {}
        self._claims: dict[str, dict[str, Any]] = {}
        self._replaced: dict[str, dict[str, Any]] = {}
        self._next_view_id = 1
        self._next_plan_id = 1
        self._next_object_id = 1
        self._compaction = CompactionService()

    def reset(self) -> None:
        """清空维护状态，供测试和本地重放使用（不动示例快照数据）。"""
        with self._lock:
            self._views.clear()
            self._plans.clear()
            self._claims.clear()
            self._replaced.clear()
            self._next_view_id = 1
            self._next_plan_id = 1
            self._next_object_id = 1

    # ---- 表与快照 -------------------------------------------------------

    def list_tables(self) -> dict[str, Any]:
        """列出有已提交快照的表，供前端选择维护目标。"""
        tables: dict[str, list[dict[str, Any]]] = {}
        for row in self._committed_snapshots():
            table = str(row.get("目标表", "")).strip()
            if table:
                tables.setdefault(table, []).append(row)

        items: list[dict[str, Any]] = []
        for table in sorted(tables):
            snapshots = tables[table]
            files: set[str] = set()
            for snapshot in snapshots:
                files.update(_split_files(snapshot.get("数据文件")))
            candidates = {
                path for path in files
                if path not in self._replaced and not self._active_claim(path)
            }
            items.append({
                "targetTable": table,
                "snapshotCount": len(snapshots),
                "fileCount": len(files),
                "candidateCount": len(candidates),
                "replacedCount": len(files & set(self._replaced)),
                "occupiedCount": sum(1 for path in files if self._active_claim(path)),
                "latestSnapshot": self._snapshot_brief(snapshots[-1]),
                "snapshots": [self._snapshot_brief(row) for row in snapshots],
            })
        return {"tables": items, "count": len(items)}

    def append_committed_snapshot(
        self,
        table: str,
        data_files: Iterable[str] | str,
        *,
        row_count: int = 0,
    ) -> dict[str, Any]:
        """向已提交快照序列追加一份同表快照（模拟视图打开后的实时写入）。"""
        table = str(table or "").strip()
        files = _split_files(data_files)
        if not table:
            raise MaintenanceError("必须指定目标表", 400)
        if not files:
            raise MaintenanceError("追加快照必须至少包含一个数据文件", 400)

        with self._lock:
            commit_rows = store.rows("iceberg_commit")
            new_id = max((int(row.get("id", 0)) for row in commit_rows), default=0) + 1
            row = {
                "id": new_id,
                "status": "已提交",
                "pending": False,
                "abnormal": False,
                "快照编号": f"IC-{new_id:04d}",
                "目标表": table,
                "数据文件": ",".join(files),
                "清单文件": f"metadata/manifest-{new_id}.avro",
                "快照ID": str(9182736450123456789 + new_id),
                "提交状态": "COMMITTED",
                "影响行数": int(row_count or 0),
                "提交时间": _now(),
            }
            commit_rows.append(row)
            return self._snapshot_brief(row)

    # ---- 维护视图 -------------------------------------------------------

    def open_view(self, table: str, snapshot_id: int | None = None) -> dict[str, Any]:
        """打开快照维护视图，只纳入打开时已经提交且可见的快照。

        snapshot_id 给定时以该快照为游标，只纳入它及之前提交的快照；
        不传则纳入目标表当前全部已提交快照。
        """
        table = str(table or "").strip()
        if not table:
            raise MaintenanceError("必须指定目标表", 400)

        committed = self._committed_snapshots(table)
        if not committed:
            raise MaintenanceError(f"目标表 {table} 没有已提交快照，无法打开维护视图", 404)

        if snapshot_id is not None:
            cursor = next(
                (row for row in committed if int(row.get("id", 0)) == int(snapshot_id)), None)
            if cursor is None:
                raise MaintenanceError(
                    f"快照 {snapshot_id} 不是目标表 {table} 的已提交快照", 404)
            visible = [
                row for row in committed if int(row.get("id", 0)) <= int(snapshot_id)]
        else:
            cursor = committed[-1]
            visible = committed

        if not visible:
            raise MaintenanceError("所选快照位点之前没有已提交快照", 409)

        with self._lock:
            view_id = self._next_view_id
            self._next_view_id += 1
            view = {
                "viewId": view_id,
                "targetTable": table,
                "status": "已打开",
                "openedAt": _now(),
                "snapshotCursorId": int(cursor.get("id", 0)),
                "snapshotCursor": str(cursor.get("快照编号", "")),
                "visibleSnapshots": [self._snapshot_brief(row) for row in visible],
                "visibleSnapshotIds": [int(row.get("id", 0)) for row in visible],
                "objects": self._build_objects(view_id, table, visible),
                "planIds": [],
            }
            self._views[view_id] = view
            return self._serialize_view(view)

    def get_view(self, view_id: int, *, candidate_only: bool = False) -> dict[str, Any]:
        with self._lock:
            view = self._view(view_id)
            payload = self._serialize_view(view)
            if candidate_only:
                payload["objects"] = payload["candidates"]
            return payload

    def list_candidates(self, view_id: int) -> dict[str, Any]:
        """查询可交给小文件合并的候选，逐条列出文件所属的基准快照。"""
        with self._lock:
            view = self._view(view_id)
            candidates = [
                decorated
                for decorated in (
                    self._decorate_object(obj) for obj in view["objects"])
                if decorated["mergeable"]
            ]
            return {
                "viewId": view["viewId"],
                "targetTable": view["targetTable"],
                "snapshotCursorId": view["snapshotCursorId"],
                "snapshotCursor": view["snapshotCursor"],
                "items": candidates,
                "total": len(candidates),
            }

    # ---- 确认与提交 -----------------------------------------------------

    def confirm_scope(
        self,
        view_id: int,
        object_ids: Iterable[int] | None = None,
        batch_size: int = 20,
    ) -> dict[str, Any]:
        """确认维护范围。确认后范围冻结，数据文件之后变化也不影响本计划的查询。"""
        with self._lock:
            view = self._view(view_id)
            decorated = {
                obj["object_id"]: self._decorate_object(obj) for obj in view["objects"]}
            candidates = [obj for obj in decorated.values() if obj["mergeable"]]

            skipped: list[dict[str, Any]] = []
            if object_ids is not None:
                requested = {int(item) for item in object_ids}
                selected = [obj for obj in candidates if obj["objectId"] in requested]
                for object_id in sorted(requested - {obj["objectId"] for obj in selected}):
                    item = decorated.get(object_id)
                    if item is None:
                        reason = "对象不属于当前视图"
                    elif item["replacement"]["status"] == "已替换":
                        reason = "文件已经被替换"
                    elif item["occupancy"]["status"] == "已占用":
                        reason = "文件已有有效写入占用"
                    else:
                        reason = "文件当前不可合并"
                    skipped.append({"objectId": object_id, "reason": reason})
            else:
                selected = candidates

            if not selected:
                if skipped:
                    detail = "；".join(
                        f"对象#{item['objectId']}：{item['reason']}" for item in skipped)
                    raise MaintenanceError(f"所选对象都不在可确认范围内（{detail}）", 409)
                raise MaintenanceError("当前视图没有可确认的候选文件", 409)

            plan_id = self._next_plan_id
            self._next_plan_id += 1
            frozen_objects = [
                {
                    "objectId": obj["objectId"],
                    "dataFile": obj["dataFile"],
                    "baseSnapshot": obj["baseSnapshot"],
                    "baseSnapshotId": obj["baseSnapshotId"],
                    "baseCommitTime": obj["baseCommitTime"],
                    "sourceSnapshots": list(obj["sourceSnapshots"]),
                    "fileSizeMB": obj["fileSizeMB"],
                }
                for obj in sorted(selected, key=lambda item: item["objectId"])
            ]
            plan = {
                "planId": plan_id,
                "viewId": view["viewId"],
                "targetTable": view["targetTable"],
                "status": "已确认",
                "confirmedAt": _now(),
                "batchSize": _batch_size(batch_size, 20),
                "snapshotCursorId": view["snapshotCursorId"],
                "snapshotCursor": view["snapshotCursor"],
                "confirmedSnapshotIds": list(view["visibleSnapshotIds"]),
                "confirmedObjects": frozen_objects,
                "confirmedPaths": [obj["dataFile"] for obj in frozen_objects],
                "handledPaths": [],
                "replacedPaths": [],
                "processedBatches": [],
                "compactionTaskIds": [],
            }
            self._plans[plan_id] = plan
            view["planIds"].append(plan_id)
            view["status"] = "已确认"
            payload = self._serialize_plan(plan)
            payload["skipped"] = skipped
            return payload

    def submit_maintenance(
        self,
        view_id: int,
        *,
        plan_id: int | None = None,
        job_id: str | None = None,
        batch_size: int | None = None,
    ) -> dict[str, Any]:
        """提交一批已确认对象，生成合并任务并返回试图替换/真实生效范围。

        两个作业同时提交时，同一路径只会产生一个有效写入占用；后来者会拿到
        冲突列表。视图打开后追加的快照不属于已确认范围，也不会被标成已替换。
        """
        with self._lock:
            view = self._view(view_id)

            if plan_id is None:
                if not view["planIds"]:
                    raise MaintenanceError("视图还没有已确认的维护计划", 409)
                plan_id = view["planIds"][-1]
            plan = self._plans.get(int(plan_id))
            if plan is None or plan["viewId"] != view["viewId"]:
                raise MaintenanceError(f"维护计划 {plan_id} 不属于视图 {view_id}", 404)

            owner = str(
                job_id or f"job-{plan['planId']}-{len(plan['processedBatches']) + 1}")
            if batch_size is not None:
                plan["batchSize"] = _batch_size(batch_size, plan["batchSize"])
            size = plan["batchSize"]
            batch_total = max(1, math.ceil(len(plan["confirmedPaths"]) / size))
            handled: set[str] = set(plan["handledPaths"])
            pending = [
                obj for obj in plan["confirmedObjects"]
                if obj["dataFile"] not in handled
            ]

            if not pending:
                payload = self._submit_payload(
                    view, plan, None, [], [], [], [], [], owner, batch_total)
                payload["message"] = "已确认范围已经全部处理，没有新的批次"
                return payload

            batch_objects = pending[:size]
            attempted = [obj["dataFile"] for obj in batch_objects]
            live_files = self._live_files(view["targetTable"])
            effective: list[dict[str, Any]] = []
            conflicts: list[dict[str, Any]] = []
            already_replaced: list[dict[str, Any]] = []
            invalid: list[dict[str, Any]] = []

            for obj in batch_objects:
                path = obj["dataFile"]
                claim = self._active_claim(path)
                if claim and claim.get("jobId") != owner:
                    replaced = self._replaced.get(path)
                    conflicts.append({
                        "path": path,
                        "owner": claim.get("jobId"),
                        "planId": claim.get("planId"),
                        "replacedByPlan": int(replaced.get("planId", 0)) if replaced else None,
                        "reason": "文件已有有效写入占用",
                    })
                    continue
                replaced = self._replaced.get(path)
                if replaced is not None:
                    already_replaced.append({
                        "path": path,
                        "planId": replaced.get("planId"),
                        "owner": replaced.get("jobId"),
                        "reason": "文件已被其他维护计划替换",
                    })
                    continue
                if path not in live_files:
                    invalid.append({
                        "path": path,
                        "reason": "文件已不在当前已提交快照的数据文件中",
                    })
                    continue

                self._claims[path] = {
                    "path": path,
                    "status": "生效中",
                    "jobId": owner,
                    "planId": plan["planId"],
                    "viewId": view["viewId"],
                    "claimedAt": _now(),
                }
                self._replaced[path] = {
                    "path": path,
                    "planId": plan["planId"],
                    "viewId": view["viewId"],
                    "jobId": owner,
                    "replacedAt": _now(),
                }
                plan["replacedPaths"].append(path)
                effective.append(obj)

            plan["handledPaths"].extend(attempted)
            batch_index = len(plan["processedBatches"]) + 1

            compaction_task = None
            if attempted:
                # 只要本批有“试图替换”的确认对象，就建立合并任务：
                # 并发失败时任务里会如实反馈未生效路径，供人工核对。
                compaction_task = self._compaction.create_from_confirmed_scope(
                    target_table=view["targetTable"],
                    objects=effective,
                    view_id=view["viewId"],
                    plan_id=plan["planId"],
                    job_id=owner,
                    batch_index=batch_index,
                    batch_total=batch_total,
                    confirmed_snapshot_ids=plan["confirmedSnapshotIds"],
                    attempted_paths=attempted,
                )
                plan["compactionTaskIds"].append(int(compaction_task["id"]))

            plan["processedBatches"].append({
                "index": batch_index,
                "total": batch_total,
                "jobId": owner,
                "submittedAt": _now(),
                "attemptedPaths": attempted,
                "effectivePaths": [obj["dataFile"] for obj in effective],
                "conflictPaths": [item["path"] for item in conflicts],
                "alreadyReplacedPaths": [item["path"] for item in already_replaced],
                "invalidPaths": [item["path"] for item in invalid],
                "compactionTaskId":
                    int(compaction_task["id"]) if compaction_task else None,
            })

            state = self._plan_state(plan)
            if state["unhandled"]:
                plan["status"] = "执行中"
            elif len(plan["replacedPaths"]) == len(plan["confirmedPaths"]):
                plan["status"] = "已完成"
            else:
                plan["status"] = "部分完成"
            view["status"] = plan["status"]

            return self._submit_payload(
                view, plan, batch_index, attempted, effective,
                conflicts, already_replaced, invalid, owner, batch_total,
                compaction_task=compaction_task,
            )

    # ---- 计划查询 -------------------------------------------------------

    def list_plans(self, table: str | None = None) -> dict[str, Any]:
        with self._lock:
            plans = [
                self._serialize_plan(plan)
                for plan in self._plans.values()
                if table is None or plan["targetTable"] == table
            ]
        plans.sort(key=lambda item: item["planId"], reverse=True)
        return {"items": plans, "total": len(plans)}

    def get_plan(self, plan_id: int) -> dict[str, Any]:
        with self._lock:
            plan = self._plans.get(int(plan_id))
            if plan is None:
                raise MaintenanceError(f"维护计划 {plan_id} 不存在", 404)
            return self._serialize_plan(plan)

    def compare_plans(self, left_id: int, right_id: int) -> dict[str, Any]:
        """对照两份计划：确认范围、已替换范围与仍可治理范围的差异。"""
        with self._lock:
            left = self._plans.get(int(left_id))
            right = self._plans.get(int(right_id))
            if left is None:
                raise MaintenanceError(f"维护计划 {left_id} 不存在", 404)
            if right is None:
                raise MaintenanceError(f"维护计划 {right_id} 不存在", 404)
            left_payload = self._serialize_plan(left)
            right_payload = self._serialize_plan(right)

        left_confirmed = set(left_payload["confirmedScope"]["paths"])
        right_confirmed = set(right_payload["confirmedScope"]["paths"])
        left_governable = set(left_payload["stillGovernable"]["paths"])
        right_governable = set(right_payload["stillGovernable"]["paths"])
        left_replaced = set(left_payload["replacedScope"]["paths"])
        right_replaced = set(right_payload["replacedScope"]["paths"])

        def ordered(paths: set[str]) -> list[str]:
            return [
                path for path in left_payload["confirmedScope"]["paths"]
                + right_payload["confirmedScope"]["paths"]
                if path in paths
            ]

        same_table = left["targetTable"] == right["targetTable"]
        return {
            "left": {
                "planId": left["planId"],
                "targetTable": left["targetTable"],
                "status": left["status"],
            },
            "right": {
                "planId": right["planId"],
                "targetTable": right["targetTable"],
                "status": right["status"],
            },
            "sameTable": same_table,
            "confirmed": {
                "leftOnly": ordered(left_confirmed - right_confirmed),
                "rightOnly": ordered(right_confirmed - left_confirmed),
                "shared": ordered(left_confirmed & right_confirmed),
            },
            "replaced": {
                "leftOnly": ordered(left_replaced - right_replaced),
                "rightOnly": ordered(right_replaced - left_replaced),
                "shared": ordered(left_replaced & right_replaced),
            },
            "stillGovernable": {
                "left": sorted(left_governable),
                "right": sorted(right_governable),
                "onlyLeft": sorted(left_governable - right_governable),
                "onlyRight": sorted(right_governable - left_governable),
            },
            "laterAppended": {
                "left": left_payload["laterAppended"],
                "right": right_payload["laterAppended"],
            },
            "unreplacedInLeft": sorted(left_confirmed - left_replaced),
            "unreplacedInRight": sorted(right_confirmed - right_replaced),
            "note": (
                "laterAppended 中的文件在视图打开后提交，不属于任何已确认范围，"
                "不会被任一份计划算作已替换。"
            ),
        }

    # ---- 内部：快照与对象 -----------------------------------------------

    def _view(self, view_id: int) -> dict[str, Any]:
        view = self._views.get(int(view_id))
        if view is None:
            raise MaintenanceError(f"快照维护视图 {view_id} 不存在", 404)
        return view

    def _committed_snapshots(self, table: str | None = None) -> list[dict[str, Any]]:
        # 按主键排序：追加提交的 id 单调递增，避免空提交时间把新快照排到最前。
        rows = [
            row for row in store.rows("iceberg_commit")
            if str(row.get("status", "")) == "已提交"
            and (table is None or str(row.get("目标表", "")) == table)
        ]
        rows.sort(key=lambda row: int(row.get("id", 0)))
        return rows

    def _build_objects(
        self, view_id: int, table: str, snapshots: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        objects: dict[str, dict[str, Any]] = {}
        for snapshot in snapshots:
            for path in _split_files(snapshot.get("数据文件")):
                if path not in objects:
                    objects[path] = {
                        "object_id": self._next_object_id,
                        "viewId": view_id,
                        "targetTable": table,
                        "dataFile": path,
                        "baseSnapshot": str(snapshot.get("快照编号", "")),
                        "baseSnapshotId": int(snapshot.get("id", 0)),
                        "baseCommitTime": str(snapshot.get("提交时间", "")),
                        "sourceSnapshots": [],
                        "fileSizeMB": _file_size_mb(path),
                    }
                    self._next_object_id += 1
                number = str(snapshot.get("快照编号", ""))
                if number not in objects[path]["sourceSnapshots"]:
                    objects[path]["sourceSnapshots"].append(number)
        return list(objects.values())

    def _snapshot_brief(self, row: dict[str, Any]) -> dict[str, Any]:
        return {
            "id": int(row.get("id", 0)),
            "snapshotNo": str(row.get("快照编号", "")),
            "snapshotId": str(row.get("快照ID", "")),
            "committedAt": str(row.get("提交时间", "")),
            "fileCount": len(_split_files(row.get("数据文件"))),
        }

    def _live_files(self, table: str) -> set[str]:
        files: set[str] = set()
        for snapshot in self._committed_snapshots(table):
            files.update(_split_files(snapshot.get("数据文件")))
        return files

    def _files_after_cursor(
        self, table: str, cursor_id: int,
    ) -> tuple[list[dict[str, Any]], list[str]]:
        """视图/计划游标之后的同表快照，以及其中首次出现的新文件。"""
        snapshots = self._committed_snapshots(table)
        known: set[str] = set()
        for snapshot in snapshots:
            if int(snapshot.get("id", 0)) <= cursor_id:
                known.update(_split_files(snapshot.get("数据文件")))
        later_snapshots: list[dict[str, Any]] = []
        later_paths: list[str] = []
        for snapshot in snapshots:
            if int(snapshot.get("id", 0)) <= cursor_id:
                continue
            later_snapshots.append(self._snapshot_brief(snapshot))
            for path in _split_files(snapshot.get("数据文件")):
                if path not in known and path not in later_paths:
                    later_paths.append(path)
        return later_snapshots, later_paths

    def _active_claim(self, path: str) -> dict[str, Any] | None:
        claim = self._claims.get(path)
        return claim if claim and claim.get("status") == "生效中" else None

    def _decorate_object(self, obj: dict[str, Any]) -> dict[str, Any]:
        item = dict(obj)
        item["objectId"] = int(item.pop("object_id", item.get("objectId", 0)))
        path = str(item.get("dataFile", ""))
        claim = self._active_claim(path)
        replaced = self._replaced.get(path)
        item["occupancy"] = {
            "status": "已占用" if claim else "未占用",
            "jobId": str(claim.get("jobId", "")) if claim else "",
            "planId": int(claim.get("planId", 0)) if claim else None,
        }
        item["replacement"] = {
            "status": "已替换" if replaced else "待处理",
            "planId": int(replaced.get("planId", 0)) if replaced else None,
            "jobId": str(replaced.get("jobId", "")) if replaced else "",
        }
        item["mergeable"] = bool(not replaced and not claim)
        return item

    def _serialize_view(self, view: dict[str, Any]) -> dict[str, Any]:
        objects = [self._decorate_object(obj) for obj in view["objects"]]
        candidates = [obj for obj in objects if obj["mergeable"]]
        later_snapshots, later_paths = self._files_after_cursor(
            view["targetTable"], view["snapshotCursorId"])
        plans = [
            self._serialize_plan(self._plans[plan_id])
            for plan_id in view["planIds"] if plan_id in self._plans
        ]
        return {
            "viewId": view["viewId"],
            "targetTable": view["targetTable"],
            "status": view["status"],
            "openedAt": view["openedAt"],
            "snapshotCursorId": view["snapshotCursorId"],
            "snapshotCursor": view["snapshotCursor"],
            "visibleSnapshots": view["visibleSnapshots"],
            "visibleSnapshotIds": view["visibleSnapshotIds"],
            "objects": objects,
            "candidateCount": len(candidates),
            "candidates": candidates,
            "confirmedCount": sum(
                len(plan["confirmedPaths"]) for plan in self._plans.values()
                if plan["viewId"] == view["viewId"]
            ),
            "replacedCount": sum(
                1 for obj in objects if obj["replacement"]["status"] == "已替换"),
            "plans": plans,
            "newSnapshotsSinceOpen": {
                "count": len(later_snapshots),
                "snapshots": later_snapshots,
            },
            "newFilesSinceOpen": {
                "count": len(later_paths),
                "paths": later_paths,
            },
        }

    # ---- 内部：计划状态 -------------------------------------------------

    def _plan_state(self, plan: dict[str, Any]) -> dict[str, Any]:
        """按当前全局占用/替换状态计算计划的各类范围。

        - unhandled：本计划还没提交过的确认对象；
        - replaced：本计划已经真正替换的文件；
        - stillGovernable：本计划还能继续治理的确认对象（未被替换、未被他人占用）；
        - blocked：被其他计划有效占用、暂时不能动的确认对象；
        - replacedElsewhere：被其他维护计划替换掉的确认对象。
        """
        handled = set(plan["handledPaths"])
        unhandled: list[str] = []
        still_governable: list[str] = []
        blocked: list[str] = []
        replaced_elsewhere: list[str] = []
        replaced_by_self: list[str] = []
        for path in plan["confirmedPaths"]:
            own_replacement = self._replaced.get(path)
            if own_replacement and own_replacement.get("planId") == plan["planId"]:
                replaced_by_self.append(path)
                if path not in handled:
                    unhandled.append(path)
                continue
            if path not in handled:
                unhandled.append(path)
            if own_replacement is not None:
                replaced_elsewhere.append(path)
                continue
            claim = self._active_claim(path)
            if claim and int(claim.get("planId", 0)) != int(plan["planId"]):
                blocked.append(path)
            else:
                still_governable.append(path)
        return {
            "unhandled": unhandled,
            "stillGovernable": still_governable,
            "blocked": blocked,
            "replacedElsewhere": replaced_elsewhere,
            "replacedBySelf": replaced_by_self,
        }

    def _serialize_plan(self, plan: dict[str, Any]) -> dict[str, Any]:
        state = self._plan_state(plan)
        later_snapshots, later_paths = self._files_after_cursor(
            plan["targetTable"], plan["snapshotCursorId"])
        batch_total = max(
            1, math.ceil(len(plan["confirmedPaths"]) / plan["batchSize"]))
        confirmed_objects = []
        for obj in plan["confirmedObjects"]:
            decorated = self._decorate_object(obj)
            item = dict(obj)
            item["occupancy"] = decorated["occupancy"]
            item["replacement"] = decorated["replacement"]
            confirmed_objects.append(item)
        conflict_batches = sum(
            1 for batch in plan["processedBatches"]
            if batch["conflictPaths"] or batch["alreadyReplacedPaths"] or batch["invalidPaths"]
        )
        return {
            "planId": plan["planId"],
            "viewId": plan["viewId"],
            "targetTable": plan["targetTable"],
            "status": plan["status"],
            "confirmedAt": plan["confirmedAt"],
            "batchSize": plan["batchSize"],
            "batchTotal": batch_total,
            "snapshotCursorId": plan["snapshotCursorId"],
            "snapshotCursor": plan["snapshotCursor"],
            "confirmedSnapshotIds": list(plan["confirmedSnapshotIds"]),
            "confirmedScope": {
                "count": len(plan["confirmedPaths"]),
                "paths": list(plan["confirmedPaths"]),
                "objects": confirmed_objects,
            },
            "replacedScope": {
                "count": len(plan["replacedPaths"]),
                "paths": list(plan["replacedPaths"]),
            },
            "remainingScope": {
                "count": len(state["unhandled"]),
                "paths": state["unhandled"],
            },
            "stillGovernable": {
                "count": len(state["stillGovernable"]),
                "paths": state["stillGovernable"],
            },
            "blockedScope": {
                "count": len(state["blocked"]),
                "paths": state["blocked"],
            },
            "replacedByOtherPlans": {
                "count": len(state["replacedElsewhere"]),
                "paths": state["replacedElsewhere"],
            },
            "processedBatches": [dict(batch) for batch in plan["processedBatches"]],
            "processedBatchCount": len(plan["processedBatches"]),
            "conflictBatchCount": conflict_batches,
            "laterAppended": {
                "count": len(later_paths),
                "paths": later_paths,
                "snapshots": later_snapshots,
            },
            "compactionTaskIds": list(plan["compactionTaskIds"]),
        }

    def _submit_payload(
        self,
        view: dict[str, Any],
        plan: dict[str, Any],
        batch_index: int | None,
        attempted: list[str],
        effective: list[dict[str, Any]],
        conflicts: list[dict[str, Any]],
        already_replaced: list[dict[str, Any]],
        invalid: list[dict[str, Any]],
        owner: str,
        batch_total: int,
        *,
        compaction_task: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        plan_payload = self._serialize_plan(plan)
        state = self._plan_state(plan)
        effective_paths = [obj["dataFile"] for obj in effective]
        batch_label = (
            f"第{batch_index}/{batch_total}批" if batch_index is not None else "无待处理批次")
        return {
            "ok": True,
            "viewId": view["viewId"],
            "planId": plan["planId"],
            "targetTable": view["targetTable"],
            "jobId": owner,
            "message": (
                f"第{batch_index}/{batch_total}批维护已提交"
                if batch_index is not None else "已确认范围已经全部处理"
            ),
            "batch": {
                "index": batch_index,
                "total": batch_total,
                "label": batch_label,
                "size": plan["batchSize"],
                "attemptedCount": len(attempted),
                "effectiveCount": len(effective_paths),
                "conflictCount": len(conflicts),
                "alreadyReplacedCount": len(already_replaced),
                "invalidCount": len(invalid),
                "remainingBatchCount": math.ceil(
                    len(state["unhandled"]) / plan["batchSize"]) if state["unhandled"] else 0,
            },
            "attemptedPaths": attempted,
            "effectiveScope": {
                "count": len(effective_paths),
                "paths": effective_paths,
                "objects": [
                    {
                        "dataFile": obj["dataFile"],
                        "baseSnapshot": obj["baseSnapshot"],
                        "baseSnapshotId": obj["baseSnapshotId"],
                    }
                    for obj in effective
                ],
            },
            "conflicts": conflicts,
            "alreadyReplaced": already_replaced,
            "invalidPaths": invalid,
            "laterAppended": {
                "count": len(plan_payload["laterAppended"]["paths"]),
                "paths": plan_payload["laterAppended"]["paths"],
                "snapshots": plan_payload["laterAppended"]["snapshots"],
                "note": "视图打开后追加的快照不属于已确认范围，不参与替换",
            },
            "confirmedScope": plan_payload["confirmedScope"],
            "remainingScope": plan_payload["remainingScope"],
            "stillGovernable": plan_payload["stillGovernable"],
            "plan": plan_payload,
            "compactionTask": compaction_task,
        }


maintenance_service = SnapshotMaintenanceService()
