"""小文件合并服务：合并后级联Iceberg过期清理和Metric聚合更新。"""
from __future__ import annotations

from typing import Any

from app.services.base import BaseModuleService
from app.store import store


class CompactionService(BaseModuleService):
    MODULE = "compaction"
    ENTITY = "合并任务"
    OPERATION_TERM = "合并"
    LIST_FIELDS = ["任务编号", "目标表", "文件数量", "合并策略", "触发阈值", "执行内存", "合并耗时", "释放空间"]
    REQUIRED_FIELDS = ["任务编号", "目标表", "文件数量"]
    STATUS_ORDER = ["待执行", "合并中", "已完成", "合并失败"]
    ACTION_RULES = {"启动合并": "合并中", "取消合并": "已完成", "标记失败": "合并失败"}
    NEGATIVE_ACTIONS = []

    def create_from_confirmed_scope(
        self,
        *,
        target_table: str,
        objects: list[dict[str, Any]],
        view_id: int,
        plan_id: int,
        job_id: str,
        batch_index: int,
        batch_total: int,
        confirmed_snapshot_ids: list[int],
        attempted_paths: list[str] | None = None,
    ) -> dict[str, Any]:
        """用快照维护视图里已确认的对象建立合并任务。

        任务按“已确认且本批提交了的对象”建立（文件数量 = 试图替换数），
        “真实生效路径”只包含真正拿到写入占用并被替换的文件。已确认对象里
        的文件可能因为并发占用或已被替换而没有生效，两者都保留下来供核对。
        """
        comp_rows = store.rows("compaction")
        new_id = max((int(r.get("id", 0)) for r in comp_rows), default=0) + 1
        effective_paths = [str(obj.get("dataFile", "")) for obj in objects]
        attempted = list(attempted_paths) if attempted_paths is not None else effective_paths
        base_snapshots: list[str] = []
        for obj in objects:
            snapshot = str(obj.get("baseSnapshot", ""))
            if snapshot and snapshot not in base_snapshots:
                base_snapshots.append(snapshot)
        total_files = sum(int(obj.get("fileSizeMB", 0) or 0) for obj in objects)
        ineffective = [path for path in attempted if path not in effective_paths]
        task = {
            "id": new_id,
            "任务编号": f"CMP-{new_id:04d}",
            "目标表": target_table,
            "文件数量": len(attempted),
            "合并策略": "BinPack-快照维护确认范围",
            "触发阈值": f"快照维护计划#{plan_id} 已确认对象",
            "执行内存": "2GB",
            "合并耗时": "未执行",
            "释放空间": f"预估{total_files}MB",
            "status": "待执行" if effective_paths else "已完成",
            "pending": bool(effective_paths),
            "abnormal": bool(ineffective),
            "维护视图": f"快照维护数据视图#{view_id}",
            "维护计划": f"维护计划#{plan_id}",
            "提交作业": job_id,
            "批次": f"第{batch_index}/{batch_total}批",
            "基准快照": ",".join(base_snapshots),
            "确认快照ID": ",".join(str(item) for item in confirmed_snapshot_ids),
            "确认对象数": len(attempted),
            "生效文件数": len(effective_paths),
            "试图替换路径": ",".join(attempted),
            "真实生效路径": ",".join(effective_paths),
            "未生效路径": ",".join(ineffective),
        }
        comp_rows.append(task)
        return task

    def trigger_expire_and_metric(self, entry_id: int) -> dict[str, Any] | None:
        """合并完成后触发expire_snap清理和metric_aggr聚合更新。

        数据流链路: compaction -> expire_snap + metric_aggr
        """
        entry = self.get_entry(entry_id)
        if entry is None or entry.get("status") != "已完成":
            return None

        table = str(entry.get("目标表", "unknown"))
        results: dict[str, Any] = {"expire_snap": None, "metric_aggr": None}

        # 触发 expire_snap
        for exp in store.rows("expire_snap"):
            if str(exp.get("目标表", "")) == table:
                exp["status"] = "预演完成"
                exp["pending"] = True
                exp["abnormal"] = False
                exp["预演结果"] = f"可清理{entry.get('文件数量', 0) * 3}个历史快照"
                results["expire_snap"] = exp
                break

        # 创建 metric_aggr 聚合
        metric_rows = store.rows("metric_aggr")
        new_metric_id = max((int(r.get("id", 0)) for r in metric_rows), default=0) + 1
        metric = {
            "id": new_metric_id,
            "窗口编号": f"METR-{new_metric_id:04d}",
            "数据流": table,
            "聚合键": "合并批次",
            "聚合函数": "SUM(file_count), AVG(file_size)",
            "窗口宽度": "1h",
            "输出频率": "每次合并后",
            "延迟更新": "实时",
            "最后写入": "2026-11-18 10:42:00",
            "status": "聚合中",
            "pending": True,
            "abnormal": False,
        }
        metric_rows.append(metric)
        results["metric_aggr"] = metric

        return results