"""快照维护数据视图路由：按已提交快照确认范围、提交维护并核对批次。"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query

from app.schemas import (
    MaintenanceAppendPayload,
    MaintenancePlanPayload,
    MaintenanceSubmitPayload,
    MaintenanceViewPayload,
)
from app.services.snapshot_maintenance import MaintenanceError, maintenance_service


def _fail(error: MaintenanceError) -> HTTPException:
    return HTTPException(status_code=error.status_code, detail=error.message)


def build_router(prefix: str) -> APIRouter:
    """同一组端点按给定前缀注册，兼容中划线/下划线两种模块 key。"""
    router = APIRouter(prefix=prefix, tags=["快照维护数据视图"])

    @router.get("/tables")
    def list_maintenance_tables() -> dict[str, Any]:
        """列出可打开维护视图的目标表及其已提交快照。"""
        return maintenance_service.list_tables()

    @router.post("/views")
    def open_maintenance_view(payload: MaintenanceViewPayload) -> dict[str, Any]:
        """打开快照维护数据视图，冻结当时可见的已提交快照。"""
        try:
            return maintenance_service.open_view(payload.targetTable, payload.snapshotId)
        except MaintenanceError as error:
            raise _fail(error) from error

    @router.post("/snapshots")
    def append_committed_snapshot(payload: MaintenanceAppendPayload) -> dict[str, Any]:
        """向目标表追加一份已提交快照，用来复现“视图打开后又写入”的场景。"""
        try:
            snapshot = maintenance_service.append_committed_snapshot(
                payload.targetTable, payload.dataFiles, row_count=payload.rowCount or 0)
        except MaintenanceError as error:
            raise _fail(error) from error
        return {"ok": True, "message": "已追加快照", "snapshot": snapshot}

    @router.get("/views/{view_id}")
    def get_maintenance_view(
        view_id: int,
        candidate_only: bool = Query(default=False),
    ) -> dict[str, Any]:
        """读取维护视图；candidate_only=true 时只返回可交给合并的候选。"""
        try:
            return maintenance_service.get_view(view_id, candidate_only=candidate_only)
        except MaintenanceError as error:
            raise _fail(error) from error

    @router.get("/views/{view_id}/candidates")
    def list_merge_candidates(view_id: int) -> dict[str, Any]:
        """查询可交给小文件合并的候选文件，逐条带基准快照。"""
        try:
            return maintenance_service.list_candidates(view_id)
        except MaintenanceError as error:
            raise _fail(error) from error

    @router.post("/views/{view_id}/plans")
    def confirm_maintenance_scope(
        view_id: int,
        payload: MaintenancePlanPayload,
    ) -> dict[str, Any]:
        """确认维护范围；确认后范围冻结，后续条件变化仍可查。"""
        try:
            return maintenance_service.confirm_scope(
                view_id, payload.objectIds, payload.batchSize or 20)
        except MaintenanceError as error:
            raise _fail(error) from error

    @router.post("/views/{view_id}/submit")
    def submit_maintenance(
        view_id: int,
        payload: MaintenanceSubmitPayload,
    ) -> dict[str, Any]:
        """提交一批已确认对象，返回试图替换路径与真实生效范围。"""
        try:
            return maintenance_service.submit_maintenance(
                view_id,
                plan_id=payload.planId,
                job_id=payload.jobId,
                batch_size=payload.batchSize,
            )
        except MaintenanceError as error:
            raise _fail(error) from error

    @router.get("/plans")
    def list_maintenance_plans(
        table: str | None = Query(default=None),
    ) -> dict[str, Any]:
        """查看所有维护计划，确认后的范围保持可查。"""
        return maintenance_service.list_plans(table)

    @router.get("/plans/compare")
    def compare_maintenance_plans(
        left: int = Query(..., description="左侧维护计划 ID"),
        right: int = Query(..., description="右侧维护计划 ID"),
    ) -> dict[str, Any]:
        """对照两份计划：确认范围、已替换范围与仍可治理范围。"""
        try:
            return maintenance_service.compare_plans(left, right)
        except MaintenanceError as error:
            raise _fail(error) from error

    @router.get("/plans/{plan_id}")
    def get_maintenance_plan(plan_id: int) -> dict[str, Any]:
        """读取单个维护计划：已确认范围、剩余可治理范围与后来追加的文件。"""
        try:
            return maintenance_service.get_plan(plan_id)
        except MaintenanceError as error:
            raise _fail(error) from error

    return router


# 兼容两种模块 key 写法；固定前缀必须排在 /api/{module} 动态资源路由之前。
ROUTERS = [
    build_router("/api/snapshot-maintenance"),
    build_router("/api/snapshot_maintenance"),
]
