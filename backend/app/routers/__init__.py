"""业务路由汇总：固定前缀视图路由在前，通用资源路由兜底。"""
from __future__ import annotations

from app.routers import resource, snapshot_maintenance

# 快照维护视图是固定前缀，必须排在带 /api/{module} 动态段的资源路由之前，
# 否则 /api/snapshot-maintenance/... 会被资源路由当成模块或条目 ID 抢先匹配。
ROUTERS = [*snapshot_maintenance.ROUTERS, resource.router]
