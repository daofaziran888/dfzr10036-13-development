import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

// ---- 快照维护数据视图 ----

export type MaintenanceSnapshot = {
  id: number
  snapshotNo: string
  snapshotId: string
  committedAt: string
  fileCount: number
}

export type MaintenanceObject = {
  objectId: number
  viewId: number
  targetTable: string
  dataFile: string
  baseSnapshot: string
  baseSnapshotId: number
  baseCommitTime: string
  sourceSnapshots: string[]
  fileSizeMB: number
  occupancy: { status: string; jobId: string; planId: number | null }
  replacement: { status: string; planId: number | null; jobId: string }
  mergeable: boolean
}

export type MaintenanceBatch = {
  index: number | null
  total: number
  label: string
  size: number
  attemptedCount: number
  effectiveCount: number
  conflictCount: number
  alreadyReplacedCount: number
  invalidCount: number
  remainingBatchCount: number
}

export type MaintenancePlan = {
  planId: number
  viewId: number
  targetTable: string
  status: string
  confirmedAt: string
  batchSize: number
  batchTotal: number
  snapshotCursorId: number
  snapshotCursor: string
  confirmedSnapshotIds: number[]
  confirmedScope: { count: number; paths: string[]; objects: MaintenanceObject[] }
  replacedScope: { count: number; paths: string[] }
  remainingScope: { count: number; paths: string[] }
  stillGovernable: { count: number; paths: string[] }
  blockedScope: { count: number; paths: string[] }
  replacedByOtherPlans: { count: number; paths: string[] }
  processedBatches: Record<string, unknown>[]
  processedBatchCount: number
  conflictBatchCount: number
  laterAppended: { count: number; paths: string[]; snapshots: MaintenanceSnapshot[] }
  compactionTaskIds: number[]
}

export type MaintenanceView = {
  viewId: number
  targetTable: string
  status: string
  openedAt: string
  snapshotCursorId: number
  snapshotCursor: string
  visibleSnapshots: MaintenanceSnapshot[]
  visibleSnapshotIds: number[]
  objects: MaintenanceObject[]
  candidateCount: number
  candidates: MaintenanceObject[]
  confirmedCount: number
  replacedCount: number
  plans: MaintenancePlan[]
  newSnapshotsSinceOpen: { count: number; snapshots: MaintenanceSnapshot[] }
  newFilesSinceOpen: { count: number; paths: string[] }
}

export type MaintenanceTable = {
  targetTable: string
  snapshotCount: number
  fileCount: number
  candidateCount: number
  replacedCount: number
  occupiedCount: number
  latestSnapshot: MaintenanceSnapshot
  snapshots: MaintenanceSnapshot[]
}

export type MaintenanceSubmitResult = {
  ok: boolean
  viewId: number
  planId: number
  targetTable: string
  jobId: string
  message: string
  batch: MaintenanceBatch
  attemptedPaths: string[]
  effectiveScope: {
    count: number
    paths: string[]
    objects: { dataFile: string; baseSnapshot: string; baseSnapshotId: number }[]
  }
  conflicts: { path: string; owner: string; planId: number; reason: string }[]
  alreadyReplaced: { path: string; planId: number; owner: string; reason: string }[]
  invalidPaths: { path: string; reason: string }[]
  laterAppended: {
    count: number
    paths: string[]
    snapshots: MaintenanceSnapshot[]
    note: string
  }
  confirmedScope: MaintenancePlan['confirmedScope']
  remainingScope: MaintenancePlan['remainingScope']
  stillGovernable: MaintenancePlan['stillGovernable']
  plan: MaintenancePlan
  compactionTask: Record<string, string | number | boolean> | null
}

async function readJson<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const detail =
      payload && typeof payload.detail === 'string' ? payload.detail : `接口返回 ${response.status}`
    throw new Error(detail)
  }
  return payload as T
}

export async function listMaintenanceTables(): Promise<{ tables: MaintenanceTable[]; count: number }> {
  return readJson('/api/snapshot-maintenance/tables')
}

export async function openMaintenanceView(
  targetTable: string,
  snapshotId?: number,
): Promise<MaintenanceView> {
  return readJson('/api/snapshot-maintenance/views', {
    method: 'POST',
    body: JSON.stringify({ targetTable, snapshotId }),
  })
}

export async function getMaintenanceView(viewId: number): Promise<MaintenanceView> {
  return readJson(`/api/snapshot-maintenance/views/${viewId}`)
}

export async function confirmMaintenanceScope(
  viewId: number,
  objectIds: number[],
  batchSize: number,
): Promise<MaintenancePlan & { skipped: { objectId: number; reason: string }[] }> {
  return readJson(`/api/snapshot-maintenance/views/${viewId}/plans`, {
    method: 'POST',
    body: JSON.stringify({ objectIds, batchSize }),
  })
}

export async function submitMaintenance(
  viewId: number,
  planId: number,
  jobId: string,
  batchSize?: number,
): Promise<MaintenanceSubmitResult> {
  return readJson(`/api/snapshot-maintenance/views/${viewId}/submit`, {
    method: 'POST',
    body: JSON.stringify({ planId, jobId, batchSize }),
  })
}

export async function appendCommittedSnapshot(
  targetTable: string,
  dataFiles: string[],
  rowCount: number,
): Promise<{ ok: boolean; message: string; snapshot: MaintenanceSnapshot }> {
  return readJson('/api/snapshot-maintenance/snapshots', {
    method: 'POST',
    body: JSON.stringify({ targetTable, dataFiles, rowCount }),
  })
}

export async function listMaintenancePlans(targetTable: string): Promise<{ items: MaintenancePlan[]; total: number }> {
  const query = new URLSearchParams({ table: targetTable }).toString()
  return readJson(`/api/snapshot-maintenance/plans?${query}`)
}

export type MaintenanceCompare = {
  left: { planId: number; targetTable: string; status: string }
  right: { planId: number; targetTable: string; status: string }
  sameTable: boolean
  confirmed: { leftOnly: string[]; rightOnly: string[]; shared: string[] }
  replaced: { leftOnly: string[]; rightOnly: string[]; shared: string[] }
  stillGovernable: { left: string[]; right: string[]; onlyLeft: string[]; onlyRight: string[] }
  laterAppended: {
    left: { count: number; paths: string[] }
    right: { count: number; paths: string[] }
  }
  unreplacedInLeft: string[]
  unreplacedInRight: string[]
  note: string
}

export async function compareMaintenancePlans(
  left: number,
  right: number,
): Promise<MaintenanceCompare> {
  return readJson(`/api/snapshot-maintenance/plans/compare?left=${left}&right=${right}`)
}
