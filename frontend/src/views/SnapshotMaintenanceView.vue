<template>
  <section class="page" data-module="snapshot_maintenance">
    <header class="page-head">
      <div>
        <h2>快照维护数据视图</h2>
        <p class="page-desc">
          按已提交快照确认小文件合并范围；候选逐条列出基准快照，确认后范围冻结，
          视图打开后追加的文件只作为“后来追加”展示。
        </p>
      </div>
      <div class="page-actions">
        <label class="filter-item">
          <span>目标表</span>
          <select v-model="selectedTable" data-testid="table-select">
            <option v-for="table in tables" :key="table.targetTable" :value="table.targetTable">
              {{ table.targetTable }}（{{ table.snapshotCount }} 快照）
            </option>
          </select>
        </label>
        <button class="btn primary" data-testid="open-view" type="button" :disabled="!selectedTable" @click="openView">
          打开维护视图
        </button>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="error">{{ errorMessage }}</p>

    <template v-if="view">
      <div class="stat-row">
        <article class="stat-card">
          <span class="stat-label">视图编号</span>
          <strong class="stat-value">#{{ view.viewId }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">快照游标</span>
          <strong class="stat-value">{{ view.snapshotCursor }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">候选文件</span>
          <strong class="stat-value">{{ view.candidateCount }}</strong>
        </article>
        <article class="stat-card">
          <span class="stat-label">已替换</span>
          <strong class="stat-value">{{ view.replacedCount }}</strong>
        </article>
      </div>

      <p class="status-legend">
        <span v-for="snapshot in view.visibleSnapshots" :key="snapshot.id" class="legend-item">
          {{ snapshot.snapshotNo }}（{{ snapshot.fileCount }} 文件）
        </span>
      </p>

      <div class="analysis-form">
        <label>
          <span>每批文件数</span>
          <input v-model.number="batchSize" data-testid="batch-size" type="number" min="1" max="200" />
        </label>
        <button class="btn" data-testid="confirm-scope" type="button" @click="confirmScope">
          确认维护范围
        </button>
        <button class="btn primary" data-testid="submit-batch" type="button" :disabled="!activePlan" @click="submitBatch">
          提交维护批次
        </button>
        <label>
          <span>追加同表快照（逗号分隔数据文件）</span>
          <input v-model="appendFiles" data-testid="append-files" placeholder="s3://warehouse/.../part-00009.parquet" />
        </label>
        <button class="btn ghost" data-testid="append-snapshot" type="button" @click="appendSnapshot">
          追加已提交快照
        </button>
      </div>

      <h3>可交给合并的候选</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>数据文件</th>
            <th>基准快照</th>
            <th>基准提交时间</th>
            <th>来源快照</th>
            <th>大小(MB)</th>
            <th>占用</th>
            <th>替换</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="item in view.candidates"
            :key="item.objectId"
            :data-testid="`candidate-${item.objectId}`"
          >
            <td>{{ item.dataFile }}</td>
            <td>{{ item.baseSnapshot }}</td>
            <td>{{ item.baseCommitTime }}</td>
            <td>{{ item.sourceSnapshots.join('、') }}</td>
            <td>{{ item.fileSizeMB }}</td>
            <td>{{ item.occupancy.status }}<template v-if="item.occupancy.jobId">（{{ item.occupancy.jobId }}）</template></td>
            <td>{{ item.replacement.status }}</td>
          </tr>
          <tr v-if="!view.candidates.length">
            <td colspan="7" class="empty-state">当前视图没有可合并的候选文件</td>
          </tr>
        </tbody>
      </table>

      <section v-if="submitResult" class="analysis-card" data-testid="submit-result">
        <h3>最近一次维护提交</h3>
        <p>{{ submitResult.message }}（作业 {{ submitResult.jobId }}）</p>
        <dl class="dag-detail">
          <dt>处理批次</dt>
          <dd data-testid="batch-label">{{ submitResult.batch.label }}，本批 {{ submitResult.batch.size }} 个文件</dd>
          <dt>剩余批次</dt>
          <dd>{{ submitResult.batch.remainingBatchCount }}</dd>
          <dt>试图替换</dt>
          <dd data-testid="attempted">{{ submitResult.batch.attemptedCount }} 个</dd>
          <dt>真实生效</dt>
          <dd data-testid="effective">{{ submitResult.batch.effectiveCount }} 个</dd>
          <dt>生效路径</dt>
          <dd>{{ submitResult.effectiveScope.paths.join('、') || '—' }}</dd>
          <dt>冲突</dt>
          <dd>{{ submitResult.conflicts.map((item) => `${item.path}（${item.reason}）`).join('；') || '—' }}</dd>
          <dt>已被其他计划替换</dt>
          <dd>{{ submitResult.alreadyReplaced.map((item) => item.path).join('、') || '—' }}</dd>
          <dt>合并任务</dt>
          <dd>
            {{ submitResult.compactionTask?.任务编号 ?? '—' }}
            <template v-if="submitResult.compactionTask">（{{ submitResult.compactionTask.批次 }}）</template>
          </dd>
          <dt>后来追加（不参与替换）</dt>
          <dd data-testid="later-appended">{{ submitResult.laterAppended.paths.join('、') || '—' }}</dd>
        </dl>
      </section>

      <h3>维护计划</h3>
      <table class="data-table">
        <thead>
          <tr>
            <th>计划</th>
            <th>状态</th>
            <th>已确认范围</th>
            <th>已替换</th>
            <th>剩余待处理</th>
            <th>仍可治理</th>
            <th>被占用阻塞</th>
            <th>被其他计划替换</th>
            <th>处理批次</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="plan in view.plans" :key="plan.planId" :data-testid="`plan-${plan.planId}`">
            <td>#{{ plan.planId }}（游标 {{ plan.snapshotCursor }}）</td>
            <td>{{ plan.status }}</td>
            <td>{{ plan.confirmedScope.count }}</td>
            <td>{{ plan.replacedScope.count }}</td>
            <td>{{ plan.remainingScope.count }}</td>
            <td>{{ plan.stillGovernable.count }}</td>
            <td>{{ plan.blockedScope.count }}</td>
            <td>{{ plan.replacedByOtherPlans.count }}</td>
            <td>{{ plan.processedBatchCount }}/{{ plan.batchTotal }}</td>
          </tr>
          <tr v-if="!view.plans.length">
            <td colspan="9" class="empty-state">还没有确认维护范围</td>
          </tr>
        </tbody>
      </table>

      <p class="status-legend">
        <span>视图打开后新增快照：{{ view.newSnapshotsSinceOpen.count }} 份</span>
        <span>新增文件：{{ view.newFilesSinceOpen.paths.join('、') || '无' }}</span>
      </p>

      <section v-if="view.plans.length >= 2" class="analysis-card">
        <h3>计划对照</h3>
        <div class="analysis-form">
          <label>
            <span>左侧计划</span>
            <select v-model.number="compareLeft" data-testid="compare-left">
              <option v-for="plan in view.plans" :key="plan.planId" :value="plan.planId">
                #{{ plan.planId }}（{{ plan.status }}）
              </option>
            </select>
          </label>
          <label>
            <span>右侧计划</span>
            <select v-model.number="compareRight" data-testid="compare-right">
              <option v-for="plan in view.plans" :key="plan.planId" :value="plan.planId">
                #{{ plan.planId }}（{{ plan.status }}）
              </option>
            </select>
          </label>
          <button class="btn" data-testid="compare-plans" type="button" @click="comparePlans">
            对照两份计划
          </button>
        </div>
        <dl v-if="comparison" class="dag-detail" data-testid="compare-result">
          <dt>共有确认范围</dt>
          <dd>{{ comparison.confirmed.shared.length }} 个文件</dd>
          <dt>左侧独有确认</dt>
          <dd>{{ comparison.confirmed.leftOnly.join('、') || '—' }}</dd>
          <dt>右侧独有确认</dt>
          <dd>{{ comparison.confirmed.rightOnly.join('、') || '—' }}</dd>
          <dt>左侧仍可治理</dt>
          <dd>{{ comparison.stillGovernable.left.join('、') || '无' }}</dd>
          <dt>右侧仍可治理</dt>
          <dd>{{ comparison.stillGovernable.right.join('、') || '无' }}</dd>
          <dt>左侧未替换</dt>
          <dd>{{ comparison.unreplacedInLeft.join('、') || '无' }}</dd>
          <dt>右侧未替换</dt>
          <dd>{{ comparison.unreplacedInRight.join('、') || '无' }}</dd>
          <dt>说明</dt>
          <dd>{{ comparison.note }}</dd>
        </dl>
      </section>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import {
  appendCommittedSnapshot,
  compareMaintenancePlans,
  confirmMaintenanceScope,
  getMaintenanceView,
  listMaintenanceTables,
  openMaintenanceView,
  submitMaintenance,
} from '@/api/backend-service'
import type {
  MaintenanceCompare,
  MaintenanceSubmitResult,
  MaintenanceTable,
  MaintenanceView,
} from '@/api/backend-service'

const tables = ref<MaintenanceTable[]>([])
const selectedTable = ref('')
const view = ref<MaintenanceView | null>(null)
const submitResult = ref<MaintenanceSubmitResult | null>(null)
const batchSize = ref(20)
const appendFiles = ref('')
const errorMessage = ref('')
const jobId = ref('')
const compareLeft = ref(0)
const compareRight = ref(0)
const comparison = ref<MaintenanceCompare | null>(null)

const activePlan = computed(() => {
  if (!view.value || !view.value.plans.length) return null
  return view.value.plans[view.value.plans.length - 1]
})

async function run(task: () => Promise<void>) {
  errorMessage.value = ''
  try {
    await task()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '操作失败'
  }
}

async function loadTables() {
  const payload = await listMaintenanceTables()
  tables.value = payload.tables
  if (!selectedTable.value && payload.tables.length) {
    selectedTable.value = payload.tables[0].targetTable
  }
}

function openView() {
  void run(async () => {
    view.value = await openMaintenanceView(selectedTable.value)
    submitResult.value = null
    jobId.value = ''
  })
}

function confirmScope() {
  if (!view.value) return
  void run(async () => {
    await confirmMaintenanceScope(
      view.value!.viewId,
      view.value!.candidates.map((item) => item.objectId),
      batchSize.value,
    )
    view.value = await getMaintenanceView(view.value!.viewId)
    jobId.value = jobId.value || `job-ui-${view.value!.viewId}`
  })
}

function comparePlans() {
  void run(async () => {
    if (!compareLeft.value || !compareRight.value) {
      if (!view.value || view.value.plans.length < 2) throw new Error('至少需要两份维护计划')
      compareLeft.value = view.value.plans[0].planId
      compareRight.value = view.value.plans[1].planId
    }
    comparison.value = await compareMaintenancePlans(compareLeft.value, compareRight.value)
  })
}

function submitBatch() {
  if (!view.value || !activePlan.value) return
  void run(async () => {
    jobId.value = jobId.value || `job-ui-${view.value!.viewId}`
    submitResult.value = await submitMaintenance(
      view.value!.viewId,
      activePlan.value!.planId,
      jobId.value,
      batchSize.value,
    )
    view.value = await getMaintenanceView(view.value!.viewId)
  })
}

function appendSnapshot() {
  if (!view.value) return
  void run(async () => {
    const files = appendFiles.value
      .split(',')
      .map((item) => item.trim())
      .filter(Boolean)
    if (!files.length) throw new Error('请填写至少一个数据文件')
    await appendCommittedSnapshot(view.value!.targetTable, files, 5000)
    appendFiles.value = ''
    view.value = await getMaintenanceView(view.value!.viewId)
  })
}

onMounted(() => {
  void run(loadTables)
})
</script>
