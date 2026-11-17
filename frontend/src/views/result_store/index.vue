<template>
  <section class="page" data-module="result_store">
    <header class="page-head">
      <div>
        <h2>采集仓储管理</h2>
        <p class="page-desc">维护采集结果的存储批次、完整性校验与版本追溯，保证原始数据可回溯。</p>
      </div>
      <div class="page-actions">
        <button class="btn primary" data-testid="open-create" type="button" @click="openCreate">登记结果批次</button>
        <button class="btn" data-testid="export" type="button" @click="exportRows">导出采集仓储清单</button>
      </div>
    </header>

    <div class="stat-row">
      <article v-for="item in stats" :key="item.label" class="stat-card">
        <span class="stat-label">{{ item.label }}</span>
        <strong class="stat-value">{{ item.value }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reload">
      <label v-for="field in filterFields" :key="field" class="filter-item">
        <span>{{ field }}</span>
        <input v-model="filters[field]" :data-testid="`filter-${field}`" :placeholder="`按${field}检索`" />
      </label>
      <label class="filter-item">
        <span>批次状态</span>
        <select v-model="statusFilter" data-testid="filter-status">
          <option value="">全部合法状态</option>
          <option v-for="item in statuses" :key="item" :value="item">{{ item }}</option>
        </select>
      </label>
      <label class="filter-item">
        <span>分发状态</span>
        <select v-model="dispatchFilter" data-testid="filter-分发状态">
          <option value="">全部分发状态</option>
          <option value="已分发">已分发</option>
          <option value="未分发">未分发</option>
        </select>
      </label>
      <label class="filter-item">
        <span>分发记录</span>
        <input v-model="filters['分发记录']" data-testid="filter-分发记录" placeholder="按分发记录编号检索" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" data-testid="reset" type="button" @click="resetFilters">重置条件</button>
    </form>

    <table class="data-table">
      <thead>
        <tr>
          <th v-for="column in displayColumns" :key="column">{{ column }}</th>
          <th>可执行动作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in rows" :key="String(row.id)" :data-testid="`row-${row.id}`" :data-status="String(row.status)">
          <td v-for="column in displayColumns" :key="column">{{ cellValue(row, column) }}</td>
          <td class="row-actions">
            <button
              v-for="action in actions"
              :key="action"
              class="link"
              type="button"
              :data-testid="`action-${row.id}-${action}`"
              @click="runAction(action, row)"
            >
              {{ action }}
            </button>
          </td>
        </tr>
        <tr v-if="!rows.length">
          <td :colspan="displayColumns.length + 1" class="empty-state">暂无采集仓储数据，可先登记结果批次</td>
        </tr>
      </tbody>
    </table>

    <div v-if="showCreate" class="modal-backdrop" data-testid="create-modal">
      <form class="modal" @submit.prevent="submitCreate">
        <h3>登记结果批次</h3>
        <label v-for="field in columns" :key="field" class="create-field">
          <span>{{ field }}</span>
          <input v-model="createValues[field]" :data-testid="`create-${field}`" required />
        </label>
        <div class="page-actions">
          <button class="btn" type="button" @click="showCreate = false">取消</button>
          <button class="btn primary" data-testid="submit-create" type="submit" :disabled="creating">
            {{ creating ? '提交中…' : '确认登记' }}
          </button>
        </div>
      </form>
    </div>

    <footer class="page-foot">
      <span>共 {{ total }} 条采集仓储记录</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref } from 'vue'

import { request } from '@/api/client'

type Row = Record<string, string | number | null>

const ENDPOINT = '/api/result_store'
const columns = ["批次编号", "关联任务", "原始行数", "有效行数", "存储路径", "压缩格式", "校验摘要", "落地时间"]
const dispatchColumns = ["批次状态", "分发状态", "分发记录"]
const displayColumns = [...columns, ...dispatchColumns]
const actions = ["触发校验", "标记通过", "标记失败"]
// 与后端 batch_status.RESULT_STATUSES 保持一致；读取时还会用接口返回值覆盖。
const statuses = ref<string[]>(["接收中", "已落盘", "校验通过", "校验失败", "已分发"])
const metricNames = ["批次总数", "校验通过批次", "已分发批次"]

const rows = ref<Row[]>([])
const total = ref(0)
const errorMessage = ref('')
const filters = ref<Record<string, string>>({})
const statusFilter = ref('')
const dispatchFilter = ref('')
const filterFields = columns.slice(0, 3)
const showCreate = ref(false)
const creating = ref(false)
const createValues = ref<Record<string, string>>({})
const stats = computed(() => [
  { label: metricNames[0], value: total.value },
  { label: metricNames[1], value: rows.value.filter((row) => row.status === '校验通过').length },
  { label: metricNames[2], value: rows.value.filter((row) => row.status === '已分发').length },
])

async function loadStatuses() {
  try {
    const response = await request(`${ENDPOINT}/status-options`)
    if (!response.ok) return
    const payload = await response.json()
    if (Array.isArray(payload.statuses) && payload.statuses.length) {
      statuses.value = payload.statuses
    }
  } catch {
    // 状态口径读取失败时保留本地默认值，不影响列表。
  }
}

function queryParams(): Record<string, string> {
  const params: Record<string, string> = { ...filters.value }
  if (statusFilter.value) params.status = statusFilter.value
  if (dispatchFilter.value) params['分发状态'] = dispatchFilter.value
  return params
}

function cellValue(row: Row, column: string): string | number {
  if (column === '批次状态') return row.status ?? '—'
  return row[column] ?? '—'
}

function resetFilters() {
  filters.value = {}
  statusFilter.value = ''
  dispatchFilter.value = ''
  void reload()
}

async function exportRows() {
  errorMessage.value = ''
  const query = new URLSearchParams(queryParams()).toString()
  try {
    const response = await request(`${ENDPOINT}/export?${query}`)
    if (!response.ok) throw new Error('导出失败，请稍后重试')
    const url = URL.createObjectURL(await response.blob())
    const link = document.createElement('a')
    link.href = url
    link.download = 'result_store.csv'
    link.click()
    URL.revokeObjectURL(url)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '导出失败'
  }
}

function openCreate() {
  createValues.value = Object.fromEntries(columns.map((field) => [field, '']))
  errorMessage.value = ''
  showCreate.value = true
}

async function submitCreate() {
  creating.value = true
  errorMessage.value = ''
  try {
    const response = await request(ENDPOINT, {
      method: 'POST', body: JSON.stringify(createValues.value),
    })
    const payload = await response.json()
    if (!response.ok || !payload.ok) throw new Error(payload.message || '登记失败')
    showCreate.value = false
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '登记失败'
  } finally {
    creating.value = false
  }
}

async function runAction(action: string, row: Row) {
  errorMessage.value = ''
  try {
    const response = await request(`${ENDPOINT}/${row.id}/actions`, {
      method: 'POST',
      body: JSON.stringify({ action }),
    })
    if (!response.ok) {
      throw new Error('采集仓储动作未生效，请稍后重试')
    }
    await reload()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集仓储操作失败'
  }
}

async function reload() {
  errorMessage.value = ''
  const query = new URLSearchParams(queryParams()).toString()
  try {
    const response = await request(`${ENDPOINT}?${query}`)
    if (!response.ok) {
      throw new Error('结果批次列表读取失败')
    }
    const payload = await response.json()
    rows.value = payload.items ?? []
    total.value = payload.total ?? rows.value.length
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '采集仓储列表读取失败'
  }
}

onMounted(async () => {
  await loadStatuses()
  await reload()
})
</script>
