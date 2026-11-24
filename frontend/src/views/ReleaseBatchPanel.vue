<template>
  <section class="batch-panel" data-testid="batch-panel">
    <header class="batch-head">
      <div>
        <h3>多窗口批量提交</h3>
        <p class="batch-desc">
          勾选同一源端的相邻窗口，一次提交整组。只有抽取完成的窗口才可对外交付，
          失败位置可按原窗口恢复。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="batch-extract" type="button" @click="extractSelected">
          先完成所选窗口抽取
        </button>
        <button class="btn" data-testid="batch-clear" type="button" @click="clearSelection">
          清空选择
        </button>
        <button
          class="btn primary"
          data-testid="batch-submit"
          type="button"
          :disabled="submitting || !selectedIds.length"
          @click="submitGroup"
        >
          {{ submitting ? '提交中…' : `提交整组（${selectedIds.length}）` }}
        </button>
      </div>
    </header>

    <div class="batch-grid">
      <div class="batch-windows">
        <div class="batch-source">
          <label>
            <span>按源端筛选</span>
            <select v-model="sourceFilter" data-testid="batch-source">
              <option value="">全部源端</option>
              <option v-for="source in sources" :key="source" :value="source">{{ source }}</option>
            </select>
          </label>
          <span class="batch-hint">{{ adjacencyHint }}</span>
        </div>
        <table class="data-table">
          <thead>
            <tr>
              <th class="pick-col">选择</th>
              <th>窗口编号</th>
              <th>源端编号</th>
              <th>窗口起点</th>
              <th>抽取状态</th>
              <th>已抽行数</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="window in visibleWindows"
              :key="window.id"
              :class="{ selected: selectedIds.includes(Number(window.id)) }"
              :data-testid="`batch-window-${window.id}`"
            >
              <td class="pick-col">
                <input
                  type="checkbox"
                  :checked="selectedIds.includes(Number(window.id))"
                  :data-testid="`batch-pick-${window.id}`"
                  @change="toggleWindow(window)"
                />
              </td>
              <td>{{ window['窗口编号'] }}</td>
              <td>{{ window['源端编号'] }}</td>
              <td>{{ window['窗口起点'] }}</td>
              <td>{{ window.status }}</td>
              <td>{{ window.extracted_rows ?? 0 }} / {{ window['抽取条数'] }}</td>
            </tr>
            <tr v-if="!visibleWindows.length">
              <td colspan="6" class="empty-state">当前源端暂无可选窗口</td>
            </tr>
          </tbody>
        </table>
      </div>

      <aside class="batch-side">
        <label class="batch-field">
          <span>目标编号</span>
          <input v-model="targetCode" data-testid="batch-target" placeholder="TARG-0001" />
        </label>
        <label class="batch-field inline">
          <input v-model="autoExtract" type="checkbox" data-testid="batch-auto-extract" />
          <span>提交时自动补齐未抽取窗口</span>
        </label>
        <p class="batch-note">
          抽取未完成的窗口仍会进入清单，但标记为「待补齐」，不会对外交付。
        </p>
      </aside>
    </div>

    <p v-if="batchError" class="error-text" data-testid="batch-error">{{ batchError }}</p>

    <section v-if="manifest" class="manifest-card" data-testid="manifest-card">
      <header class="manifest-head">
        <div>
          <strong data-testid="manifest-code">{{ manifest.manifest_code }}</strong>
          <span class="manifest-status" data-testid="manifest-status">{{ manifest.status }}</span>
          <span class="manifest-meta">
            有效窗口 {{ manifest.effective_windows }} / {{ manifest.members.length }}，
            已交付 {{ manifest.delivered_windows }}，待补齐 {{ manifest.pending_windows }}
          </span>
        </div>
        <div class="page-actions">
          <button class="btn" data-testid="manifest-retry" type="button" @click="retryGroup">
            重试这组任务
          </button>
          <button class="btn ghost" data-testid="manifest-refresh" type="button" @click="refreshScope">
            刷新到达范围
          </button>
        </div>
      </header>

      <table class="data-table member-table">
        <thead>
          <tr>
            <th>窗口编号</th>
            <th>应到行</th>
            <th>实际行</th>
            <th>失败位置</th>
            <th>交付状态</th>
            <th>到达状态</th>
            <th>交付次数</th>
            <th>操作</th>
          </tr>
        </thead>
        <tbody>
          <tr
            v-for="member in manifest.members"
            :key="member.window_id"
            :data-testid="`member-${member.window_id}`"
            :data-delivery-status="member.delivery_status"
          >
            <td>{{ member.window_code }}</td>
            <td>{{ member.expected_rows }}</td>
            <td>{{ member.actual_rows }}</td>
            <td>{{ member.failure_position ?? '—' }}</td>
            <td>{{ member.delivery_status }}</td>
            <td>{{ member.arrival_status }}</td>
            <td>{{ member.delivery_count }}</td>
            <td>
              <button
                v-if="member.delivery_status !== '已交付'"
                class="link"
                type="button"
                :data-testid="`member-supplement-${member.window_id}`"
                @click="supplement(member)"
              >
                单独补数
              </button>
              <span v-else class="batch-note">已交付</span>
            </td>
          </tr>
        </tbody>
      </table>

      <div class="scope-grid">
        <article class="scope-card">
          <h4>共享发布清单</h4>
          <p>已抵达：{{ listText(scope?.manifest?.arrived_windows) }}</p>
          <p>待补齐：{{ listText(scope?.manifest?.pending_windows) }}</p>
        </article>
        <article class="scope-card">
          <h4>暂存交付范围</h4>
          <p>已抵达：{{ listText(scope?.staging?.arrived_windows) }}</p>
          <p>待补齐：{{ listText(scope?.staging?.waiting_windows) }}</p>
        </article>
        <article class="scope-card">
          <h4>到达对账</h4>
          <p>已抵达：{{ listText(scope?.reconcile?.arrived_windows) }}</p>
          <p>待补齐：{{ listText(scope?.reconcile?.waiting_windows) }}</p>
        </article>
      </div>

      <p v-if="notice" class="batch-notice" data-testid="batch-notice">{{ notice }}</p>
    </section>
  </section>
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'

import {
  extractReleaseWindows,
  listEntries,
  loadReleaseManifest,
  loadReleaseScope,
  retryReleaseGroup,
  submitReleaseGroup,
  supplementReleaseWindow,
} from '@/api/backend-service'
import type {
  EntryRow,
  ReleaseMember,
  ReleaseScope,
  ReleaseSharedScope,
  ReleaseSubmitResult,
} from '@/data/types'

type WindowRow = EntryRow & { extracted_rows?: number }

const props = defineProps<{ rows: WindowRow[]; refreshToken?: number }>()

const selectedIds = ref<number[]>([])
const sourceFilter = ref('')
const targetCode = ref('TARG-0001')
const autoExtract = ref(false)
const submitting = ref(false)
const batchError = ref('')
const notice = ref('')
const manifest = ref<ReleaseSubmitResult | null>(null)
const scope = ref<ReleaseSharedScope | null>(null)
const extractedCounts = ref<Record<number, number>>({})

const sources = computed(() => {
  const values = props.rows.map((row) => String(row['源端编号'] ?? '')).filter(Boolean)
  return Array.from(new Set(values))
})

const visibleWindows = computed<WindowRow[]>(() =>
  props.rows
    .filter((row) => !sourceFilter.value || String(row['源端编号']) === sourceFilter.value)
    .map((row): WindowRow => ({
      ...row,
      extracted_rows: extractedCounts.value[Number(row.id)] ?? 0,
    }))
    .sort((left, right) => String(left['窗口起点']).localeCompare(String(right['窗口起点']))
      || Number(left.id) - Number(right.id)),
)

const adjacencyHint = computed(() => {
  if (selectedIds.value.length < 2) return '至少勾选两个相邻窗口'
  const ordered = visibleWindows.value.filter((row) => selectedIds.value.includes(Number(row.id)))
  const contiguous = ordered.every(
    (row, index) => index === 0
      || Number(row.id) - Number(ordered[index - 1].id) === 1,
  )
  return contiguous ? '所选窗口相邻，可成组提交' : '所选窗口不相邻，请补齐中间窗口'
})

watch(
  () => props.refreshToken,
  () => {
    void refreshExtractedCounts()
    if (manifest.value) void refreshScope()
  },
  { immediate: true },
)

watch(sourceFilter, () => {
  selectedIds.value = []
})

async function refreshExtractedCounts() {
  const counts: Record<number, number> = {}
  await Promise.all(
    props.rows.map(async (row) => {
      try {
        const detail = await listEntries('extract_window', { 窗口编号: String(row['窗口编号'] ?? '') })
        const match = detail.items.find((item) => Number(item.id) === Number(row.id))
        const watermark = Number((match?.['推进水位'] ?? row['推进水位']) || 0)
        counts[Number(row.id)] = Number.isFinite(watermark) ? watermark : 0
      } catch {
        counts[Number(row.id)] = 0
      }
    }),
  )
  extractedCounts.value = counts
}

function toggleWindow(window: WindowRow) {
  const id = Number(window.id)
  if (selectedIds.value.includes(id)) {
    selectedIds.value = selectedIds.value.filter((item) => item !== id)
  } else {
    selectedIds.value = [...selectedIds.value, id]
  }
}

function clearSelection() {
  selectedIds.value = []
  batchError.value = ''
  notice.value = ''
}

async function extractSelected() {
  if (!selectedIds.value.length) return
  submitting.value = true
  batchError.value = ''
  try {
    await extractReleaseWindows(selectedIds.value)
    notice.value = '所选窗口已完成抽取，可提交整组'
    await refreshExtractedCounts()
  } catch (error) {
    batchError.value = error instanceof Error ? error.message : '抽取失败'
  } finally {
    submitting.value = false
  }
}

async function submitGroup() {
  submitting.value = true
  batchError.value = ''
  notice.value = ''
  try {
    const result = await submitReleaseGroup({
      windowIds: selectedIds.value,
      targetCode: targetCode.value || undefined,
      autoExtract: autoExtract.value,
    })
    manifest.value = result
    notice.value = result.message
    await refreshScope()
  } catch (error) {
    batchError.value = error instanceof Error ? error.message : '成组提交失败'
  } finally {
    submitting.value = false
  }
}

async function retryGroup() {
  if (!manifest.value) return
  submitting.value = true
  batchError.value = ''
  try {
    const result = await retryReleaseGroup(manifest.value.manifest_id)
    manifest.value = result
    notice.value = result.message
    await refreshScope()
  } catch (error) {
    batchError.value = error instanceof Error ? error.message : '重试失败'
  } finally {
    submitting.value = false
  }
}

async function supplement(member: ReleaseMember) {
  if (!manifest.value) return
  batchError.value = ''
  try {
    const result = await supplementReleaseWindow(manifest.value.manifest_id, member.window_id)
    notice.value = String(result.message ?? '已补数，请重新提交原申请')
    await refreshManifest()
    await refreshScope()
  } catch (error) {
    batchError.value = error instanceof Error ? error.message : '补数失败'
  }
}

async function refreshManifest() {
  if (!manifest.value) return
  const entry = await loadReleaseManifest(manifest.value.manifest_id)
  manifest.value = {
    ...manifest.value,
    status: String(entry.status ?? manifest.value.status),
    members: (entry.members as ReleaseMember[]) ?? manifest.value.members,
    scope: (entry.scope as ReleaseScope) ?? manifest.value.scope,
  }
}

async function refreshScope() {
  if (!manifest.value) return
  try {
    scope.value = await loadReleaseScope(manifest.value.manifest_code)
    const delivered = scope.value.manifest.delivered_windows.length
    manifest.value = {
      ...manifest.value,
      delivered_windows: delivered,
      pending_windows: scope.value.manifest.pending_windows.length,
    }
  } catch {
    scope.value = null
  }
}

function listText(values: string[] | undefined) {
  return values && values.length ? values.join('、') : '无'
}
</script>

<style scoped>
.batch-panel { margin: 16px 0; }
.batch-head { display: flex; justify-content: space-between; align-items: flex-start; gap: 16px; }
.batch-head h3 { margin: 0; font-size: 15px; }
.batch-desc { color: var(--muted); font-size: 12px; margin: 4px 0 0; max-width: 720px; }
.batch-grid { display: grid; grid-template-columns: minmax(0, 3fr) minmax(220px, 1fr); gap: 12px; margin-top: 10px; }
.batch-source { display: flex; align-items: center; gap: 12px; margin-bottom: 8px; }
.batch-source label span { display: block; font-size: 12px; color: var(--muted); }
.batch-hint { font-size: 12px; color: var(--muted); }
.batch-side { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; }
.batch-field { display: block; margin-bottom: 10px; }
.batch-field span { display: block; font-size: 12px; color: var(--muted); margin-bottom: 4px; }
.batch-field input:not([type='checkbox']) { width: 100%; box-sizing: border-box; border: 1px solid var(--border); border-radius: 6px; padding: 8px; }
.batch-field.inline { display: flex; align-items: center; gap: 8px; }
.batch-field.inline span { margin: 0; }
.batch-note { font-size: 12px; color: var(--muted); }
.batch-notice { font-size: 13px; color: #0a7a3d; }
.pick-col { width: 56px; text-align: center; }
tr.selected { background: #eef4ff; }
.manifest-card { background: #fff; border: 1px solid var(--border); border-radius: 8px; padding: 12px; margin-top: 12px; }
.manifest-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; flex-wrap: wrap; }
.manifest-status { margin: 0 8px; padding: 2px 8px; border-radius: 10px; background: #eef4ff; color: var(--brand); font-size: 12px; }
.manifest-meta { color: var(--muted); font-size: 12px; }
.member-table { margin-top: 8px; }
.scope-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 10px; margin-top: 10px; }
.scope-card { border: 1px solid var(--border); border-radius: 8px; padding: 10px; background: #fafbfc; }
.scope-card h4 { margin: 0 0 6px; font-size: 13px; }
.scope-card p { margin: 2px 0; font-size: 12px; color: var(--muted); }
.error-text { color: #b42318; font-size: 12px; }
@media (max-width: 960px) {
  .batch-grid, .scope-grid { grid-template-columns: minmax(0, 1fr); }
}
</style>
