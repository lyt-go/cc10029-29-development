import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type {
  ActionResult,
  EntryRow,
  OverviewResult,
  PageResult,
  ReleaseSharedScope,
  ReleaseSubmitResult,
} from '@/data/types'

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

export type ReleaseSubmitOptions = {
  windowIds: number[]
  windowCodes?: string[]
  targetCode?: string
  autoExtract?: boolean
  failWindow?: number | string | null
  failAfterRows?: number | null
}

async function releaseRequest<T>(path: string, init?: RequestInit): Promise<T> {
  const response = await request(path, init)
  const payload = await response.json().catch(() => null)
  if (!response.ok) {
    const detail =
      (payload && typeof payload === 'object' && 'detail' in payload && String(payload.detail)) ||
      '成组发布请求失败'
    throw new Error(detail)
  }
  return payload as T
}

/** 一次提交整组相邻窗口，返回发布清单与逐窗交付结果。 */
export async function submitReleaseGroup(
  options: ReleaseSubmitOptions,
): Promise<ReleaseSubmitResult> {
  return releaseRequest<ReleaseSubmitResult>('/api/release/submit', {
    method: 'POST',
    body: JSON.stringify({
      window_ids: options.windowIds,
      window_codes: options.windowCodes ?? [],
      target_code: options.targetCode,
      auto_extract: options.autoExtract ?? false,
      fail_window: options.failWindow ?? null,
      fail_after_rows: options.failAfterRows ?? null,
    }),
  })
}

/** 按原申请重试整组任务，已交付成员不会重复写入。 */
export async function retryReleaseGroup(manifestId: number): Promise<ReleaseSubmitResult> {
  return releaseRequest<ReleaseSubmitResult>(`/api/release/manifests/${manifestId}/retry`, {
    method: 'POST',
    body: JSON.stringify({}),
  })
}

/** 单独补上故障窗口的数据，之后再重新提交原申请。 */
export async function supplementReleaseWindow(
  manifestId: number,
  windowId: number | string,
  recordCount?: number,
): Promise<Record<string, unknown>> {
  return releaseRequest<Record<string, unknown>>(
    `/api/release/manifests/${manifestId}/supplement`,
    {
      method: 'POST',
      body: JSON.stringify({ window_id: windowId, record_count: recordCount ?? null }),
    },
  )
}

/** 依次对所选窗口执行抽取，抽取完成才可进入成组交付。 */
export async function extractReleaseWindows(windowIds: number[]): Promise<void> {
  for (const windowId of windowIds) {
    await releaseRequest<Record<string, unknown>>(`/api/release/windows/${windowId}/extract`, {
      method: 'POST',
      body: JSON.stringify({}),
    })
  }
}

/** 传输、暂存、对账共享的成组范围：已抵达窗口与待补齐窗口。 */
export async function loadReleaseScope(manifestCode: string): Promise<ReleaseSharedScope> {
  return releaseRequest<ReleaseSharedScope>(`/api/release/scope/${manifestCode}`)
}

export async function loadReleaseManifest(manifestId: number): Promise<EntryRow> {
  return releaseRequest<EntryRow>(`/api/release/manifests/${manifestId}`)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}
