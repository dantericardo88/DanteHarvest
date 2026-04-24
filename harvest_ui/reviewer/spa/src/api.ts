export interface PackSummary {
  pack_id: string
  pack_type: string
  title: string
  promotion_status: string
  confidence_score: number
  confidence_band: string
  step_count: number
}

export interface ChainEntry {
  sequence: number
  signal: string
  machine: string
  timestamp: number
  data: Record<string, unknown>
}

const BASE = '/api'

export async function fetchPacks(status?: string): Promise<PackSummary[]> {
  const url = status ? `${BASE}/packs?status=${status}` : `${BASE}/packs`
  const r = await fetch(url)
  if (!r.ok) throw new Error(`fetchPacks: ${r.status}`)
  return r.json()
}

export async function fetchPack(packId: string): Promise<Record<string, unknown>> {
  const r = await fetch(`${BASE}/packs/${packId}`)
  if (!r.ok) throw new Error(`fetchPack: ${r.status}`)
  return r.json()
}

export async function approvePack(packId: string, runId: string, receiptId?: string): Promise<void> {
  const r = await fetch(`${BASE}/packs/${packId}/approve`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ run_id: runId, receipt_id: receiptId }),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => ({}))
    throw new Error(body?.detail?.error ?? `approve: ${r.status}`)
  }
}

export async function rejectPack(packId: string, runId: string, reason: string): Promise<void> {
  const r = await fetch(`${BASE}/packs/${packId}/reject`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ run_id: runId, reason }),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => ({}))
    throw new Error(body?.detail?.error ?? `reject: ${r.status}`)
  }
}

export async function deferPack(packId: string, runId: string, reason: string): Promise<void> {
  const r = await fetch(`${BASE}/packs/${packId}/defer`, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ run_id: runId, reason }),
  })
  if (!r.ok) {
    const body = await r.json().catch(() => ({}))
    throw new Error(body?.detail?.error ?? `defer: ${r.status}`)
  }
}

export async function fetchChain(runId: string): Promise<ChainEntry[]> {
  const r = await fetch(`${BASE}/runs/${runId}/chain`)
  if (!r.ok) throw new Error(`fetchChain: ${r.status}`)
  return r.json()
}
