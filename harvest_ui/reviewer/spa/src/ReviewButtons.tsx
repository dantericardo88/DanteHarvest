import { useState } from 'react'
import { approvePack, rejectPack, deferPack } from './api'

interface Props {
  packId: string
  onDone: () => void
}

export function ReviewButtons({ packId, onDone }: Props) {
  const [busy, setBusy] = useState(false)
  const [err, setErr] = useState<string | null>(null)
  const [receiptId, setReceiptId] = useState('')
  const [reason, setReason] = useState('')

  const runId = `review-${Date.now()}`

  async function handle(action: 'approve' | 'reject' | 'defer') {
    setBusy(true)
    setErr(null)
    try {
      if (action === 'approve') await approvePack(packId, runId, receiptId || undefined)
      else if (action === 'reject') await rejectPack(packId, runId, reason || 'No reason given')
      else await deferPack(packId, runId, reason || 'Deferred')
      onDone()
    } catch (e) {
      setErr(String(e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div style={{ marginTop: 12 }}>
      <input
        placeholder="Receipt ID (for approve)"
        value={receiptId}
        onChange={e => setReceiptId(e.target.value)}
        style={{ width: '100%', marginBottom: 6, padding: '4px 8px', boxSizing: 'border-box' }}
      />
      <input
        placeholder="Reason (for reject / defer)"
        value={reason}
        onChange={e => setReason(e.target.value)}
        style={{ width: '100%', marginBottom: 6, padding: '4px 8px', boxSizing: 'border-box' }}
      />
      <div style={{ display: 'flex', gap: 8 }}>
        <button disabled={busy} onClick={() => handle('approve')}
          style={{ flex: 1, background: '#22c55e', color: '#fff', border: 'none', borderRadius: 4, padding: '6px 0', cursor: 'pointer' }}>
          Approve
        </button>
        <button disabled={busy} onClick={() => handle('defer')}
          style={{ flex: 1, background: '#eab308', color: '#fff', border: 'none', borderRadius: 4, padding: '6px 0', cursor: 'pointer' }}>
          Defer
        </button>
        <button disabled={busy} onClick={() => handle('reject')}
          style={{ flex: 1, background: '#ef4444', color: '#fff', border: 'none', borderRadius: 4, padding: '6px 0', cursor: 'pointer' }}>
          Reject
        </button>
      </div>
      {err && <p style={{ color: '#ef4444', marginTop: 6, fontSize: 13 }}>{err}</p>}
    </div>
  )
}
