import { useState } from 'react'
import type { PackSummary } from './api'
import { ConfidenceBadge } from './ConfidenceBadge'
import { ReviewButtons } from './ReviewButtons'

interface Props {
  pack: PackSummary
  onRefresh: () => void
}

export function PackCard({ pack, onRefresh }: Props) {
  const [expanded, setExpanded] = useState(false)

  return (
    <div style={{
      border: '1px solid #e5e7eb',
      borderRadius: 8,
      padding: 16,
      marginBottom: 12,
      background: '#fff',
    }}>
      <div style={{ display: 'flex', alignItems: 'flex-start', gap: 12 }}>
        <div style={{ flex: 1 }}>
          <div style={{ fontWeight: 600, fontSize: 15 }}>{pack.title}</div>
          <div style={{ color: '#6b7280', fontSize: 13, marginTop: 2 }}>
            {pack.pack_id} · {pack.pack_type} · {pack.step_count} steps
          </div>
        </div>
        <ConfidenceBadge band={pack.confidence_band} score={pack.confidence_score} />
      </div>

      <button
        onClick={() => setExpanded(e => !e)}
        style={{ marginTop: 10, fontSize: 13, color: '#2563eb', background: 'none', border: 'none', cursor: 'pointer', padding: 0 }}>
        {expanded ? 'Hide review' : 'Review…'}
      </button>

      {expanded && (
        <ReviewButtons packId={pack.pack_id} onDone={() => { setExpanded(false); onRefresh() }} />
      )}
    </div>
  )
}
