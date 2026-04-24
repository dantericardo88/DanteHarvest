import { usePacks } from './usePacks'
import { PackCard } from './PackCard'

const STATUS_FILTER: Record<string, string | undefined> = {
  Pending: 'candidate',
  Deferred: 'deferred',
  Approved: 'approved',
  Rejected: 'rejected',
}

interface Props {
  tab: string
}

export function PackList({ tab }: Props) {
  const status = STATUS_FILTER[tab]
  const { packs, loading, error, refresh } = usePacks(status)

  if (loading) return <p style={{ color: '#6b7280' }}>Loading…</p>
  if (error) return <p style={{ color: '#ef4444' }}>Error: {error}</p>
  if (packs.length === 0) return <p style={{ color: '#6b7280' }}>No packs in this tab.</p>

  return (
    <div>
      {packs.map(p => (
        <PackCard key={p.pack_id} pack={p} onRefresh={refresh} />
      ))}
    </div>
  )
}
