const COLORS: Record<string, string> = {
  GREEN: '#22c55e',
  YELLOW: '#eab308',
  ORANGE: '#f97316',
  RED: '#ef4444',
}

export function ConfidenceBadge({ band, score }: { band: string; score: number }) {
  return (
    <span style={{
      background: COLORS[band] ?? '#6b7280',
      color: '#fff',
      borderRadius: 4,
      padding: '2px 8px',
      fontSize: 12,
      fontWeight: 600,
    }}>
      {band} {(score * 100).toFixed(0)}%
    </span>
  )
}
