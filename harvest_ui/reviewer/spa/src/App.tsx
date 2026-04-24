import { useState } from 'react'
import { PackList } from './PackList'

const TABS = ['Pending', 'Deferred', 'Approved', 'Rejected']

export default function App() {
  const [tab, setTab] = useState('Pending')

  return (
    <div style={{ fontFamily: 'system-ui, sans-serif', maxWidth: 860, margin: '0 auto', padding: 24 }}>
      <header style={{ marginBottom: 24 }}>
        <h1 style={{ fontSize: 22, fontWeight: 700, margin: 0 }}>Harvest Reviewer</h1>
        <p style={{ color: '#6b7280', margin: '4px 0 0' }}>Pack review, approval, and chain inspection</p>
      </header>

      <nav style={{ display: 'flex', gap: 4, marginBottom: 20, borderBottom: '1px solid #e5e7eb' }}>
        {TABS.map(t => (
          <button
            key={t}
            onClick={() => setTab(t)}
            style={{
              padding: '8px 16px',
              background: 'none',
              border: 'none',
              borderBottom: tab === t ? '2px solid #2563eb' : '2px solid transparent',
              color: tab === t ? '#2563eb' : '#374151',
              fontWeight: tab === t ? 600 : 400,
              cursor: 'pointer',
              fontSize: 14,
            }}>
            {t}
          </button>
        ))}
      </nav>

      <main>
        <PackList tab={tab} />
      </main>
    </div>
  )
}
