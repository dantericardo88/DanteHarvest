import { useState, useEffect, useCallback } from 'react'
import { fetchPacks } from './api'
import type { PackSummary } from './api'

export function usePacks(status: string | undefined, pollMs = 5000) {
  const [packs, setPacks] = useState<PackSummary[]>([])
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState<string | null>(null)

  const load = useCallback(async () => {
    try {
      const data = await fetchPacks(status)
      setPacks(data)
      setError(null)
    } catch (e) {
      setError(String(e))
    } finally {
      setLoading(false)
    }
  }, [status])

  useEffect(() => {
    load()
    const id = setInterval(load, pollMs)
    return () => clearInterval(id)
  }, [load, pollMs])

  return { packs, loading, error, refresh: load }
}
