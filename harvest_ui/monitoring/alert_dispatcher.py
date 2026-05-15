"""
AlertDispatcher — persistent webhook/email delivery with dead-letter queue.

Wave 3b: monitoring_and_alerting — persistent delivery for job/system events.

Features:
- AlertRule: condition-based rule that fires on job state changes
- AlertDispatcher: evaluates rules and delivers alerts via webhook or email
- Dead-letter queue: failed deliveries written to disk for inspection/replay
- Retry: up to 3 delivery attempts with 5s backoff

Constitutional guarantees:
- Fail-closed: delivery failure never raises into caller code
- Local-first: dead-letter queue is a JSONL file, no external queue service
- Zero-ambiguity: all alert payloads include job_id, event, timestamp
"""

from __future__ import annotations

import asyncio
import json
import time
from dataclasses import dataclass, field, asdict
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional
from uuid import uuid4


# ---------------------------------------------------------------------------
# AlertRule
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    """
    Condition-based rule that triggers an alert delivery.

    condition_fn: takes (job_dict) → bool. If True, alert fires.
    destination:  webhook URL ("http://...") or email address
    name:         human-readable rule label
    """
    name: str
    destination: str
    condition_fn: Callable[[Dict[str, Any]], bool]
    alert_id: str = field(default_factory=lambda: str(uuid4())[:8])

    def matches(self, job: Dict[str, Any]) -> bool:
        try:
            return self.condition_fn(job)
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Pre-built rule factories
# ---------------------------------------------------------------------------

def rule_on_job_failed(destination: str, kind_filter: Optional[str] = None) -> AlertRule:
    def _cond(job: Dict[str, Any]) -> bool:
        if kind_filter and job.get("kind") != kind_filter:
            return False
        return job.get("status") == "failed"
    return AlertRule(name=f"job_failed{'_' + kind_filter if kind_filter else ''}", destination=destination, condition_fn=_cond)


def rule_on_job_completed(destination: str, kind_filter: Optional[str] = None) -> AlertRule:
    def _cond(job: Dict[str, Any]) -> bool:
        if kind_filter and job.get("kind") != kind_filter:
            return False
        return job.get("status") == "completed"
    return AlertRule(name=f"job_completed{'_' + kind_filter if kind_filter else ''}", destination=destination, condition_fn=_cond)


# ---------------------------------------------------------------------------
# DeadLetterQueue
# ---------------------------------------------------------------------------

class DeadLetterQueue:
    """Persists failed alert deliveries to JSONL for inspection and replay."""

    def __init__(self, storage_root: str = "storage"):
        self._path = Path(storage_root) / "alerts" / "dead_letter.jsonl"
        self._path.parent.mkdir(parents=True, exist_ok=True)

    def append(self, entry: Dict[str, Any]) -> None:
        try:
            with self._path.open("a", encoding="utf-8") as f:
                f.write(json.dumps({**entry, "dlq_at": time.time()}) + "\n")
        except Exception:
            pass

    def read_all(self) -> List[Dict[str, Any]]:
        if not self._path.exists():
            return []
        result = []
        for line in self._path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line:
                try:
                    result.append(json.loads(line))
                except Exception:
                    pass
        return result

    def clear(self) -> None:
        if self._path.exists():
            self._path.write_text("", encoding="utf-8")


# ---------------------------------------------------------------------------
# AlertDispatcher
# ---------------------------------------------------------------------------

class AlertDispatcher:
    """
    Evaluate alert rules against job events and deliver notifications.

    Usage:
        dispatcher = AlertDispatcher(storage_root="storage")
        dispatcher.add_rule(rule_on_job_failed("https://hook.example.com"))
        await dispatcher.evaluate(job_dict)
    """

    MAX_ATTEMPTS = 3
    RETRY_DELAY_S = 5.0

    def __init__(self, storage_root: str = "storage"):
        self._rules: List[AlertRule] = []
        self._dlq = DeadLetterQueue(storage_root)

    def add_rule(self, rule: AlertRule) -> None:
        self._rules.append(rule)

    def remove_rule(self, alert_id: str) -> None:
        self._rules = [r for r in self._rules if r.alert_id != alert_id]

    async def evaluate(self, job: Dict[str, Any]) -> None:
        """Evaluate all rules against *job* and fire matching alerts."""
        for rule in self._rules:
            if rule.matches(job):
                asyncio.create_task(self._deliver(rule, job))

    async def _deliver(self, rule: AlertRule, job: Dict[str, Any]) -> None:
        payload = {
            "alert": rule.name,
            "job_id": job.get("job_id", ""),
            "job_kind": job.get("kind", ""),
            "status": job.get("status", ""),
            "url": job.get("url", ""),
            "error": job.get("error"),
            "timestamp": time.time(),
        }

        dest = rule.destination
        success = False

        for attempt in range(1, self.MAX_ATTEMPTS + 1):
            try:
                if dest.startswith("http://") or dest.startswith("https://"):
                    success = await self._deliver_webhook(dest, payload)
                elif "@" in dest:
                    success = await self._deliver_email(dest, payload)
                if success:
                    return
            except Exception:
                pass
            if attempt < self.MAX_ATTEMPTS:
                await asyncio.sleep(self.RETRY_DELAY_S * attempt)

        # All attempts failed — write to dead-letter queue
        self._dlq.append({"rule": rule.name, "destination": dest, "payload": payload})

    async def _deliver_webhook(self, url: str, payload: Dict[str, Any]) -> bool:
        try:
            import aiohttp
            payload_bytes = json.dumps(payload).encode()
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url,
                    data=payload_bytes,
                    headers={"Content-Type": "application/json", "X-Harvest-Alert": "1"},
                    timeout=aiohttp.ClientTimeout(total=10),
                ) as resp:
                    return resp.status < 400
        except ImportError:
            # aiohttp not installed — try urllib fallback
            import urllib.request
            req = urllib.request.Request(
                url,
                data=json.dumps(payload).encode(),
                headers={"Content-Type": "application/json"},
            )
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status < 400
        except Exception:
            return False

    async def _deliver_email(self, address: str, payload: Dict[str, Any]) -> bool:
        """SMTP delivery — requires SMTP_HOST/SMTP_FROM env vars."""
        import os
        smtp_host = os.environ.get("SMTP_HOST", "")
        smtp_from = os.environ.get("SMTP_FROM", "harvest@localhost")
        if not smtp_host:
            return False
        try:
            import smtplib
            from email.mime.text import MIMEText
            msg = MIMEText(json.dumps(payload, indent=2))
            msg["Subject"] = f"[Harvest Alert] {payload.get('alert')} — {payload.get('status')}"
            msg["From"] = smtp_from
            msg["To"] = address
            smtp_port = int(os.environ.get("SMTP_PORT", "25"))
            with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as s:
                s.sendmail(smtp_from, [address], msg.as_string())
            return True
        except Exception:
            return False

    @property
    def dead_letter_queue(self) -> DeadLetterQueue:
        return self._dlq

    @property
    def rules(self) -> List[AlertRule]:
        return list(self._rules)
