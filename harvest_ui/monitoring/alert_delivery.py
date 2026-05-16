"""
AlertDelivery — delivers alert notifications via webhook or email.

Uses stdlib only (urllib.request, smtplib). No external dependencies.

Features:
- Rate-limiting: suppresses repeated identical alerts within a configurable window
- Message templating: integrates AlertTemplate for payload rendering
- Fail-closed: delivery errors are logged, never raised to caller
- Stats: tracks sent vs suppressed counts

Constitutional guarantees:
- Stdlib-only: zero external dependencies
- Fail-closed: never raises into caller
- Rate-limited: identical alert keys are suppressed within rate_limit_seconds
"""

from __future__ import annotations

import json
import logging
import smtplib
import time
import urllib.request
from email.mime.text import MIMEText
from typing import Any, Dict, List, Optional

from harvest_ui.monitoring.alert_template import AlertTemplate

logger = logging.getLogger(__name__)


class AlertDelivery:
    """Delivers alert notifications via webhook or email using stdlib only."""

    def __init__(self, rate_limit_seconds: int = 300) -> None:
        # rate_limit_seconds: minimum seconds between identical alerts
        self._last_sent: Dict[str, float] = {}  # key -> timestamp
        self.rate_limit_seconds = rate_limit_seconds
        self._sent_count: int = 0
        self._suppressed_count: int = 0
        self._template = AlertTemplate()

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------

    def _is_rate_limited(self, alert_key: str) -> bool:
        """Return True if this alert was sent within rate_limit_seconds."""
        last = self._last_sent.get(alert_key)
        if last is None:
            return False
        return (time.time() - last) < self.rate_limit_seconds

    def _record_sent(self, alert_key: str) -> None:
        self._last_sent[alert_key] = time.time()

    # ------------------------------------------------------------------
    # Delivery methods
    # ------------------------------------------------------------------

    def send_webhook(
        self,
        url: str,
        payload: dict,
        headers: Optional[Dict[str, str]] = None,
    ) -> bool:
        """POST JSON payload to url using urllib.request.

        Returns True on success (2xx status), False on failure.
        Logs errors; never raises.
        """
        try:
            body = json.dumps(payload).encode("utf-8")
            req_headers = {
                "Content-Type": "application/json",
                "X-Harvest-Alert": "1",
            }
            if headers:
                req_headers.update(headers)

            request = urllib.request.Request(
                url,
                data=body,
                headers=req_headers,
                method="POST",
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.status < 400
        except Exception as exc:
            logger.error("Webhook delivery failed to %s: %s", url, exc)
            return False

    def send_email(
        self,
        smtp_host: str,
        smtp_port: int,
        from_addr: str,
        to_addrs: List[str],
        subject: str,
        body: str,
        use_tls: bool = True,
    ) -> bool:
        """Send email via smtplib.SMTP / SMTP_SSL.

        Returns True on success, False on failure.
        Logs errors; never raises.
        """
        try:
            msg = MIMEText(body, "plain", "utf-8")
            msg["Subject"] = subject
            msg["From"] = from_addr
            msg["To"] = ", ".join(to_addrs)

            if use_tls:
                with smtplib.SMTP_SSL(smtp_host, smtp_port, timeout=10) as smtp:
                    smtp.sendmail(from_addr, to_addrs, msg.as_string())
            else:
                with smtplib.SMTP(smtp_host, smtp_port, timeout=10) as smtp:
                    smtp.sendmail(from_addr, to_addrs, msg.as_string())

            return True
        except Exception as exc:
            logger.error(
                "Email delivery failed to %s via %s:%s: %s",
                to_addrs,
                smtp_host,
                smtp_port,
                exc,
            )
            return False

    # ------------------------------------------------------------------
    # Unified delivery entry point
    # ------------------------------------------------------------------

    def deliver(
        self,
        alert_key: str,
        channel: str,
        config: dict,
        payload: dict,
    ) -> bool:
        """Deliver an alert via the specified channel.

        Args:
            alert_key: Unique identifier for rate-limit tracking (e.g. "job_failed:job_123").
            channel:   "webhook" or "email".
            config:    Channel-specific configuration dict.
                       webhook: {"url": str, "headers": dict (optional), "template": str (optional)}
                       email:   {"smtp_host": str, "smtp_port": int, "from": str,
                                 "to": list[str], "use_tls": bool (optional),
                                 "subject_template": str (optional),
                                 "body_template": str (optional)}
            payload:   Dict that forms the alert context for template rendering.

        Returns:
            True if alert was sent, False if suppressed or delivery failed.
        """
        # Check rate limit first
        if self._is_rate_limited(alert_key):
            self._suppressed_count += 1
            logger.debug("Alert %s suppressed by rate limiter", alert_key)
            return False

        success = False

        if channel == "webhook":
            url = config.get("url", "")
            headers = config.get("headers")
            tmpl = config.get("template")
            # Render payload via template
            rendered_payload = self._template.render_webhook(payload, template=tmpl)
            success = self.send_webhook(url, rendered_payload, headers=headers)

        elif channel == "email":
            smtp_host = config.get("smtp_host", "")
            smtp_port = int(config.get("smtp_port", 465))
            from_addr = config.get("from", "harvest@localhost")
            to_addrs = config.get("to", [])
            use_tls = bool(config.get("use_tls", True))
            subject_tmpl = config.get("subject_template")
            body_tmpl = config.get("body_template")
            subject = self._template.render_email_subject(payload, template=subject_tmpl)
            body = self._template.render_email_body(payload, template=body_tmpl)
            success = self.send_email(
                smtp_host=smtp_host,
                smtp_port=smtp_port,
                from_addr=from_addr,
                to_addrs=to_addrs,
                subject=subject,
                body=body,
                use_tls=use_tls,
            )
        else:
            logger.warning("Unknown delivery channel: %s", channel)
            return False

        if success:
            self._record_sent(alert_key)
            self._sent_count += 1
        return success

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    def get_stats(self) -> dict:
        """Return delivery statistics."""
        return {
            "sent": self._sent_count,
            "suppressed": self._suppressed_count,
            "rate_limit_seconds": self.rate_limit_seconds,
        }
