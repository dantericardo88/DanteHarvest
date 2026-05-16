"""
harvest_ui.monitoring — alert dispatch, delivery, and templating.

Public API:
    AlertRule            — condition-based rule that triggers on job state
    AlertDispatcher      — evaluates rules and delivers via webhook/email
    DeadLetterQueue      — persists failed deliveries to JSONL
    rule_on_job_failed   — pre-built rule factory
    rule_on_job_completed — pre-built rule factory

    AlertDelivery        — synchronous webhook/email delivery with rate-limiting
    AlertTemplate        — {{key}} token renderer for alert messages
"""

from harvest_ui.monitoring.alert_dispatcher import (
    AlertRule,
    AlertDispatcher,
    DeadLetterQueue,
    rule_on_job_failed,
    rule_on_job_completed,
)
from harvest_ui.monitoring.alert_delivery import AlertDelivery
from harvest_ui.monitoring.alert_template import AlertTemplate

__all__ = [
    "AlertRule",
    "AlertDispatcher",
    "DeadLetterQueue",
    "rule_on_job_failed",
    "rule_on_job_completed",
    "AlertDelivery",
    "AlertTemplate",
]
