"""
AlertTemplate — simple string template renderer for alert messages.

Replaces {{key}} tokens with context values. No external dependencies.

Constitutional guarantees:
- Zero external dependencies (stdlib only)
- Never raises on missing keys (replaces with empty string)
- details_json / details_text auto-generated from context['details']
"""

from __future__ import annotations

import json
import re
from typing import Any, Dict, Optional


class AlertTemplate:
    """Simple string template renderer for alert messages."""

    DEFAULT_WEBHOOK_TEMPLATE = """{
  "alert": "{{alert_name}}",
  "severity": "{{severity}}",
  "message": "{{message}}",
  "timestamp": "{{timestamp}}",
  "details": {{details_json}}
}"""

    DEFAULT_EMAIL_SUBJECT = "[DanteHarvest Alert] {{severity}}: {{alert_name}}"

    DEFAULT_EMAIL_BODY = """Alert: {{alert_name}}
Severity: {{severity}}
Time: {{timestamp}}

{{message}}

Details:
{{details_text}}
"""

    _TOKEN_RE = re.compile(r"\{\{(\w+)\}\}")

    def render(self, template: str, context: Dict[str, Any]) -> str:
        """Replace {{key}} with context[key].

        Special keys auto-computed if not in context:
        - details_json: JSON string of context.get('details', {})
        - details_text: newline-separated "key: value" pairs from details
        """
        # Build augmented context with auto-computed specials
        details = context.get("details", {})
        augmented = dict(context)
        if "details_json" not in augmented:
            try:
                augmented["details_json"] = json.dumps(details, indent=2)
            except Exception:
                augmented["details_json"] = "{}"
        if "details_text" not in augmented:
            if isinstance(details, dict):
                augmented["details_text"] = "\n".join(
                    f"{k}: {v}" for k, v in details.items()
                )
            else:
                try:
                    augmented["details_text"] = str(details)
                except Exception:
                    augmented["details_text"] = ""

        def _replace(m: re.Match) -> str:
            key = m.group(1)
            val = augmented.get(key, "")
            return str(val)

        return self._TOKEN_RE.sub(_replace, template)

    def render_webhook(
        self, context: Dict[str, Any], template: Optional[str] = None
    ) -> dict:
        """Render webhook template and return parsed dict."""
        tmpl = template if template is not None else self.DEFAULT_WEBHOOK_TEMPLATE
        rendered = self.render(tmpl, context)
        try:
            return json.loads(rendered)
        except json.JSONDecodeError:
            # Fallback: return context as-is if template produces invalid JSON
            return dict(context)

    def render_email_subject(
        self, context: Dict[str, Any], template: Optional[str] = None
    ) -> str:
        """Render email subject template."""
        tmpl = template if template is not None else self.DEFAULT_EMAIL_SUBJECT
        return self.render(tmpl, context)

    def render_email_body(
        self, context: Dict[str, Any], template: Optional[str] = None
    ) -> str:
        """Render email body template."""
        tmpl = template if template is not None else self.DEFAULT_EMAIL_BODY
        return self.render(tmpl, context)
