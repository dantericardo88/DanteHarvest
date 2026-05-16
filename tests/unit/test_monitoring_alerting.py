"""
Tests for monitoring_and_alerting dimension.

Covers:
- AlertDelivery rate limiting
- AlertDelivery.send_webhook() (mocked urllib)
- AlertDelivery.send_email() (mocked smtplib)
- AlertDelivery.deliver() channel dispatch and stats
- AlertTemplate.render() token replacement
- AlertTemplate.render_webhook() returns dict
- AlertTemplate.render_email_subject() and render_email_body()
"""

from __future__ import annotations

import json
import time
from io import BytesIO
from unittest.mock import MagicMock, patch

import pytest

from harvest_ui.monitoring.alert_delivery import AlertDelivery
from harvest_ui.monitoring.alert_template import AlertTemplate
from harvest_ui.monitoring import AlertDelivery as AlertDeliveryFromInit
from harvest_ui.monitoring import AlertTemplate as AlertTemplateFromInit


# ---------------------------------------------------------------------------
# AlertTemplate
# ---------------------------------------------------------------------------


class TestAlertTemplateRender:
    def test_single_token_replaced(self):
        tmpl = AlertTemplate()
        result = tmpl.render("Hello {{name}}!", {"name": "World"})
        assert result == "Hello World!"

    def test_multiple_tokens_replaced(self):
        tmpl = AlertTemplate()
        result = tmpl.render("{{a}} + {{b}} = {{c}}", {"a": "1", "b": "2", "c": "3"})
        assert result == "1 + 2 = 3"

    def test_missing_key_replaced_with_empty_string(self):
        tmpl = AlertTemplate()
        result = tmpl.render("Value: {{missing}}", {})
        assert result == "Value: "

    def test_non_str_values_converted(self):
        tmpl = AlertTemplate()
        result = tmpl.render("Count: {{count}}", {"count": 42})
        assert result == "Count: 42"

    def test_details_json_auto_generated(self):
        tmpl = AlertTemplate()
        ctx = {"details": {"k": "v"}}
        result = tmpl.render("{{details_json}}", ctx)
        parsed = json.loads(result)
        assert parsed == {"k": "v"}

    def test_details_text_auto_generated(self):
        tmpl = AlertTemplate()
        ctx = {"details": {"foo": "bar", "baz": "qux"}}
        result = tmpl.render("{{details_text}}", ctx)
        assert "foo: bar" in result
        assert "baz: qux" in result

    def test_details_json_empty_when_no_details(self):
        tmpl = AlertTemplate()
        result = tmpl.render("{{details_json}}", {})
        assert json.loads(result) == {}

    def test_explicit_details_json_not_overridden(self):
        tmpl = AlertTemplate()
        ctx = {"details": {"k": "v"}, "details_json": '"override"'}
        result = tmpl.render("{{details_json}}", ctx)
        assert result == '"override"'


class TestAlertTemplateRenderWebhook:
    def test_returns_dict(self):
        tmpl = AlertTemplate()
        ctx = {
            "alert_name": "test_alert",
            "severity": "critical",
            "message": "Something failed",
            "timestamp": "2026-05-15T00:00:00Z",
            "details": {},
        }
        result = tmpl.render_webhook(ctx)
        assert isinstance(result, dict)

    def test_keys_present_in_default_template(self):
        tmpl = AlertTemplate()
        ctx = {
            "alert_name": "my_alert",
            "severity": "warning",
            "message": "Check this",
            "timestamp": "2026-05-15T00:00:00Z",
            "details": {"count": 5},
        }
        result = tmpl.render_webhook(ctx)
        assert result["alert"] == "my_alert"
        assert result["severity"] == "warning"
        assert result["message"] == "Check this"

    def test_custom_template(self):
        tmpl = AlertTemplate()
        custom = '{"x": "{{alert_name}}"}'
        ctx = {"alert_name": "boom"}
        result = tmpl.render_webhook(ctx, template=custom)
        assert result == {"x": "boom"}

    def test_fallback_on_invalid_json_template(self):
        tmpl = AlertTemplate()
        bad_template = "not valid json {{alert_name}}"
        ctx = {"alert_name": "x"}
        result = tmpl.render_webhook(ctx, template=bad_template)
        # Should fall back to context dict
        assert isinstance(result, dict)


class TestAlertTemplateEmail:
    def test_render_email_subject_default(self):
        tmpl = AlertTemplate()
        ctx = {"severity": "CRITICAL", "alert_name": "disk_full"}
        subject = tmpl.render_email_subject(ctx)
        assert "CRITICAL" in subject
        assert "disk_full" in subject

    def test_render_email_subject_custom_template(self):
        tmpl = AlertTemplate()
        ctx = {"severity": "WARN"}
        subject = tmpl.render_email_subject(ctx, template="Alert: {{severity}}")
        assert subject == "Alert: WARN"

    def test_render_email_body_default(self):
        tmpl = AlertTemplate()
        ctx = {
            "alert_name": "cpu_high",
            "severity": "WARNING",
            "timestamp": "2026-05-15T12:00:00Z",
            "message": "CPU usage is high",
            "details": {"cpu_pct": "95"},
        }
        body = tmpl.render_email_body(ctx)
        assert "cpu_high" in body
        assert "WARNING" in body
        assert "CPU usage is high" in body
        assert "cpu_pct: 95" in body

    def test_render_email_body_custom_template(self):
        tmpl = AlertTemplate()
        ctx = {"message": "hello"}
        body = tmpl.render_email_body(ctx, template="MSG: {{message}}")
        assert body == "MSG: hello"


# ---------------------------------------------------------------------------
# AlertDelivery — rate limiting
# ---------------------------------------------------------------------------


class TestAlertDeliveryRateLimit:
    def test_first_call_not_rate_limited(self):
        ad = AlertDelivery(rate_limit_seconds=300)
        assert ad._is_rate_limited("key1") is False

    def test_after_recording_sent_is_rate_limited(self):
        ad = AlertDelivery(rate_limit_seconds=300)
        ad._record_sent("key1")
        assert ad._is_rate_limited("key1") is True

    def test_after_window_expires_not_rate_limited(self):
        ad = AlertDelivery(rate_limit_seconds=1)
        ad._last_sent["key1"] = time.time() - 2  # 2 seconds ago
        assert ad._is_rate_limited("key1") is False

    def test_different_keys_independent(self):
        ad = AlertDelivery(rate_limit_seconds=300)
        ad._record_sent("key1")
        assert ad._is_rate_limited("key1") is True
        assert ad._is_rate_limited("key2") is False

    def test_zero_rate_limit_never_suppresses(self):
        ad = AlertDelivery(rate_limit_seconds=0)
        ad._record_sent("key1")
        # With 0 seconds, even a freshly recorded key should pass
        assert ad._is_rate_limited("key1") is False


# ---------------------------------------------------------------------------
# AlertDelivery — send_webhook
# ---------------------------------------------------------------------------


class TestAlertDeliverySendWebhook:
    def _make_mock_response(self, status: int = 200) -> MagicMock:
        resp = MagicMock()
        resp.status = status
        resp.__enter__ = lambda s: s
        resp.__exit__ = MagicMock(return_value=False)
        return resp

    def test_successful_webhook_returns_true(self):
        ad = AlertDelivery()
        mock_resp = self._make_mock_response(200)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = ad.send_webhook("http://example.com/hook", {"event": "test"})
        assert result is True

    def test_4xx_status_returns_false(self):
        ad = AlertDelivery()
        mock_resp = self._make_mock_response(404)
        with patch("urllib.request.urlopen", return_value=mock_resp):
            result = ad.send_webhook("http://example.com/hook", {"event": "test"})
        assert result is False

    def test_exception_returns_false(self):
        ad = AlertDelivery()
        with patch("urllib.request.urlopen", side_effect=Exception("connection refused")):
            result = ad.send_webhook("http://example.com/hook", {"event": "test"})
        assert result is False

    def test_posts_json_body(self):
        ad = AlertDelivery()
        captured_requests = []

        def capture_urlopen(req, timeout=None):
            captured_requests.append(req)
            resp = self._make_mock_response(200)
            return resp

        with patch("urllib.request.urlopen", side_effect=capture_urlopen):
            ad.send_webhook("http://example.com/hook", {"key": "value"})

        assert len(captured_requests) == 1
        req = captured_requests[0]
        body = json.loads(req.data)
        assert body == {"key": "value"}

    def test_custom_headers_merged(self):
        ad = AlertDelivery()
        captured_requests = []

        def capture_urlopen(req, timeout=None):
            captured_requests.append(req)
            return self._make_mock_response(200)

        with patch("urllib.request.urlopen", side_effect=capture_urlopen):
            ad.send_webhook(
                "http://example.com/hook",
                {"k": "v"},
                headers={"X-Custom": "test"},
            )

        req = captured_requests[0]
        assert req.get_header("X-custom") == "test"

    def test_content_type_header_set(self):
        ad = AlertDelivery()
        captured_requests = []

        def capture_urlopen(req, timeout=None):
            captured_requests.append(req)
            return self._make_mock_response(200)

        with patch("urllib.request.urlopen", side_effect=capture_urlopen):
            ad.send_webhook("http://example.com/hook", {})

        req = captured_requests[0]
        assert req.get_header("Content-type") == "application/json"


# ---------------------------------------------------------------------------
# AlertDelivery — send_email
# ---------------------------------------------------------------------------


class TestAlertDeliverySendEmail:
    def test_successful_email_returns_true(self):
        ad = AlertDelivery()
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP_SSL", return_value=mock_smtp):
            result = ad.send_email(
                smtp_host="smtp.example.com",
                smtp_port=465,
                from_addr="from@example.com",
                to_addrs=["to@example.com"],
                subject="Test Subject",
                body="Test body",
                use_tls=True,
            )
        assert result is True

    def test_smtp_exception_returns_false(self):
        ad = AlertDelivery()
        with patch("smtplib.SMTP_SSL", side_effect=Exception("connection refused")):
            result = ad.send_email(
                smtp_host="bad.host",
                smtp_port=465,
                from_addr="from@example.com",
                to_addrs=["to@example.com"],
                subject="Subject",
                body="Body",
                use_tls=True,
            )
        assert result is False

    def test_no_tls_uses_smtp(self):
        ad = AlertDelivery()
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP", return_value=mock_smtp) as mock_cls:
            result = ad.send_email(
                smtp_host="smtp.example.com",
                smtp_port=25,
                from_addr="from@example.com",
                to_addrs=["to@example.com"],
                subject="Subject",
                body="Body",
                use_tls=False,
            )
        mock_cls.assert_called_once()
        assert result is True

    def test_uses_tls_uses_smtp_ssl(self):
        ad = AlertDelivery()
        mock_smtp = MagicMock()
        mock_smtp.__enter__ = lambda s: s
        mock_smtp.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP_SSL", return_value=mock_smtp) as mock_cls:
            result = ad.send_email(
                smtp_host="smtp.example.com",
                smtp_port=465,
                from_addr="from@example.com",
                to_addrs=["to@example.com"],
                subject="Subject",
                body="Body",
                use_tls=True,
            )
        mock_cls.assert_called_once()
        assert result is True

    def test_sendmail_called_with_correct_recipients(self):
        ad = AlertDelivery()
        mock_smtp_instance = MagicMock()
        mock_smtp_ctx = MagicMock()
        mock_smtp_ctx.__enter__ = MagicMock(return_value=mock_smtp_instance)
        mock_smtp_ctx.__exit__ = MagicMock(return_value=False)

        with patch("smtplib.SMTP_SSL", return_value=mock_smtp_ctx):
            ad.send_email(
                smtp_host="smtp.example.com",
                smtp_port=465,
                from_addr="sender@example.com",
                to_addrs=["a@x.com", "b@x.com"],
                subject="S",
                body="B",
                use_tls=True,
            )

        mock_smtp_instance.sendmail.assert_called_once()
        call_args = mock_smtp_instance.sendmail.call_args
        assert call_args[0][0] == "sender@example.com"
        assert call_args[0][1] == ["a@x.com", "b@x.com"]


# ---------------------------------------------------------------------------
# AlertDelivery — deliver() channel dispatch and stats
# ---------------------------------------------------------------------------


class TestAlertDeliveryDeliver:
    def test_deliver_webhook_success_updates_stats(self):
        ad = AlertDelivery()
        with patch.object(ad, "send_webhook", return_value=True):
            result = ad.deliver(
                alert_key="key1",
                channel="webhook",
                config={"url": "http://example.com/hook"},
                payload={"alert_name": "test", "severity": "info",
                         "message": "msg", "timestamp": "now", "details": {}},
            )
        assert result is True
        stats = ad.get_stats()
        assert stats["sent"] == 1
        assert stats["suppressed"] == 0

    def test_deliver_webhook_failure_does_not_increment_sent(self):
        ad = AlertDelivery()
        with patch.object(ad, "send_webhook", return_value=False):
            result = ad.deliver(
                alert_key="key1",
                channel="webhook",
                config={"url": "http://example.com/hook"},
                payload={"alert_name": "test", "severity": "info",
                         "message": "msg", "timestamp": "now", "details": {}},
            )
        assert result is False
        stats = ad.get_stats()
        assert stats["sent"] == 0

    def test_deliver_email_channel(self):
        ad = AlertDelivery()
        with patch.object(ad, "send_email", return_value=True):
            result = ad.deliver(
                alert_key="key_email",
                channel="email",
                config={
                    "smtp_host": "smtp.example.com",
                    "smtp_port": 465,
                    "from": "from@example.com",
                    "to": ["to@example.com"],
                    "use_tls": True,
                },
                payload={"alert_name": "test", "severity": "critical",
                         "message": "msg", "timestamp": "now", "details": {}},
            )
        assert result is True
        assert ad.get_stats()["sent"] == 1

    def test_deliver_rate_limited_increments_suppressed(self):
        ad = AlertDelivery(rate_limit_seconds=300)
        ad._record_sent("key1")  # Mark as recently sent
        with patch.object(ad, "send_webhook", return_value=True) as mock_send:
            result = ad.deliver(
                alert_key="key1",
                channel="webhook",
                config={"url": "http://example.com/hook"},
                payload={},
            )
        assert result is False
        mock_send.assert_not_called()
        stats = ad.get_stats()
        assert stats["suppressed"] == 1
        assert stats["sent"] == 0

    def test_deliver_unknown_channel_returns_false(self):
        ad = AlertDelivery()
        result = ad.deliver(
            alert_key="key1",
            channel="sms",
            config={},
            payload={},
        )
        assert result is False

    def test_deliver_sends_second_call_after_window(self):
        ad = AlertDelivery(rate_limit_seconds=1)
        ad._last_sent["key1"] = time.time() - 2  # expired window
        with patch.object(ad, "send_webhook", return_value=True):
            result = ad.deliver(
                alert_key="key1",
                channel="webhook",
                config={"url": "http://example.com/hook"},
                payload={"alert_name": "test", "severity": "info",
                         "message": "msg", "timestamp": "now", "details": {}},
            )
        assert result is True

    def test_deliver_uses_template_rendering(self):
        ad = AlertDelivery()
        captured_payloads = []

        def capture_send(url, payload, headers=None):
            captured_payloads.append(payload)
            return True

        with patch.object(ad, "send_webhook", side_effect=capture_send):
            ad.deliver(
                alert_key="key1",
                channel="webhook",
                config={"url": "http://example.com/hook"},
                payload={
                    "alert_name": "cpu_alert",
                    "severity": "critical",
                    "message": "CPU too high",
                    "timestamp": "2026-05-15T00:00:00Z",
                    "details": {},
                },
            )

        assert len(captured_payloads) == 1
        # The rendered payload should be a dict from template rendering
        assert isinstance(captured_payloads[0], dict)
        assert captured_payloads[0].get("alert") == "cpu_alert"


# ---------------------------------------------------------------------------
# AlertDelivery — get_stats
# ---------------------------------------------------------------------------


class TestAlertDeliveryStats:
    def test_initial_stats(self):
        ad = AlertDelivery(rate_limit_seconds=120)
        stats = ad.get_stats()
        assert stats == {"sent": 0, "suppressed": 0, "rate_limit_seconds": 120}

    def test_stats_after_multiple_delivers(self):
        ad = AlertDelivery(rate_limit_seconds=300)
        # First send succeeds
        with patch.object(ad, "send_webhook", return_value=True):
            ad.deliver("k1", "webhook", {"url": "http://x.com"}, {
                "alert_name": "a", "severity": "info",
                "message": "m", "timestamp": "t", "details": {}
            })
        # Second send for same key is suppressed
        with patch.object(ad, "send_webhook", return_value=True):
            ad.deliver("k1", "webhook", {"url": "http://x.com"}, {})
        # Third send for different key succeeds
        with patch.object(ad, "send_webhook", return_value=True):
            ad.deliver("k2", "webhook", {"url": "http://x.com"}, {
                "alert_name": "b", "severity": "info",
                "message": "m", "timestamp": "t", "details": {}
            })

        stats = ad.get_stats()
        assert stats["sent"] == 2
        assert stats["suppressed"] == 1


# ---------------------------------------------------------------------------
# __init__.py exports
# ---------------------------------------------------------------------------


class TestMonitoringInit:
    def test_alert_delivery_importable(self):
        assert AlertDeliveryFromInit is AlertDelivery

    def test_alert_template_importable(self):
        assert AlertTemplateFromInit is AlertTemplate
