"""
Unit tests for observation_plane_depth improvements:
- SessionRecorder: default capture_interval, set_capture_interval, get_ocr_status,
  get_observation_summary
- NetworkCapture: record_request, get_summary, enable/disable
"""

from __future__ import annotations

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

def _make_recorder(tmp_path, **kwargs):
    from harvest_observe.capture.session_recorder import SessionRecorder
    return SessionRecorder(storage_root=str(tmp_path / "sessions"), **kwargs)


# ---------------------------------------------------------------------------
# Default capture interval
# ---------------------------------------------------------------------------

class TestCaptureIntervalDefault:
    def test_default_is_one_second_or_less(self, tmp_path):
        rec = _make_recorder(tmp_path)
        assert rec._capture_interval <= 1.0

    def test_default_is_exactly_one_second(self, tmp_path):
        rec = _make_recorder(tmp_path)
        assert rec._capture_interval == 1.0

    def test_custom_interval_preserved(self, tmp_path):
        rec = _make_recorder(tmp_path, capture_interval=2.5)
        assert rec._capture_interval == 2.5


# ---------------------------------------------------------------------------
# set_capture_interval
# ---------------------------------------------------------------------------

class TestSetCaptureInterval:
    def test_set_valid_interval(self, tmp_path):
        rec = _make_recorder(tmp_path)
        rec.set_capture_interval(0.5)
        assert rec._capture_interval == 0.5

    def test_set_minimum_valid(self, tmp_path):
        rec = _make_recorder(tmp_path)
        rec.set_capture_interval(0.1)
        assert rec._capture_interval == 0.1

    def test_raises_on_below_minimum(self, tmp_path):
        rec = _make_recorder(tmp_path)
        with pytest.raises(ValueError, match="0.1"):
            rec.set_capture_interval(0.05)

    def test_raises_on_zero(self, tmp_path):
        rec = _make_recorder(tmp_path)
        with pytest.raises(ValueError):
            rec.set_capture_interval(0.0)

    def test_raises_on_negative(self, tmp_path):
        rec = _make_recorder(tmp_path)
        with pytest.raises(ValueError):
            rec.set_capture_interval(-1.0)

    def test_set_large_interval(self, tmp_path):
        rec = _make_recorder(tmp_path)
        rec.set_capture_interval(60.0)
        assert rec._capture_interval == 60.0


# ---------------------------------------------------------------------------
# OCR default and get_ocr_status
# ---------------------------------------------------------------------------

class TestOcrStatus:
    def test_ocr_enabled_by_default(self, tmp_path):
        rec = _make_recorder(tmp_path)
        assert rec._ocr_enabled is True

    def test_ocr_disabled_via_constructor(self, tmp_path):
        rec = _make_recorder(tmp_path, ocr_enabled=False)
        assert rec._ocr_enabled is False

    def test_get_ocr_status_returns_dict(self, tmp_path):
        rec = _make_recorder(tmp_path)
        status = rec.get_ocr_status()
        assert isinstance(status, dict)

    def test_get_ocr_status_has_required_keys(self, tmp_path):
        rec = _make_recorder(tmp_path)
        status = rec.get_ocr_status()
        assert "enabled" in status
        assert "available" in status
        assert "backend" in status

    def test_get_ocr_status_enabled_reflects_setting(self, tmp_path):
        rec_on = _make_recorder(tmp_path, ocr_enabled=True)
        assert rec_on.get_ocr_status()["enabled"] is True

        rec_off = _make_recorder(tmp_path, ocr_enabled=False)
        assert rec_off.get_ocr_status()["enabled"] is False

    def test_get_ocr_status_backend_is_string(self, tmp_path):
        rec = _make_recorder(tmp_path)
        status = rec.get_ocr_status()
        assert isinstance(status["backend"], str)


# ---------------------------------------------------------------------------
# NetworkCapture
# ---------------------------------------------------------------------------

class TestNetworkCapture:
    def _make_nc(self):
        from harvest_observe.capture.session_recorder import NetworkCapture
        return NetworkCapture()

    def test_initially_disabled(self):
        nc = self._make_nc()
        assert nc.is_enabled is False

    def test_enable(self):
        nc = self._make_nc()
        nc.enable()
        assert nc.is_enabled is True

    def test_disable(self):
        nc = self._make_nc()
        nc.enable()
        nc.disable()
        assert nc.is_enabled is False

    def test_record_request_only_when_enabled(self):
        nc = self._make_nc()
        nc.record_request("https://example.com", "GET", 200)
        assert nc.get_requests() == []

    def test_record_request_stores_when_enabled(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://example.com", "GET", 200, size_bytes=1024, duration_ms=50.0)
        reqs = nc.get_requests()
        assert len(reqs) == 1
        req = reqs[0]
        assert req["url"] == "https://example.com"
        assert req["method"] == "GET"
        assert req["status"] == 200
        assert req["size_bytes"] == 1024
        assert req["duration_ms"] == 50.0
        assert "ts" in req

    def test_record_multiple_requests(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://a.com", "GET", 200)
        nc.record_request("https://b.com", "POST", 404)
        nc.record_request("https://c.com", "GET", 500)
        assert len(nc.get_requests()) == 3

    def test_get_requests_returns_copy(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://x.com", "GET", 200)
        r1 = nc.get_requests()
        r1.clear()
        assert len(nc.get_requests()) == 1

    def test_clear(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://x.com", "GET", 200)
        nc.clear()
        assert nc.get_requests() == []


# ---------------------------------------------------------------------------
# NetworkCapture.get_summary
# ---------------------------------------------------------------------------

class TestNetworkCaptureSummary:
    def _make_nc(self):
        from harvest_observe.capture.session_recorder import NetworkCapture
        return NetworkCapture()

    def test_empty_summary(self):
        nc = self._make_nc()
        s = nc.get_summary()
        assert s["total_requests"] == 0
        assert s["total_bytes"] == 0
        assert s["error_count"] == 0

    def test_summary_no_errors(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://a.com", "GET", 200, size_bytes=100)
        nc.record_request("https://b.com", "GET", 301, size_bytes=50)
        s = nc.get_summary()
        assert s["total_requests"] == 2
        assert s["total_bytes"] == 150
        assert s["error_count"] == 0
        assert s["error_rate"] == 0.0

    def test_summary_with_errors(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://a.com", "GET", 200, size_bytes=200)
        nc.record_request("https://b.com", "GET", 404, size_bytes=50)
        nc.record_request("https://c.com", "GET", 500, size_bytes=10)
        s = nc.get_summary()
        assert s["total_requests"] == 3
        assert s["error_count"] == 2
        assert abs(s["error_rate"] - 2 / 3) < 1e-9

    def test_summary_all_errors(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://x.com", "GET", 400)
        nc.record_request("https://y.com", "GET", 503)
        s = nc.get_summary()
        assert s["error_rate"] == 1.0

    def test_400_is_error_boundary(self):
        nc = self._make_nc()
        nc.enable()
        nc.record_request("https://x.com", "GET", 399)
        nc.record_request("https://y.com", "GET", 400)
        s = nc.get_summary()
        assert s["error_count"] == 1


# ---------------------------------------------------------------------------
# get_observation_summary
# ---------------------------------------------------------------------------

class TestGetObservationSummary:
    def test_returns_dict(self, tmp_path):
        rec = _make_recorder(tmp_path)
        s = rec.get_observation_summary()
        assert isinstance(s, dict)

    def test_required_keys_present(self, tmp_path):
        rec = _make_recorder(tmp_path)
        s = rec.get_observation_summary()
        required = {
            "capture_interval_seconds",
            "ocr_enabled",
            "network_capture_enabled",
            "keyframes_captured",
            "network_requests_captured",
        }
        assert required.issubset(s.keys())

    def test_capture_interval_seconds_matches(self, tmp_path):
        rec = _make_recorder(tmp_path, capture_interval=1.5)
        s = rec.get_observation_summary()
        assert s["capture_interval_seconds"] == 1.5

    def test_ocr_enabled_reflects_setting(self, tmp_path):
        rec = _make_recorder(tmp_path, ocr_enabled=False)
        s = rec.get_observation_summary()
        assert s["ocr_enabled"] is False

    def test_network_capture_disabled_by_default(self, tmp_path):
        rec = _make_recorder(tmp_path)
        s = rec.get_observation_summary()
        assert s["network_capture_enabled"] is False

    def test_network_capture_enabled_after_enable(self, tmp_path):
        rec = _make_recorder(tmp_path)
        rec.network_capture.enable()
        s = rec.get_observation_summary()
        assert s["network_capture_enabled"] is True

    def test_keyframes_captured_initially_zero(self, tmp_path):
        rec = _make_recorder(tmp_path)
        s = rec.get_observation_summary()
        assert s["keyframes_captured"] == 0

    def test_network_requests_captured_counts_correctly(self, tmp_path):
        rec = _make_recorder(tmp_path)
        rec.network_capture.enable()
        rec.network_capture.record_request("https://a.com", "GET", 200)
        rec.network_capture.record_request("https://b.com", "GET", 200)
        s = rec.get_observation_summary()
        assert s["network_requests_captured"] == 2

    def test_network_capture_attached_to_recorder(self, tmp_path):
        rec = _make_recorder(tmp_path)
        assert hasattr(rec, "network_capture")
        from harvest_observe.capture.session_recorder import NetworkCapture
        assert isinstance(rec.network_capture, NetworkCapture)
