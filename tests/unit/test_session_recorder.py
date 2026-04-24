"""Unit tests for BrowserSessionRecorder, AudioRecorder, ScreenRecorder."""

import pytest
import json
from pathlib import Path

from harvest_observe.browser_session.session_recorder import BrowserSessionRecorder
from harvest_observe.audio.audio_recorder import AudioRecorder
from harvest_observe.screen.screen_recorder import ScreenRecorder
from harvest_core.provenance.chain_writer import ChainWriter


def _make_writer(tmp_path, run_id="run-001") -> ChainWriter:
    return ChainWriter(tmp_path / "chain.jsonl", run_id)


class TestBrowserSessionRecorder:
    @pytest.mark.asyncio
    async def test_start_and_end_session(self, tmp_path):
        writer = _make_writer(tmp_path)
        recorder = BrowserSessionRecorder(writer, storage_root=str(tmp_path / "store"))

        session = await recorder.start_session(url="https://example.com", run_id="run-001")
        assert session.session_id
        assert session.start_url == "https://example.com"

        await recorder.end_session(session)
        assert session.end_time is not None

    @pytest.mark.asyncio
    async def test_record_action_emits_chain_signal(self, tmp_path):
        writer = _make_writer(tmp_path)
        recorder = BrowserSessionRecorder(writer, storage_root=str(tmp_path / "store"))

        session = await recorder.start_session(url="https://example.com", run_id="run-001")
        await recorder.record_action(session, action_type="click", target_selector="#btn")
        await recorder.end_session(session)

        entries = writer.read_all()
        signals = [e.signal for e in entries]
        assert "session.started" in signals
        assert "session.action_recorded" in signals
        assert "session.completed" in signals

    @pytest.mark.asyncio
    async def test_action_stored_in_session(self, tmp_path):
        writer = _make_writer(tmp_path)
        recorder = BrowserSessionRecorder(writer, storage_root=str(tmp_path / "store"))
        session = await recorder.start_session(url="https://example.com", run_id="run-001")
        await recorder.record_action(session, action_type="type", value="hello")
        assert len(session.actions) == 1
        assert session.actions[0].action_type == "type"

    @pytest.mark.asyncio
    async def test_end_session_writes_manifest(self, tmp_path):
        writer = _make_writer(tmp_path)
        recorder = BrowserSessionRecorder(writer, storage_root=str(tmp_path / "store"))
        session = await recorder.start_session(url="https://test.com", run_id="run-001")
        await recorder.end_session(session)
        manifest = Path(session.storage_dir) / "session.json"
        assert manifest.exists()
        data = json.loads(manifest.read_text())
        assert data["session_id"] == session.session_id

    @pytest.mark.asyncio
    async def test_ingest_trace_file(self, tmp_path):
        writer = _make_writer(tmp_path)
        recorder = BrowserSessionRecorder(writer, storage_root=str(tmp_path / "store"))
        trace = {
            "start_url": "https://demo.com",
            "actions": [
                {"type": "click", "selector": "#ok"},
                {"type": "navigate", "selector": None, "value": "https://demo.com/next"},
            ],
        }
        trace_path = tmp_path / "trace.json"
        trace_path.write_text(json.dumps(trace))
        session = await recorder.ingest_trace_file(trace_path, run_id="run-001")
        assert len(session.actions) == 2


class TestAudioRecorder:
    @pytest.mark.asyncio
    async def test_ingest_wav_file(self, tmp_path):
        # Create a minimal valid WAV (44-byte header only — size est from bytes)
        import struct, wave, io
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(1)
            wf.setsampwidth(2)
            wf.setframerate(16000)
            wf.writeframes(b"\x00\x00" * 16000)  # 1 second of silence

        wav_path = tmp_path / "test.wav"
        wav_path.write_bytes(buf.getvalue())

        writer = _make_writer(tmp_path)
        recorder = AudioRecorder(writer, storage_root=str(tmp_path / "store"))
        session = await recorder.ingest_file(wav_path, run_id="run-001")

        assert session.session_id
        assert session.chunk_count == 1
        assert session.total_duration > 0

    @pytest.mark.asyncio
    async def test_ingest_emits_chain_signals(self, tmp_path):
        import struct, wave, io
        buf = io.BytesIO()
        with wave.open(buf, "wb") as wf:
            wf.setnchannels(1); wf.setsampwidth(2); wf.setframerate(16000)
            wf.writeframes(b"\x00\x00" * 800)
        wav_path = tmp_path / "rec.wav"
        wav_path.write_bytes(buf.getvalue())

        writer = _make_writer(tmp_path)
        recorder = AudioRecorder(writer, storage_root=str(tmp_path / "store"))
        await recorder.ingest_file(wav_path, run_id="run-001")

        signals = [e.signal for e in writer.read_all()]
        assert "audio.started" in signals
        assert "audio.chunk_written" in signals
        assert "audio.completed" in signals

    @pytest.mark.asyncio
    async def test_missing_file_raises_and_emits_failed(self, tmp_path):
        from harvest_observe.audio.audio_recorder import AudioObservationError
        writer = _make_writer(tmp_path)
        recorder = AudioRecorder(writer, storage_root=str(tmp_path / "store"))
        with pytest.raises(AudioObservationError):
            await recorder.ingest_file(tmp_path / "missing.wav", run_id="run-001")
        signals = [e.signal for e in writer.read_all()]
        assert "audio.failed" in signals


class TestScreenRecorder:
    @pytest.mark.asyncio
    async def test_ingest_frame_directory(self, tmp_path):
        # Create 3 tiny PNG frames
        frames_dir = tmp_path / "frames"
        frames_dir.mkdir()
        for i in range(3):
            # Minimal 1x1 PNG
            import base64
            png_b64 = (
                "iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNk"
                "YPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
            )
            (frames_dir / f"frame_{i:03d}.png").write_bytes(base64.b64decode(png_b64))

        writer = _make_writer(tmp_path)
        recorder = ScreenRecorder(writer, storage_root=str(tmp_path / "store"), fps=1.0)
        session = await recorder.ingest_frame_directory(frames_dir, run_id="run-001")

        assert session.frame_count == 3
        signals = [e.signal for e in writer.read_all()]
        assert "screen.started" in signals
        assert "screen.completed" in signals
        assert signals.count("screen.frame_captured") == 3

    @pytest.mark.asyncio
    async def test_missing_directory_raises(self, tmp_path):
        from harvest_observe.screen.screen_recorder import ScreenObservationError
        writer = _make_writer(tmp_path)
        recorder = ScreenRecorder(writer, storage_root=str(tmp_path / "store"))
        with pytest.raises(ScreenObservationError):
            await recorder.ingest_frame_directory(tmp_path / "nonexistent", run_id="run-001")
