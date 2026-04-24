"""
Harvest Reviewer Server — FastAPI web server for the pack reviewer workflow.

Sprint 1 target: close ui_reviewer_workflow gap (DH: 3 → 8 vs Rewind.ai: 9).

Provides:
  GET  /api/packs                  — list all packs by status
  GET  /api/packs/{pack_id}        — pack detail + steps + confidence band
  POST /api/packs/{pack_id}/approve — issue receipt and promote
  POST /api/packs/{pack_id}/reject  — reject with reason (chain entry)
  GET  /api/runs/{run_id}/chain    — chain entries for a run
  GET  /api/runs/{run_id}/receipt  — receipt for a run (if issued)
  GET  /health                     — health check

Constitutional guarantees:
- Fail-closed: every HTTP error returns typed JSON with error field (not HTML)
- Local-first: server reads from local filesystem registry and chain files
- Append-only chain: approve/reject both emit chain entries before responding
- Zero-ambiguity: all endpoints return typed response schemas
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, List, Optional

try:
    from fastapi import FastAPI, HTTPException
    from fastapi.middleware.cors import CORSMiddleware
    from fastapi.responses import JSONResponse
    from fastapi.staticfiles import StaticFiles
    from pydantic import BaseModel as _BaseModel
    _FASTAPI_AVAILABLE = True
except ImportError:
    _BaseModel = object  # type: ignore[assignment,misc]
    _FASTAPI_AVAILABLE = False


def _require_fastapi():
    if not _FASTAPI_AVAILABLE:
        raise ImportError(
            "fastapi not installed. Run: pip install fastapi uvicorn"
        )


# ---------------------------------------------------------------------------
# Request / Response schemas
# ---------------------------------------------------------------------------

class ApproveRequest(_BaseModel):
    run_id: str
    receipt_id: Optional[str] = None
    notes: Optional[str] = None


class RejectRequest(_BaseModel):
    run_id: str
    reason: str


class PackSummary(_BaseModel):
    pack_id: str
    pack_type: str
    title: str
    promotion_status: str
    confidence_score: float
    confidence_band: str
    step_count: int


class ChainEntryView(_BaseModel):
    sequence: int
    signal: str
    machine: str
    timestamp: float
    data: Dict[str, Any]


# ---------------------------------------------------------------------------
# App factory
# ---------------------------------------------------------------------------

def _to_pack_status(raw: str):
    """Map registry promotion_status strings to PackStatus enum values."""
    from harvest_ui.reviewer.review_states import PackStatus
    _MAP = {"candidate": PackStatus.PENDING, "promoted": PackStatus.APPROVED}
    try:
        return PackStatus(raw)
    except ValueError:
        if raw in _MAP:
            return _MAP[raw]
        raise


def create_app(
    registry_root: str = "registry",
    storage_root: str = "storage",
) -> Any:
    """
    Create the FastAPI reviewer app.
    Raises ImportError if fastapi is not installed (fail-closed).
    """
    _require_fastapi()

    app = FastAPI(
        title="Harvest Reviewer",
        description="Pack review, approval, and chain inspection UI",
        version="0.1.0",
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_methods=["*"],
        allow_headers=["*"],
    )

    _registry_root = Path(registry_root)
    _storage_root = Path(storage_root)

    # ------------------------------------------------------------------
    # Health
    # ------------------------------------------------------------------

    @app.get("/health")
    def health():
        return {"status": "ok", "registry": str(_registry_root)}

    # ------------------------------------------------------------------
    # Pack endpoints
    # ------------------------------------------------------------------

    @app.get("/api/packs", response_model=List[PackSummary])
    def list_packs(status: Optional[str] = None, pack_type: Optional[str] = None):
        try:
            from harvest_index.registry.pack_registry import PackRegistry
            registry = PackRegistry(root=str(_registry_root))
            entries = registry.list(pack_type=pack_type, status=status)
            return [
                PackSummary(
                    pack_id=e.pack_id,
                    pack_type=e.pack_type,
                    title=e.title,
                    promotion_status=e.promotion_status,
                    confidence_score=getattr(e, "confidence_score", 0.0),
                    confidence_band=_confidence_band(getattr(e, "confidence_score", 0.0)),
                    step_count=getattr(e, "step_count", 0),
                )
                for e in entries
            ]
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    @app.get("/api/packs/{pack_id}")
    def get_pack(pack_id: str):
        try:
            from harvest_index.registry.pack_registry import PackRegistry
            registry = PackRegistry(root=str(_registry_root))
            pack_data = registry.load_pack_json(pack_id)
            return JSONResponse(content=pack_data)
        except Exception as e:
            raise HTTPException(status_code=404, detail={"error": str(e)})

    @app.post("/api/packs/{pack_id}/approve")
    async def approve_pack(pack_id: str, req: ApproveRequest):
        try:
            from harvest_index.registry.pack_registry import PackRegistry
            from harvest_core.provenance.chain_writer import ChainWriter
            from harvest_ui.reviewer.review_states import PackStatus, transition, InvalidTransitionError

            registry = PackRegistry(root=str(_registry_root))
            chain_path = _storage_root / "chain" / f"{req.run_id}.jsonl"
            chain_path.parent.mkdir(parents=True, exist_ok=True)
            writer = ChainWriter(chain_path, req.run_id)

            # Attach receipt before transition if provided
            if req.receipt_id:
                registry.attach_receipt(pack_id, req.receipt_id)

            current_entry = registry.get(pack_id)
            current_status = _to_pack_status(current_entry.promotion_status)
            transition(
                pack_id, current_status, PackStatus.APPROVED,
                registry=registry, chain_writer=writer,
                reviewer=getattr(req, "reviewer", None),
            )
            final_entry = registry.get(pack_id)
            return {"ok": True, "pack_id": pack_id, "status": final_entry.promotion_status}
        except InvalidTransitionError as e:
            raise HTTPException(status_code=409, detail={"error": str(e)})
        except Exception as e:
            raise HTTPException(status_code=400, detail={"error": str(e)})

    @app.post("/api/packs/{pack_id}/reject")
    async def reject_pack(pack_id: str, req: RejectRequest):
        try:
            from harvest_index.registry.pack_registry import PackRegistry
            from harvest_core.provenance.chain_writer import ChainWriter
            from harvest_ui.reviewer.review_states import PackStatus, transition, InvalidTransitionError

            registry = PackRegistry(root=str(_registry_root))
            chain_path = _storage_root / "chain" / f"{req.run_id}.jsonl"
            chain_path.parent.mkdir(parents=True, exist_ok=True)
            writer = ChainWriter(chain_path, req.run_id)

            current_entry = registry.get(pack_id)
            current_status = _to_pack_status(current_entry.promotion_status)
            transition(
                pack_id, current_status, PackStatus.REJECTED,
                registry=registry, chain_writer=writer,
                reason=req.reason,
            )
            final_entry = registry.get(pack_id)
            return {"ok": True, "pack_id": pack_id, "reason": req.reason, "status": final_entry.promotion_status}
        except InvalidTransitionError as e:
            raise HTTPException(status_code=409, detail={"error": str(e)})
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    @app.post("/api/packs/{pack_id}/defer")
    async def defer_pack(pack_id: str, req: RejectRequest):
        try:
            from harvest_index.registry.pack_registry import PackRegistry
            from harvest_core.provenance.chain_writer import ChainWriter
            from harvest_ui.reviewer.review_states import PackStatus, transition, InvalidTransitionError

            registry = PackRegistry(root=str(_registry_root))
            chain_path = _storage_root / "chain" / f"{req.run_id}.jsonl"
            chain_path.parent.mkdir(parents=True, exist_ok=True)
            writer = ChainWriter(chain_path, req.run_id)

            current_entry = registry.get(pack_id)
            current_status = _to_pack_status(current_entry.promotion_status)
            transition(
                pack_id, current_status, PackStatus.DEFERRED,
                registry=registry, chain_writer=writer,
                reason=req.reason,
            )
            final_entry = registry.get(pack_id)
            return {"ok": True, "pack_id": pack_id, "status": final_entry.promotion_status}
        except InvalidTransitionError as e:
            raise HTTPException(status_code=409, detail={"error": str(e)})
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    # ------------------------------------------------------------------
    # Chain endpoints
    # ------------------------------------------------------------------

    @app.get("/api/runs/{run_id}/chain", response_model=List[ChainEntryView])
    def get_chain(run_id: str):
        chain_path = _storage_root / "chain" / f"{run_id}.jsonl"
        if not chain_path.exists():
            raise HTTPException(status_code=404, detail={"error": f"Chain not found for run {run_id}"})
        try:
            from harvest_core.provenance.chain_writer import ChainWriter
            writer = ChainWriter(chain_path, run_id)
            entries = writer.read_all()
            result = []
            for i, e in enumerate(entries):
                ts = e.timestamp
                import datetime as _dt
                if isinstance(ts, _dt.datetime):
                    ts = ts.timestamp()
                elif isinstance(ts, str):
                    try:
                        ts = _dt.datetime.fromisoformat(ts).timestamp()
                    except ValueError:
                        ts = 0.0
                result.append(ChainEntryView(
                    sequence=e.sequence if e.sequence is not None else i,
                    signal=e.signal,
                    machine=e.machine,
                    timestamp=float(ts),
                    data=e.data or {},
                ))
            return result
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    @app.get("/api/runs/{run_id}/receipt")
    def get_receipt(run_id: str):
        receipt_path = _storage_root / "receipts" / f"{run_id}.json"
        if not receipt_path.exists():
            raise HTTPException(status_code=404, detail={"error": f"No receipt for run {run_id}"})
        try:
            return JSONResponse(content=json.loads(receipt_path.read_text(encoding="utf-8")))
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    @app.get("/api/replays/{replay_id}/trace")
    def get_replay_trace(replay_id: str):
        """Serve Playwright trace .zip for `playwright show-trace`."""
        trace_path = _storage_root / "traces" / f"{replay_id}.zip"
        if not trace_path.exists():
            raise HTTPException(status_code=404, detail={"error": f"No trace for replay {replay_id}"})
        try:
            from fastapi.responses import FileResponse
            return FileResponse(str(trace_path), media_type="application/zip", filename=f"{replay_id}.zip")
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    @app.get("/api/stats")
    def get_stats():
        try:
            from harvest_index.registry.pack_registry import PackRegistry
            registry = PackRegistry(root=str(_registry_root))
            return registry.stats()
        except Exception as e:
            raise HTTPException(status_code=500, detail={"error": str(e)})

    # ------------------------------------------------------------------
    # Static SPA fallback (serves React build if present)
    # ------------------------------------------------------------------
    static_dir = Path(__file__).parent / "static"
    if static_dir.exists():
        app.mount("/", StaticFiles(directory=str(static_dir), html=True), name="static")

    return app


def _confidence_band(score: float) -> str:
    if score >= 0.90:
        return "GREEN"
    elif score >= 0.75:
        return "YELLOW"
    elif score >= 0.50:
        return "ORANGE"
    return "RED"


# ---------------------------------------------------------------------------
# CLI entry point: `harvest serve`
# ---------------------------------------------------------------------------

def serve(
    host: str = "127.0.0.1",
    port: int = 8742,
    registry_root: str = "registry",
    storage_root: str = "storage",
    reload: bool = False,
) -> None:
    """Start the Harvest Reviewer server. Fail-closed: raises if fastapi/uvicorn not installed."""
    _require_fastapi()
    try:
        import uvicorn
    except ImportError as e:
        raise ImportError("uvicorn not installed. Run: pip install uvicorn") from e

    app = create_app(registry_root=registry_root, storage_root=storage_root)
    uvicorn.run(app, host=host, port=port, reload=reload)
