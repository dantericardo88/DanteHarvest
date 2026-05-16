# PYTHON_ARGCOMPLETE_OK
"""
harvest CLI — operator entry point for DANTEHARVEST.

Commands:
  harvest ingest file <path>          — ingest a local file into the artifact store
  harvest ingest url <url>            — ingest a URL (robots.txt check first)
  harvest ingest batch <dir>          — ingest all supported files in a directory
  harvest crawl <url>                 — crawl a URL with Crawl4AI adapter
  harvest run create                  — create a new harvest run
  harvest run status <run-id>         — show run state and chain stats
  harvest observe browser <trace>     — ingest a browser session trace file
  harvest watch <dir>                 — watch a directory for new files and auto-ingest
  harvest pack list                   — list all registered packs
  harvest pack promote <pack-id>      — promote a CANDIDATE pack
  harvest pack export <pack-id>       — export a PROMOTED pack as HarvestHandoff JSON
  harvest stats                       — show pack registry statistics
  harvest serve                       — start the pack reviewer web server
  harvest version                     — print version and config

All commands emit exit code 0 on success, 1 on error.
Fail-closed: errors are always surfaced, never swallowed.
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import os
import sys
from pathlib import Path


__version__ = "0.1.0"

_SUPPORTED_FORMATS = ("table", "json", "csv")


# ---------------------------------------------------------------------------
# Output formatting helpers
# ---------------------------------------------------------------------------

def _format_pack_list(entries, fmt: str) -> str:
    """Format a list of pack registry entries as table, json, or csv."""
    if fmt == "json":
        return json.dumps(
            [
                {
                    "pack_id": e.pack_id,
                    "pack_type": e.pack_type,
                    "promotion_status": e.promotion_status,
                    "title": e.title,
                }
                for e in entries
            ],
            indent=2,
        )
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["pack_id", "pack_type", "promotion_status", "title"])
        for e in entries:
            writer.writerow([e.pack_id, e.pack_type, e.promotion_status, e.title])
        return buf.getvalue().rstrip()
    # default: table
    lines = []
    for e in entries:
        lines.append(f"  [{e.promotion_status:12s}] {e.pack_type:25s} {e.pack_id}  {e.title}")
    return "\n".join(lines)


def _format_stats(stats: dict, fmt: str) -> str:
    """Format registry stats as table, json, or csv."""
    if fmt == "json":
        return json.dumps(stats, indent=2)
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(["key", "value"])
        for k, v in stats.items():
            writer.writerow([k, v])
        return buf.getvalue().rstrip()
    # default: table
    lines = []
    for k, v in stats.items():
        lines.append(f"  {k:<30s} {v}")
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Ingest commands
# ---------------------------------------------------------------------------

async def cmd_ingest_file(args) -> int:
    from harvest_acquire.files.file_ingestor import FileIngestor
    from harvest_core.provenance.chain_writer import ChainWriter
    from harvest_core.rights.rights_model import SourceClass, default_rights_for

    path = Path(args.path)
    if not path.exists():
        print(f"error: file not found: {path}", file=sys.stderr)
        return 1

    run_id = args.run_id or f"cli-{path.stem}"
    storage_root = args.storage or "storage"
    chain_path = Path(storage_root) / "chain" / f"{run_id}.jsonl"
    chain_path.parent.mkdir(parents=True, exist_ok=True)

    writer = ChainWriter(chain_path, run_id)
    ingestor = FileIngestor(writer, storage_root=storage_root)

    try:
        result = await ingestor.ingest(
            path=path,
            run_id=run_id,
            rights_profile=default_rights_for(SourceClass.OWNED_INTERNAL),
        )
        print(json.dumps({
            "status": "ok",
            "artifact_id": result.artifact_id,
            "sha256": result.sha256,
            "source_type": result.source_type,
            "storage_uri": result.storage_uri,
        }, indent=2))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


async def cmd_ingest_url(args) -> int:
    from harvest_acquire.urls.url_ingestor import URLIngestor
    from harvest_acquire.browser.playwright_engine import create_playwright_engine
    from harvest_core.provenance.chain_writer import ChainWriter
    from harvest_core.rights.rights_model import SourceClass, default_rights_for

    run_id = args.run_id or "cli-url"
    storage_root = args.storage or "storage"
    chain_path = Path(storage_root) / "chain" / f"{run_id}.jsonl"
    chain_path.parent.mkdir(parents=True, exist_ok=True)

    writer = ChainWriter(chain_path, run_id)
    engine = None

    try:
        engine = await create_playwright_engine()
        ingestor = URLIngestor(writer, engine, storage_root=storage_root)
        result = await ingestor.ingest(
            url=args.url,
            run_id=run_id,
            rights_profile=default_rights_for(SourceClass.PUBLIC_WEB),
        )
        print(json.dumps({
            "status": "ok",
            "artifact_id": result.artifact_id,
            "url": args.url,
            "sha256": result.sha256,
            "storage_uri": result.storage_uri,
        }, indent=2))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    finally:
        if engine is not None:
            await engine.close()


_INGESTABLE_SUFFIXES = {
    ".pdf", ".docx", ".pptx", ".xlsx", ".xls", ".xlsm", ".csv", ".epub",
    ".txt", ".md", ".html", ".htm", ".mp3", ".wav", ".m4a", ".mp4", ".mov",
    ".png", ".jpg", ".jpeg", ".gif", ".webp",
}


async def cmd_ingest_batch(args) -> int:
    from harvest_acquire.files.file_ingestor import FileIngestor
    from harvest_core.provenance.chain_writer import ChainWriter
    from harvest_core.rights.rights_model import SourceClass, default_rights_for
    from rich.progress import Progress, SpinnerColumn, BarColumn, TextColumn, TimeElapsedColumn

    directory = Path(args.directory)
    if not directory.is_dir():
        print(f"error: not a directory: {directory}", file=sys.stderr)
        return 1

    pattern = getattr(args, "pattern", None) or "**/*"
    files = [
        f for f in directory.glob(pattern)
        if f.is_file() and f.suffix.lower() in _INGESTABLE_SUFFIXES
    ]

    if not files:
        print(json.dumps({"status": "ok", "files_ingested": 0, "message": "no supported files found"}))
        return 0

    storage_root = args.storage or "storage"
    run_id = args.run_id or f"batch-{directory.name}"
    chain_path = Path(storage_root) / "chain" / f"{run_id}.jsonl"
    chain_path.parent.mkdir(parents=True, exist_ok=True)

    writer = ChainWriter(chain_path, run_id)
    ingestor = FileIngestor(writer, storage_root=storage_root)
    rights = default_rights_for(SourceClass.OWNED_INTERNAL)

    results = []
    errors = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TextColumn("{task.completed}/{task.total}"),
        TimeElapsedColumn(),
        transient=True,
    ) as progress:
        task = progress.add_task(f"Ingesting {len(files)} files…", total=len(files))
        for file in files:
            progress.update(task, description=f"[cyan]{file.name}[/cyan]")
            try:
                result = await ingestor.ingest(path=file, run_id=run_id, rights_profile=rights)
                results.append({"file": str(file), "artifact_id": result.artifact_id, "sha256": result.sha256})
            except Exception as e:
                errors.append({"file": str(file), "error": str(e)})
            progress.advance(task)

    print(json.dumps({
        "status": "ok",
        "files_ingested": len(results),
        "errors": len(errors),
        "results": results,
        "error_details": errors,
    }, indent=2))
    return 0 if not errors else 1


async def cmd_crawl(args) -> int:
    from harvest_acquire.crawl.crawlee_adapter import CrawleeAdapter
    from harvest_core.provenance.chain_writer import ChainWriter
    from harvest_core.rights.rights_model import SourceClass, default_rights_for

    run_id = args.run_id or "cli-crawl"
    storage_root = args.storage or "storage"
    chain_path = Path(storage_root) / "chain" / f"{run_id}.jsonl"
    chain_path.parent.mkdir(parents=True, exist_ok=True)

    writer = ChainWriter(chain_path, run_id)
    adapter = CrawleeAdapter(writer, storage_root=storage_root)
    max_pages = getattr(args, "max_pages", 10)

    try:
        if getattr(args, "sitemap", False):
            from harvest_acquire.crawl.sitemap_parser import SitemapParser
            parser = SitemapParser(max_urls=max_pages)
            urls = parser.discover_and_parse(args.url)
            if not urls:
                print(json.dumps({"status": "ok", "pages_crawled": 0, "note": "no sitemap found"}))
                return 0
            total_pages = 0
            for url in urls[:max_pages]:
                r = await adapter.crawl(url=url, run_id=run_id, max_pages=1)
                total_pages += r.page_count
            print(json.dumps({"status": "ok", "pages_crawled": total_pages, "sitemap_urls": len(urls)}))
        else:
            result = await adapter.crawl(
                url=args.url,
                run_id=run_id,
                rights_profile=default_rights_for(SourceClass.PUBLIC_WEB),
                max_pages=max_pages,
            )
            print(json.dumps({
                "status": "ok",
                "pages_crawled": result.page_count,
                "total_bytes": result.total_bytes,
            }, indent=2))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Run commands
# ---------------------------------------------------------------------------

async def cmd_run_create(args) -> int:
    from harvest_core.control.run_registry import RunRegistry
    from harvest_core.control.run_contract import RunContract
    from harvest_core.rights.rights_model import SourceClass

    storage_root = args.storage or "storage"
    project_id = args.project_id or "default"
    source_class = SourceClass(args.source_class) if args.source_class else SourceClass.OWNED_INTERNAL
    initiated_by = getattr(args, "initiated_by", None) or "cli"

    registry = RunRegistry(storage_root=storage_root)
    contract = RunContract(
        project_id=project_id,
        source_class=source_class,
        initiated_by=initiated_by,
    )

    try:
        record = await registry.create_run(contract)
        print(json.dumps({
            "status": "ok",
            "run_id": record.contract.run_id,
            "project_id": record.contract.project_id,
            "state": record.status.value,
        }, indent=2))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_run_status(args) -> int:
    from harvest_core.control.run_registry import RunRegistry

    storage_root = args.storage or "storage"
    registry = RunRegistry(storage_root=storage_root)

    try:
        record = registry.get_run(args.run_id)
        chain_path = Path(record.contract.chain_file_path(storage_root))
        entry_count = 0
        if chain_path.exists():
            entry_count = sum(1 for _ in chain_path.read_text(encoding="utf-8").splitlines() if _.strip())
        print(json.dumps({
            "run_id": record.contract.run_id,
            "state": record.status.value,
            "project_id": record.contract.project_id,
            "source_class": record.contract.source_class.value,
            "chain_entries": entry_count,
        }, indent=2))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Observe commands
# ---------------------------------------------------------------------------

async def cmd_observe_daemon(args) -> int:
    """Start the 24/7 observation daemon."""
    from harvest_observe.daemon.observation_daemon import ObservationDaemon, DaemonConfig
    from harvest_core.provenance.chain_writer import ChainWriter

    storage_root = args.storage or "storage"
    run_id = args.run_id or f"daemon-{Path(storage_root).name}"
    chain_path = Path(storage_root) / "chain" / f"{run_id}.jsonl"
    chain_path.parent.mkdir(parents=True, exist_ok=True)

    writer = ChainWriter(chain_path, run_id)
    config = DaemonConfig(
        storage_root=storage_root,
        run_id=run_id,
        capture_interval_s=getattr(args, "interval", 5.0),
        heartbeat_interval_s=getattr(args, "heartbeat", 60.0),
        ocr_enabled=not getattr(args, "no_ocr", False),
        event_capture_enabled=not getattr(args, "no_events", False),
        pid_file=getattr(args, "pid_file", None),
    )
    daemon = ObservationDaemon(config=config, chain_writer=writer)
    print(f"Starting observation daemon (run_id={run_id}, Ctrl+C to stop)")
    try:
        await daemon.run()
    except KeyboardInterrupt:
        daemon.stop()
    return 0


async def cmd_observe_browser(args) -> int:
    from harvest_observe.browser_session.session_recorder import SessionRecorder
    from harvest_core.provenance.chain_writer import ChainWriter

    trace_path = Path(args.trace)
    if not trace_path.exists():
        print(f"error: trace file not found: {trace_path}", file=sys.stderr)
        return 1

    run_id = args.run_id or f"cli-obs-{trace_path.stem}"
    storage_root = args.storage or "storage"
    chain_path = Path(storage_root) / "chain" / f"{run_id}.jsonl"
    chain_path.parent.mkdir(parents=True, exist_ok=True)

    writer = ChainWriter(chain_path, run_id)
    recorder = SessionRecorder(
        writer,
        session_id=run_id,
        storage_root=storage_root,
    )

    try:
        result = await recorder.ingest_trace_file(trace_path, run_id=run_id)
        print(json.dumps({
            "status": "ok",
            "session_id": result.session_id,
            "action_count": result.action_count,
        }, indent=2))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Pack commands
# ---------------------------------------------------------------------------

def cmd_pack_list(args) -> int:
    from harvest_index.registry.pack_registry import PackRegistry

    root = args.registry or "registry"
    fmt = getattr(args, "format", "table") or "table"
    try:
        registry = PackRegistry(root=root)
        entries = registry.list(
            pack_type=getattr(args, "type", None),
            status=getattr(args, "status", None),
        )
        if not entries:
            if fmt == "json":
                print("[]")
            elif fmt == "csv":
                print("pack_id,pack_type,promotion_status,title")
            else:
                print("No packs registered.")
            return 0
        print(_format_pack_list(entries, fmt))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_pack_promote(args) -> int:
    from harvest_index.registry.pack_registry import PackRegistry, RegistryError

    root = args.registry or "registry"
    try:
        registry = PackRegistry(root=root)
        receipt_id = getattr(args, "receipt_id", None)
        entry = registry.promote(args.pack_id, receipt_id=receipt_id)
        print(f"promoted: {entry.pack_id} ({entry.pack_type})")
        return 0
    except RegistryError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_pack_export(args) -> int:
    from harvest_index.registry.pack_registry import PackRegistry
    from harvest_distill.packs.dante_agents_contract import DanteAgentsExporter

    root = args.registry or "registry"
    output = args.output or f"{args.pack_id}.handoff.json"
    try:
        registry = PackRegistry(root=root)
        exporter = DanteAgentsExporter(registry)
        handoff = exporter.export(args.pack_id, domain=getattr(args, "domain", "general"))
        handoff.write(Path(output))
        print(f"exported: {output}")
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_registry_stats(args) -> int:
    from harvest_index.registry.pack_registry import PackRegistry

    root = args.registry or "registry"
    fmt = getattr(args, "format", "table") or "table"
    try:
        registry = PackRegistry(root=root)
        stats = registry.stats()
        print(_format_stats(stats, fmt))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


# ---------------------------------------------------------------------------
# Watch command — watchdog-powered filesystem monitoring
# ---------------------------------------------------------------------------

class _HarvestEventHandler:
    """Watchdog file-system event handler that auto-ingests new/modified files."""

    def __init__(self, ingestor, run_id: str, rights, loop: asyncio.AbstractEventLoop):
        self._ingestor = ingestor
        self._run_id = run_id
        self._rights = rights
        self._loop = loop

    # watchdog calls these synchronously from a background thread
    def on_created(self, event) -> None:
        if not event.is_directory:
            self._handle(event.src_path, "created")

    def on_modified(self, event) -> None:
        if not event.is_directory:
            self._handle(event.src_path, "modified")

    def _handle(self, src_path: str, event_type: str) -> None:
        path = Path(src_path)
        if path.suffix.lower() not in _INGESTABLE_SUFFIXES:
            return
        print(json.dumps({"event": event_type, "file": str(path)}))
        future = asyncio.run_coroutine_threadsafe(
            self._ingest(path), self._loop
        )
        try:
            future.result(timeout=60)
        except Exception as exc:
            print(json.dumps({"event": "error", "file": str(path), "error": str(exc)}),
                  file=sys.stderr)

    async def _ingest(self, path: Path) -> None:
        result = await self._ingestor.ingest(
            path=path, run_id=self._run_id, rights_profile=self._rights
        )
        print(json.dumps({
            "event": "ingested",
            "file": str(path),
            "artifact_id": result.artifact_id,
        }))


async def cmd_watch(args) -> int:
    """
    Watch a directory for new/modified files and auto-ingest them.
    Uses watchdog when available; falls back to polling every --interval seconds.
    """
    from harvest_acquire.files.file_ingestor import FileIngestor
    from harvest_core.provenance.chain_writer import ChainWriter
    from harvest_core.rights.rights_model import SourceClass, default_rights_for

    directory = Path(args.directory)
    if not directory.is_dir():
        print(f"error: not a directory: {directory}", file=sys.stderr)
        return 1

    interval = getattr(args, "interval", 5)
    storage_root = args.storage or "storage"
    run_id = args.run_id or f"watch-{directory.name}"
    chain_path = Path(storage_root) / "chain" / f"{run_id}.jsonl"
    chain_path.parent.mkdir(parents=True, exist_ok=True)

    writer = ChainWriter(chain_path, run_id)
    ingestor = FileIngestor(writer, storage_root=storage_root)
    rights = default_rights_for(SourceClass.OWNED_INTERNAL)

    # Try watchdog first
    try:
        from watchdog.observers import Observer
        from watchdog.events import FileSystemEventHandler

        loop = asyncio.get_event_loop()
        harvest_handler = _HarvestEventHandler(ingestor, run_id, rights, loop)

        # Wrap our handler so watchdog can call it
        class _WatchdogBridge(FileSystemEventHandler):
            def on_created(self, event):  # type: ignore[override]
                harvest_handler.on_created(event)

            def on_modified(self, event):  # type: ignore[override]
                harvest_handler.on_modified(event)

        observer = Observer()
        observer.schedule(_WatchdogBridge(), str(directory), recursive=True)
        observer.start()
        print(f"watching {directory} (watchdog, Ctrl+C to stop)")
        try:
            while True:
                await asyncio.sleep(1)
        except KeyboardInterrupt:
            pass
        finally:
            observer.stop()
            observer.join()
        print("\nwatch stopped.")
        return 0

    except ImportError:
        pass  # fall back to polling

    # Polling fallback
    import time

    seen: set = set(
        f for f in directory.glob("**/*")
        if f.is_file() and f.suffix.lower() in _INGESTABLE_SUFFIXES
    )
    print(f"watching {directory} (polling interval={interval}s, {len(seen)} existing files skipped)")

    try:
        while True:
            time.sleep(interval)
            current = set(
                f for f in directory.glob("**/*")
                if f.is_file() and f.suffix.lower() in _INGESTABLE_SUFFIXES
            )
            new_files = current - seen
            for file in sorted(new_files):
                try:
                    result = await ingestor.ingest(path=file, run_id=run_id, rights_profile=rights)
                    print(json.dumps({"event": "ingested", "file": str(file), "artifact_id": result.artifact_id}))
                except Exception as e:
                    print(json.dumps({"event": "error", "file": str(file), "error": str(e)}), file=sys.stderr)
            seen = current
    except KeyboardInterrupt:
        print("\nwatch stopped.")
    return 0


def cmd_tui(args) -> int:
    """Launch the interactive TUI dashboard."""
    from harvest_ui.tui_app import launch_tui
    launch_tui(
        storage_root=args.storage or "storage",
        registry_root=args.registry or "registry",
        refresh_interval=getattr(args, "refresh", 2.0) or 2.0,
    )
    return 0


def cmd_serve(args) -> int:
    from harvest_ui.reviewer.server import serve
    try:
        serve(
            host=args.host,
            port=args.port,
            registry_root=args.registry or "registry",
            storage_root=args.storage or "storage",
        )
        return 0
    except ImportError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_version(_args) -> int:
    """Show DanteHarvest version."""
    print(f"harvest {__version__}")
    return 0


def cmd_status(args) -> int:
    """Show DanteHarvest system status — connectors, storage, indexing."""
    output_format = getattr(args, "output_format", "text") or "text"
    try:
        from harvest_acquire.connectors.connector_registry import ConnectorRegistry
        raw = ConnectorRegistry.discover_available() if hasattr(ConnectorRegistry, 'discover_available') else {}
        # Normalize to a JSON-safe dict (ConnectorStatus objects may not be serializable)
        if isinstance(raw, dict):
            connectors = {
                k: (v if isinstance(v, (str, int, float, bool, type(None)))
                    else (v.__dict__ if hasattr(v, '__dict__') else str(v)))
                for k, v in raw.items()
            }
        else:
            connectors = {}
    except Exception:
        connectors = {}

    status = {
        "connectors": connectors,
        "storage": "ok",
        "version": __version__,
    }

    if output_format == "json":
        print(json.dumps(status, indent=2))
    elif output_format == "table":
        print(f"{'Key':<20} {'Value'}")
        print(f"{'---':<20} {'-----'}")
        print(f"{'version':<20} {status['version']}")
        print(f"{'storage':<20} {status['storage']}")
        print(f"{'connectors':<20} {len(status.get('connectors', {}))} configured")
    else:
        print("DanteHarvest Status")
        print(f"  Storage: {status['storage']}")
        print(f"  Version: {status['version']}")
        print(f"  Connectors: {len(status.get('connectors', {}))} configured")
    return 0


def cmd_validate(args) -> int:
    """Validate a harvest configuration file."""
    output_format = getattr(args, "output_format", "text") or "text"
    config_path = args.config_path
    errors = []
    if not os.path.exists(config_path):
        errors.append(f"File not found: {config_path}")
    else:
        try:
            with open(config_path) as f:
                data = json.load(f)
            if "source" not in data and "sources" not in data:
                errors.append("Missing required field: 'source' or 'sources'")
        except json.JSONDecodeError as e:
            errors.append(f"Invalid JSON: {e}")

    result = {"valid": len(errors) == 0, "errors": errors, "path": config_path}

    if output_format == "json":
        print(json.dumps(result, indent=2))
    else:
        if result["valid"]:
            print(f"OK {config_path} is valid")
        else:
            for err in errors:
                print(f"FAIL {err}", file=sys.stderr)
    return 0 if result["valid"] else 1


# ---------------------------------------------------------------------------
# Schedule commands
# ---------------------------------------------------------------------------

def cmd_schedule_add(args) -> int:
    from harvest_ui.scheduler.scheduler import HarvestScheduler, SchedulerError
    import json as _json
    scheduler = HarvestScheduler(storage_root=args.storage)
    extra_args: dict = {}
    if hasattr(args, "url") and args.url:
        extra_args["url"] = args.url
    if hasattr(args, "path") and args.path:
        extra_args["path"] = args.path
    if hasattr(args, "depth") and args.depth is not None:
        extra_args["max_depth"] = args.depth
    if hasattr(args, "pages") and args.pages is not None:
        extra_args["max_pages"] = args.pages
    try:
        entry = scheduler.add(command=args.schedule_command, cron_expr=args.cron, args=extra_args)
        print(f"Scheduled: {entry.schedule_id}")
        print(f"  command : {entry.command}")
        print(f"  cron    : {entry.cron_expr}")
        print(f"  args    : {_json.dumps(entry.args)}")
        if entry.next_run:
            import datetime
            print(f"  next_run: {datetime.datetime.fromtimestamp(entry.next_run, tz=datetime.timezone.utc).isoformat()}Z")
        return 0
    except SchedulerError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_schedule_list(args) -> int:
    from harvest_ui.scheduler.scheduler import HarvestScheduler
    scheduler = HarvestScheduler(storage_root=args.storage)
    entries = scheduler.list(status=getattr(args, "status", None))
    if not entries:
        print("No scheduled jobs.")
        return 0
    for e in entries:
        print(f"{e.schedule_id[:8]}  {e.status:<8}  {e.cron_expr:<15}  {e.command}  runs={e.run_count}")
    return 0


def cmd_schedule_remove(args) -> int:
    from harvest_ui.scheduler.scheduler import HarvestScheduler
    scheduler = HarvestScheduler(storage_root=args.storage)
    removed = scheduler.remove(args.schedule_id)
    if removed:
        print(f"Removed: {args.schedule_id}")
        return 0
    print(f"error: schedule not found: {args.schedule_id}", file=sys.stderr)
    return 1


def cmd_ingest_github(args) -> int:
    from harvest_acquire.connectors.github_connector import GitHubConnector, ConnectorError
    try:
        connector = GitHubConnector(token=args.token or None, storage_root=args.storage)
        aids = connector.ingest(
            repo=args.repo,
            path=getattr(args, "path", "") or "",
            branch=getattr(args, "branch", "HEAD") or "HEAD",
        )
        print(f"GitHub: ingested {len(aids)} files from {args.repo}")
        return 0
    except ConnectorError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_ingest_notion(args) -> int:
    from harvest_acquire.connectors.notion_connector import NotionConnector, ConnectorError
    try:
        connector = NotionConnector(token=args.token, storage_root=args.storage)
        if hasattr(args, "page_id") and args.page_id:
            aids = connector.ingest(page_id=args.page_id)
        elif hasattr(args, "database_id") and args.database_id:
            aids = connector.ingest(database_id=args.database_id)
        else:
            print("error: specify --page-id or --database-id", file=sys.stderr)
            return 1
        print(f"Notion: ingested {len(aids)} pages")
        return 0
    except ConnectorError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_ingest_s3(args) -> int:
    from harvest_acquire.connectors.s3_connector import S3Connector, ConnectorError
    try:
        connector = S3Connector(
            bucket=args.bucket,
            endpoint_url=getattr(args, "endpoint_url", None),
            storage_root=args.storage,
        )
        aids = connector.ingest(prefix=getattr(args, "prefix", "") or "")
        print(f"S3: ingested {len(aids)} files from s3://{args.bucket}")
        return 0
    except ConnectorError as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


def cmd_verify_chain(args) -> int:
    """Verify the Merkle-sealed evidence chain for a run."""
    from harvest_core.provenance.merkle_chain import MerkleChainManifest
    from harvest_core.provenance.chain_writer import ChainWriter

    chain_path = Path(args.chain_path)
    if not chain_path.exists():
        print(f"error: chain file not found: {chain_path}", file=sys.stderr)
        return 1

    run_id = args.run_id or chain_path.stem
    mcm = MerkleChainManifest(chain_path)

    if not mcm.is_sealed():
        if getattr(args, "seal", False):
            writer = ChainWriter(chain_path, run_id)
            entries = writer.read_all()
            manifest = mcm.seal(entries)
            print(json.dumps({
                "status": "sealed",
                "run_id": run_id,
                "entry_count": manifest.entry_count,
                "merkle_root": manifest.merkle_root,
                "manifest_path": str(mcm.manifest_path),
            }, indent=2))
            return 0
        print(json.dumps({"status": "unsealed", "chain": str(chain_path),
                          "hint": "run with --seal to seal it"}, indent=2))
        return 1

    writer = ChainWriter(chain_path, run_id)
    entries = writer.read_all()
    ok, reason = mcm.verify(entries)

    result = {
        "status": "ok" if ok else "tampered",
        "chain": str(chain_path),
        "entry_count": len(entries),
        "merkle_root": mcm.load_manifest().merkle_root if ok else None,
        "reason": reason,
    }
    print(json.dumps(result, indent=2))
    return 0 if ok else 1


def cmd_gc(args) -> int:
    """Garbage-collect artifacts past their retention window."""
    from harvest_core.rights.retention_enforcer import RetentionEnforcer

    storage_root = args.storage or "storage"
    dry_run = getattr(args, "dry_run", False)
    enforcer = RetentionEnforcer(store_path=Path(storage_root))
    report = enforcer.gc(dry_run=dry_run)

    if not report:
        print(json.dumps({"status": "ok", "expired": 0,
                          "dry_run": dry_run}))
        return 0

    print(json.dumps({
        "status": "ok",
        "expired": len(report),
        "dry_run": dry_run,
        "artifacts": [
            {"artifact_id": ea.artifact_id, "retention_class": ea.retention_class,
             "expires_at": ea.expires_at}
            for ea in report
        ],
    }, indent=2))
    return 0


def cmd_replay_diff(args) -> int:
    """Side-by-side diff of two ReplayReport JSON files."""
    from harvest_index.registry.replay_differ import ReplayDiffer

    fmt = getattr(args, "format", "text") or "text"
    differ = ReplayDiffer()
    try:
        diff = differ.diff_files(Path(args.report_a), Path(args.report_b))
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    if fmt == "json":
        print(json.dumps(diff.to_dict(), indent=2))
    else:
        print(diff.to_text())
    return 0


def cmd_pack_diff(args) -> int:
    """Diff two pack versions stored in the registry."""
    from harvest_index.registry.pack_registry import PackRegistry
    from harvest_distill.packs.pack_differ import PackDiffer

    root = args.registry or "registry"
    fmt = getattr(args, "format", "text") or "text"

    registry = PackRegistry(root=root)
    differ = PackDiffer(changelog_dir=Path(root) / "changelogs")

    try:
        old_pack = registry.load_pack_json(args.old_pack_id)
        new_pack = registry.load_pack_json(args.new_pack_id)
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1

    diff = differ.diff(
        old_pack, new_pack,
        old_label=args.old_pack_id,
        new_label=args.new_pack_id,
    )

    if getattr(args, "record", False):
        differ.record_changelog(diff)

    if fmt == "json":
        print(json.dumps(diff.to_dict(), indent=2))
    else:
        print(diff.to_text())
    return 0


def cmd_pack_changelog(args) -> int:
    """Show changelog history for a pack."""
    from harvest_distill.packs.pack_differ import PackDiffer

    root = args.registry or "registry"
    fmt = getattr(args, "format", "table") or "table"
    differ = PackDiffer(changelog_dir=Path(root) / "changelogs")
    entries = differ.changelog_for(args.pack_id)

    if not entries:
        print(f"No changelog entries for {args.pack_id}")
        return 0

    if fmt == "json":
        print(json.dumps([e.to_dict() for e in entries], indent=2))
        return 0

    import datetime as _dt
    print(f"\nChangelog for {args.pack_id}:\n")
    for e in entries:
        ts = _dt.datetime.fromtimestamp(e.recorded_at, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
        print(f"  {ts}  {e.old_version_label} → {e.new_version_label}  {e.summary}")
    print()
    return 0


def cmd_key_rotate(args) -> int:
    """Rotate encryption key: re-encrypt all artifacts with a new passphrase."""
    from harvest_core.crypto.key_manager import KeyManager, KeyRotator
    from harvest_core.storage.encrypted_store import EncryptedStore

    storage_root = Path(args.storage or "storage")
    old_passphrase = os.environ.get("HARVEST_ENCRYPT_KEY") or getattr(args, "old_key", None)
    new_passphrase = getattr(args, "new_key", None) or os.environ.get("HARVEST_ENCRYPT_KEY_NEW")

    if not old_passphrase:
        print("error: set HARVEST_ENCRYPT_KEY (or --old-key) for the current passphrase", file=sys.stderr)
        return 1
    if not new_passphrase:
        print("error: set HARVEST_ENCRYPT_KEY_NEW (or --new-key) for the new passphrase", file=sys.stderr)
        return 1

    km = KeyManager(storage_root=storage_root)
    km.initialize(old_passphrase)
    old_store = EncryptedStore(passphrase=old_passphrase)
    new_version = km.rotate(new_passphrase, hint=getattr(args, "hint", "") or "")
    new_store = EncryptedStore(passphrase=new_passphrase)

    artifacts_dir = storage_root / "artifacts"
    if not artifacts_dir.exists():
        print(json.dumps({"status": "ok", "rotated": 0, "note": "no artifacts directory"}))
        return 0

    rotator = KeyRotator(artifacts_dir=artifacts_dir, log_dir=storage_root / "crypto")
    ok_count = err_count = 0
    errors = []
    for result in rotator.rotate(old_store, new_store):
        if result.success:
            ok_count += 1
        else:
            err_count += 1
            errors.append({"path": result.artifact_path, "error": result.error})

    print(json.dumps({
        "status": "ok" if not err_count else "partial",
        "new_version_id": new_version.version_id,
        "rotated": ok_count,
        "errors": err_count,
        "error_details": errors,
    }, indent=2))
    return 0 if not err_count else 1


def cmd_trace_view(args) -> int:
    """Render a SessionTracer JSONL trace file in human-readable format."""
    import datetime as _dt

    trace_path = Path(args.trace_file)
    if not trace_path.exists():
        print(f"error: trace file not found: {trace_path}", file=sys.stderr)
        return 1

    lines = [l.strip() for l in trace_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    if not lines:
        print("error: trace file is empty", file=sys.stderr)
        return 1

    try:
        header = json.loads(lines[0])
    except Exception:
        print("error: could not parse trace header", file=sys.stderr)
        return 1

    events = []
    for line in lines[1:]:
        try:
            events.append(json.loads(line))
        except Exception:
            pass

    fmt = getattr(args, "format", "table") or "table"
    filter_event = getattr(args, "filter", None)
    if filter_event:
        events = [e for e in events if filter_event.lower() in e.get("event_name", "").lower()]

    if fmt == "json":
        print(json.dumps({"header": header, "events": events}, indent=2))
        return 0

    # Table / human view
    tid = header.get("trajectory_id", "?")
    started = header.get("started_at", 0)
    finished = header.get("finished_at") or 0
    duration = finished - started if finished and started else None

    started_str = _dt.datetime.fromtimestamp(started, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC") if started else "?"
    dur_str = f"{duration:.2f}s" if duration is not None else "?"

    print(f"\n{'='*64}")
    print(f"  Trace: {tid}")
    print(f"  Started  : {started_str}")
    print(f"  Duration : {dur_str}")
    if header.get("playwright_trace_path"):
        print(f"  PW trace : {header['playwright_trace_path']}")
    print(f"  Events   : {len(events)}")
    print(f"{'='*64}")

    for i, ev in enumerate(events, 1):
        ts = ev.get("timestamp", 0)
        rel = ts - started if started else 0
        name = ev.get("event_name", "?")
        data = ev.get("data", {})
        shot = ev.get("screenshot_path")

        data_str = "  ".join(f"{k}={json.dumps(v)}" for k, v in data.items()) if data else ""
        shot_str = f"  [screenshot: {shot}]" if shot else ""
        print(f"  [{i:3d}] +{rel:7.3f}s  {name:<35s}  {data_str}{shot_str}")

    print(f"{'='*64}\n")
    return 0


def cmd_trace_list(args) -> int:
    """List all trace files in the trace directory."""
    import datetime as _dt

    trace_dir = Path(getattr(args, "trace_dir", None) or "storage/traces")
    if not trace_dir.exists():
        print(json.dumps({"traces": [], "trace_dir": str(trace_dir)}))
        return 0

    traces = sorted(trace_dir.glob("*.jsonl"), key=lambda p: p.stat().st_mtime, reverse=True)
    limit = getattr(args, "limit", 20) or 20
    traces = traces[:limit]

    fmt = getattr(args, "format", "table") or "table"

    if fmt == "json":
        out = []
        for t in traces:
            try:
                first_line = t.read_text(encoding="utf-8").splitlines()[0]
                header = json.loads(first_line)
            except Exception:
                header = {}
            out.append({
                "path": str(t),
                "trajectory_id": header.get("trajectory_id", t.stem),
                "started_at": header.get("started_at"),
                "event_count": header.get("event_count", "?"),
            })
        print(json.dumps({"traces": out}, indent=2))
        return 0

    if not traces:
        print(f"No traces found in {trace_dir}")
        return 0

    print(f"\nTraces in {trace_dir}  (newest first):\n")
    for t in traces:
        try:
            first_line = t.read_text(encoding="utf-8").splitlines()[0]
            header = json.loads(first_line)
            tid = header.get("trajectory_id", t.stem)[:36]
            started = header.get("started_at", 0)
            count = header.get("event_count", "?")
            started_str = _dt.datetime.fromtimestamp(started, tz=_dt.timezone.utc).strftime("%Y-%m-%d %H:%M") if started else "?"
            print(f"  {started_str}  events={count:<4}  {tid}  {t.name}")
        except Exception:
            print(f"  (unreadable)  {t.name}")
    print()
    return 0


def cmd_trace_diff(args) -> int:
    """Side-by-side diff of two trace files."""
    left_path = Path(args.trace_a)
    right_path = Path(args.trace_b)

    for p in (left_path, right_path):
        if not p.exists():
            print(f"error: trace file not found: {p}", file=sys.stderr)
            return 1

    def _load_events(p: Path):
        lines = [l.strip() for l in p.read_text(encoding="utf-8").splitlines() if l.strip()]
        header = json.loads(lines[0]) if lines else {}
        events = []
        for line in lines[1:]:
            try:
                events.append(json.loads(line))
            except Exception:
                pass
        return header, events

    h_a, ev_a = _load_events(left_path)
    h_b, ev_b = _load_events(right_path)

    max_len = max(len(ev_a), len(ev_b))
    col = 50

    print(f"\n{'A: ' + left_path.name:<{col}}  {'B: ' + right_path.name}")
    print(f"{'─'*col}  {'─'*col}")

    for i in range(max_len):
        a_name = ev_a[i].get("event_name", "?") if i < len(ev_a) else "<missing>"
        b_name = ev_b[i].get("event_name", "?") if i < len(ev_b) else "<missing>"
        marker = "  " if a_name == b_name else "!!"
        print(f"  {i+1:3d}  {a_name:<{col-6}}{marker}  {b_name}")

    total_a, total_b = len(ev_a), len(ev_b)
    matched = sum(
        1 for i in range(min(total_a, total_b))
        if ev_a[i].get("event_name") == ev_b[i].get("event_name")
    )
    print(f"\nA={total_a} events, B={total_b} events, {matched}/{min(total_a, total_b)} matched by position\n")
    return 0


def cmd_schedule_run_now(args) -> int:
    from harvest_ui.scheduler.scheduler import HarvestScheduler
    scheduler = HarvestScheduler(storage_root=args.storage)
    entry = scheduler._store.get(args.schedule_id)
    if entry is None:
        print(f"error: schedule not found: {args.schedule_id}", file=sys.stderr)
        return 1
    print(f"Running {entry.command} ({args.schedule_id}) now…")
    asyncio.run(scheduler._execute(entry))
    print("Done.")
    return 0


# ---------------------------------------------------------------------------
# Argument parser
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="harvest",
        description="DANTEHARVEST — evidence-rich acquisition and pack factory",
    )
    parser.add_argument("--registry", default="registry", help="Pack registry root directory")
    parser.add_argument("--storage", default="storage", help="Artifact storage root directory")

    sub = parser.add_subparsers(dest="command", metavar="COMMAND")

    # ingest
    ingest = sub.add_parser("ingest", help="Ingest content into the artifact store")
    ingest_sub = ingest.add_subparsers(dest="ingest_type", metavar="TYPE")

    ingest_file = ingest_sub.add_parser("file", help="Ingest a local file")
    ingest_file.add_argument("path", help="Path to the file")
    ingest_file.add_argument("--run-id", dest="run_id", default=None)

    ingest_url = ingest_sub.add_parser("url", help="Ingest a URL (robots.txt enforced)")
    ingest_url.add_argument("url", help="URL to fetch")
    ingest_url.add_argument("--run-id", dest="run_id", default=None)

    ingest_batch = ingest_sub.add_parser("batch", help="Ingest all supported files in a directory")
    ingest_batch.add_argument("directory", help="Directory to scan")
    ingest_batch.add_argument("--run-id", dest="run_id", default=None)
    ingest_batch.add_argument("--pattern", default=None, help="Glob pattern (default: **/*)")

    ingest_github = ingest_sub.add_parser("github", help="Ingest a GitHub repository")
    ingest_github.add_argument("--repo", required=True, help="Repository in 'owner/repo' format")
    ingest_github.add_argument("--token", default=None, help="GitHub personal access token")
    ingest_github.add_argument("--path", default="", help="Repository path to start from")
    ingest_github.add_argument("--branch", default="HEAD", help="Branch or ref (default: HEAD)")

    ingest_notion = ingest_sub.add_parser("notion", help="Ingest a Notion page or database")
    ingest_notion.add_argument("--token", required=True, help="Notion integration token")
    ingest_notion.add_argument("--page-id", dest="page_id", default=None)
    ingest_notion.add_argument("--database-id", dest="database_id", default=None)

    ingest_s3 = ingest_sub.add_parser("s3", help="Ingest files from S3/GCS/MinIO")
    ingest_s3.add_argument("--bucket", required=True, help="S3 bucket name")
    ingest_s3.add_argument("--prefix", default="", help="Key prefix filter")
    ingest_s3.add_argument("--endpoint-url", dest="endpoint_url", default=None,
                           help="Custom endpoint URL (for MinIO/GCS)")

    # crawl
    crawl_p = sub.add_parser("crawl", help="Crawl a URL with Crawl4AI or Crawlee adapter")
    crawl_p.add_argument("url", help="URL to crawl")
    crawl_p.add_argument("--run-id", dest="run_id", default=None)
    crawl_p.add_argument("--sitemap", action="store_true", default=False,
                         help="Discover and crawl all URLs from sitemap.xml")
    crawl_p.add_argument("--max-pages", dest="max_pages", type=int, default=10)

    # run
    run = sub.add_parser("run", help="Run lifecycle operations")
    run_sub = run.add_subparsers(dest="run_cmd", metavar="SUBCMD")

    run_create = run_sub.add_parser("create", help="Create a new harvest run")
    run_create.add_argument("--project-id", dest="project_id", default=None)
    run_create.add_argument("--source-class", dest="source_class", default=None,
                            help="SourceClass enum value (e.g. owned_internal)")
    run_create.add_argument("--initiated-by", dest="initiated_by", default="cli",
                            help="Operator or service identity (default: cli)")

    run_status = run_sub.add_parser("status", help="Show run state")
    run_status.add_argument("run_id", help="Run ID")

    # observe
    observe = sub.add_parser("observe", help="Observation plane operations")
    observe_sub = observe.add_subparsers(dest="observe_cmd", metavar="SUBCMD")

    obs_browser = observe_sub.add_parser("browser", help="Ingest a browser session trace")
    obs_browser.add_argument("trace", help="Path to trace JSON file")
    obs_browser.add_argument("--run-id", dest="run_id", default=None)

    obs_daemon = observe_sub.add_parser("daemon", help="Start 24/7 observation daemon")
    obs_daemon.add_argument("--run-id", dest="run_id", default=None)
    obs_daemon.add_argument("--interval", type=float, default=5.0,
                            help="Screen capture interval in seconds (default: 5.0)")
    obs_daemon.add_argument("--heartbeat", type=float, default=60.0,
                            help="Chain heartbeat interval in seconds (default: 60.0)")
    obs_daemon.add_argument("--no-ocr", dest="no_ocr", action="store_true", default=False)
    obs_daemon.add_argument("--no-events", dest="no_events", action="store_true", default=False)
    obs_daemon.add_argument("--pid-file", dest="pid_file", default=None)

    # pack
    pack = sub.add_parser("pack", help="Pack registry operations")
    pack_sub = pack.add_subparsers(dest="pack_cmd", metavar="SUBCMD")

    pack_list = pack_sub.add_parser("list", help="List registered packs")
    pack_list.add_argument("--type", default=None, help="Filter by pack type")
    pack_list.add_argument("--status", default=None, help="Filter by status")
    pack_list.add_argument(
        "--format", dest="format", default="table",
        choices=_SUPPORTED_FORMATS,
        help="Output format: table (default), json, csv",
    )

    pack_promote = pack_sub.add_parser("promote", help="Promote a CANDIDATE pack")
    pack_promote.add_argument("pack_id", help="Pack ID to promote")
    pack_promote.add_argument("--receipt-id", dest="receipt_id", default=None, help="EvidenceReceipt ID to attach")

    pack_export = pack_sub.add_parser("export", help="Export a PROMOTED pack")
    pack_export.add_argument("pack_id", help="Pack ID to export")
    pack_export.add_argument("--output", default=None, help="Output file path")
    pack_export.add_argument("--domain", default="general", help="Domain label")

    pack_diff = pack_sub.add_parser("diff", help="Diff two pack versions")
    pack_diff.add_argument("old_pack_id", help="Pack ID of the old version")
    pack_diff.add_argument("new_pack_id", help="Pack ID of the new version")
    pack_diff.add_argument(
        "--format", dest="format", default="text",
        choices=["text", "json"],
        help="Output format (default: text)",
    )
    pack_diff.add_argument("--record", action="store_true", default=False,
                           help="Append to changelog after diff")

    pack_changelog = pack_sub.add_parser("changelog", help="Show changelog history for a pack")
    pack_changelog.add_argument("pack_id", help="Pack ID")
    pack_changelog.add_argument(
        "--format", dest="format", default="table",
        choices=["table", "json"],
    )

    # watch
    watch_p = sub.add_parser("watch", help="Watch a directory and auto-ingest new files")
    watch_p.add_argument("directory", help="Directory to watch")
    watch_p.add_argument("--run-id", dest="run_id", default=None)
    watch_p.add_argument("--interval", type=int, default=5, help="Poll interval in seconds (default: 5)")

    # stats
    stats_p = sub.add_parser("stats", help="Show pack registry statistics")
    stats_p.add_argument(
        "--format", dest="format", default="table",
        choices=_SUPPORTED_FORMATS,
        help="Output format: table (default), json, csv",
    )

    # tui
    tui_p = sub.add_parser("tui", help="Launch interactive TUI dashboard")
    tui_p.add_argument("--refresh", type=float, default=2.0,
                       help="Refresh interval in seconds (default: 2.0)")

    # serve
    serve_p = sub.add_parser("serve", help="Start the pack reviewer web server")
    serve_p.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    serve_p.add_argument("--port", type=int, default=8742, help="Bind port (default: 8742)")

    # version
    sub.add_parser("version", help="Print DanteHarvest version")

    # status
    status_p = sub.add_parser("status", help="Show DanteHarvest system status")
    status_p.add_argument(
        "--output-format", dest="output_format", default="text",
        choices=["text", "json", "table"],
        help="Output format: text (default), json, table",
    )

    # validate
    validate_p = sub.add_parser("validate", help="Validate a harvest configuration file")
    validate_p.add_argument("config_path", help="Path to the config JSON file")
    validate_p.add_argument(
        "--output-format", dest="output_format", default="text",
        choices=["text", "json"],
        help="Output format: text (default), json",
    )

    # replay
    replay = sub.add_parser("replay", help="Session replay operations")
    replay_sub = replay.add_subparsers(dest="replay_cmd", metavar="SUBCMD")

    replay_diff_p = replay_sub.add_parser("diff", help="Side-by-side diff of two replay reports")
    replay_diff_p.add_argument("report_a", help="Path to first ReplayReport JSON")
    replay_diff_p.add_argument("report_b", help="Path to second ReplayReport JSON")
    replay_diff_p.add_argument(
        "--format", dest="format", default="text",
        choices=["text", "json"],
    )

    # key
    key = sub.add_parser("key", help="Encryption key management")
    key_sub = key.add_subparsers(dest="key_cmd", metavar="SUBCMD")

    key_rotate = key_sub.add_parser("rotate", help="Rotate encryption key and re-encrypt artifacts")
    key_rotate.add_argument("--old-key", dest="old_key", default=None,
                            help="Current passphrase (default: HARVEST_ENCRYPT_KEY env var)")
    key_rotate.add_argument("--new-key", dest="new_key", default=None,
                            help="New passphrase (default: HARVEST_ENCRYPT_KEY_NEW env var)")
    key_rotate.add_argument("--hint", dest="hint", default="",
                            help="Non-secret label for the new key version")

    # trace
    trace = sub.add_parser("trace", help="Inspect SessionTracer JSONL trace files")
    trace_sub = trace.add_subparsers(dest="trace_cmd", metavar="SUBCMD")

    trace_view = trace_sub.add_parser("view", help="Render a trace file in human-readable format")
    trace_view.add_argument("trace_file", help="Path to .jsonl trace file")
    trace_view.add_argument(
        "--format", dest="format", default="table",
        choices=["table", "json"],
        help="Output format (default: table)",
    )
    trace_view.add_argument(
        "--filter", dest="filter", default=None,
        help="Filter events by name substring",
    )

    trace_list = trace_sub.add_parser("list", help="List available trace files")
    trace_list.add_argument(
        "--trace-dir", dest="trace_dir", default="storage/traces",
        help="Trace directory (default: storage/traces)",
    )
    trace_list.add_argument(
        "--format", dest="format", default="table",
        choices=["table", "json"],
    )
    trace_list.add_argument("--limit", type=int, default=20)

    trace_diff = trace_sub.add_parser("diff", help="Side-by-side diff of two trace files")
    trace_diff.add_argument("trace_a", help="First trace .jsonl file")
    trace_diff.add_argument("trace_b", help="Second trace .jsonl file")

    # verify-chain
    vc_p = sub.add_parser("verify-chain", help="Verify the Merkle-sealed evidence chain")
    vc_p.add_argument("chain_path", help="Path to the chain .jsonl file")
    vc_p.add_argument("--run-id", dest="run_id", default=None,
                      help="Run ID (defaults to filename stem)")
    vc_p.add_argument("--seal", action="store_true", default=False,
                      help="Seal the chain if not yet sealed")

    # gc
    gc_p = sub.add_parser("gc", help="Garbage-collect expired artifacts")
    gc_p.add_argument("--dry-run", dest="dry_run", action="store_true", default=False,
                      help="Print what would be deleted without deleting")

    # schedule
    sched = sub.add_parser("schedule", help="Manage recurring harvest jobs")
    sched_sub = sched.add_subparsers(dest="sched_cmd", metavar="SUBCOMMAND")

    sched_add = sched_sub.add_parser("add", help="Add a recurring schedule")
    sched_add.add_argument("schedule_command", choices=["crawl", "ingest"],
                           help="Command to schedule")
    sched_add.add_argument("--cron", required=True, help='Cron expression (5 fields, e.g. "0 * * * *")')
    sched_add.add_argument("--url", default=None, help="URL for crawl schedules")
    sched_add.add_argument("--path", default=None, help="Path for ingest schedules")
    sched_add.add_argument("--depth", type=int, default=None, help="Max crawl depth")
    sched_add.add_argument("--pages", type=int, default=None, help="Max pages per crawl")

    sched_list = sched_sub.add_parser("list", help="List all scheduled jobs")
    sched_list.add_argument("--status", default=None, choices=["active", "paused", "deleted"])

    sched_rm = sched_sub.add_parser("remove", help="Remove a scheduled job")
    sched_rm.add_argument("schedule_id", help="Schedule ID to remove")

    sched_run = sched_sub.add_parser("run-now", help="Run a scheduled job immediately")
    sched_run.add_argument("schedule_id", help="Schedule ID to run now")

    return parser


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def main(argv=None) -> int:
    try:
        import argcomplete
        parser = build_parser()
        argcomplete.autocomplete(parser)
    except ImportError:
        parser = build_parser()

    args = parser.parse_args(argv)

    if args.command == "ingest":
        if args.ingest_type == "file":
            return asyncio.run(cmd_ingest_file(args))
        elif args.ingest_type == "url":
            return asyncio.run(cmd_ingest_url(args))
        elif args.ingest_type == "batch":
            return asyncio.run(cmd_ingest_batch(args))
        elif args.ingest_type == "github":
            return cmd_ingest_github(args)
        elif args.ingest_type == "notion":
            return cmd_ingest_notion(args)
        elif args.ingest_type == "s3":
            return cmd_ingest_s3(args)
        else:
            print("error: specify 'harvest ingest file|url|batch|github|notion|s3'", file=sys.stderr)
            return 1
    elif args.command == "watch":
        return asyncio.run(cmd_watch(args))
    elif args.command == "crawl":
        return asyncio.run(cmd_crawl(args))
    elif args.command == "run":
        if args.run_cmd == "create":
            return asyncio.run(cmd_run_create(args))
        elif args.run_cmd == "status":
            return cmd_run_status(args)
        else:
            print("error: specify 'harvest run create' or 'harvest run status <id>'",
                  file=sys.stderr)
            return 1
    elif args.command == "observe":
        if args.observe_cmd == "browser":
            return asyncio.run(cmd_observe_browser(args))
        elif args.observe_cmd == "daemon":
            return asyncio.run(cmd_observe_daemon(args))
        else:
            print("error: specify 'harvest observe browser|daemon'", file=sys.stderr)
            return 1
    elif args.command == "pack":
        if args.pack_cmd == "list":
            return cmd_pack_list(args)
        elif args.pack_cmd == "promote":
            return cmd_pack_promote(args)
        elif args.pack_cmd == "export":
            return cmd_pack_export(args)
        elif args.pack_cmd == "diff":
            return cmd_pack_diff(args)
        elif args.pack_cmd == "changelog":
            return cmd_pack_changelog(args)
        else:
            print("error: unknown pack subcommand", file=sys.stderr)
            return 1
    elif args.command == "stats":
        return cmd_registry_stats(args)
    elif args.command == "tui":
        return cmd_tui(args)
    elif args.command == "serve":
        return cmd_serve(args)
    elif args.command == "schedule":
        if args.sched_cmd == "add":
            return cmd_schedule_add(args)
        elif args.sched_cmd == "list":
            return cmd_schedule_list(args)
        elif args.sched_cmd == "remove":
            return cmd_schedule_remove(args)
        elif args.sched_cmd == "run-now":
            return cmd_schedule_run_now(args)
        else:
            print("error: specify 'harvest schedule add|list|remove|run-now'", file=sys.stderr)
            return 1
    elif args.command == "replay":
        if args.replay_cmd == "diff":
            return cmd_replay_diff(args)
        else:
            print("error: specify 'harvest replay diff'", file=sys.stderr)
            return 1
    elif args.command == "key":
        if args.key_cmd == "rotate":
            return cmd_key_rotate(args)
        else:
            print("error: specify 'harvest key rotate'", file=sys.stderr)
            return 1
    elif args.command == "trace":
        if args.trace_cmd == "view":
            return cmd_trace_view(args)
        elif args.trace_cmd == "list":
            return cmd_trace_list(args)
        elif args.trace_cmd == "diff":
            return cmd_trace_diff(args)
        else:
            print("error: specify 'harvest trace view|list|diff'", file=sys.stderr)
            return 1
    elif args.command == "verify-chain":
        return cmd_verify_chain(args)
    elif args.command == "gc":
        return cmd_gc(args)
    elif args.command == "version":
        return cmd_version(args)
    elif args.command == "status":
        return cmd_status(args)
    elif args.command == "validate":
        return cmd_validate(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
