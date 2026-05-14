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
import json
import sys
from pathlib import Path


__version__ = "0.1.0"


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
    for file in files:
        try:
            result = await ingestor.ingest(path=file, run_id=run_id, rights_profile=rights)
            results.append({"file": str(file), "artifact_id": result.artifact_id, "sha256": result.sha256})
        except Exception as e:
            errors.append({"file": str(file), "error": str(e)})

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
    try:
        registry = PackRegistry(root=root)
        entries = registry.list(
            pack_type=getattr(args, "type", None),
            status=getattr(args, "status", None),
        )
        if not entries:
            print("No packs registered.")
            return 0
        for e in entries:
            print(f"  [{e.promotion_status:12s}] {e.pack_type:25s} {e.pack_id}  {e.title}")
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
    try:
        registry = PackRegistry(root=root)
        stats = registry.stats()
        print(json.dumps(stats, indent=2))
        return 0
    except Exception as e:
        print(f"error: {e}", file=sys.stderr)
        return 1


async def cmd_watch(args) -> int:
    """
    Watch a directory for new files and auto-ingest them as they appear.
    Uses watchdog if available; falls back to polling every --interval seconds.
    """
    from harvest_acquire.files.file_ingestor import FileIngestor
    from harvest_core.provenance.chain_writer import ChainWriter
    from harvest_core.rights.rights_model import SourceClass, default_rights_for
    import time

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

    seen: set = set(
        f for f in directory.glob("**/*")
        if f.is_file() and f.suffix.lower() in _INGESTABLE_SUFFIXES
    )
    print(f"watching {directory} (interval={interval}s, {len(seen)} existing files skipped)")

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
    print(f"harvest {__version__}")
    return 0


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
            print(f"  next_run: {datetime.datetime.utcfromtimestamp(entry.next_run).isoformat()}Z")
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

    # pack
    pack = sub.add_parser("pack", help="Pack registry operations")
    pack_sub = pack.add_subparsers(dest="pack_cmd", metavar="SUBCMD")

    pack_list = pack_sub.add_parser("list", help="List registered packs")
    pack_list.add_argument("--type", default=None, help="Filter by pack type")
    pack_list.add_argument("--status", default=None, help="Filter by status")

    pack_promote = pack_sub.add_parser("promote", help="Promote a CANDIDATE pack")
    pack_promote.add_argument("pack_id", help="Pack ID to promote")
    pack_promote.add_argument("--receipt-id", dest="receipt_id", default=None, help="EvidenceReceipt ID to attach")

    pack_export = pack_sub.add_parser("export", help="Export a PROMOTED pack")
    pack_export.add_argument("pack_id", help="Pack ID to export")
    pack_export.add_argument("--output", default=None, help="Output file path")
    pack_export.add_argument("--domain", default="general", help="Domain label")

    # watch
    watch_p = sub.add_parser("watch", help="Watch a directory and auto-ingest new files")
    watch_p.add_argument("directory", help="Directory to watch")
    watch_p.add_argument("--run-id", dest="run_id", default=None)
    watch_p.add_argument("--interval", type=int, default=5, help="Poll interval in seconds (default: 5)")

    # stats
    sub.add_parser("stats", help="Show pack registry statistics")

    # serve
    serve_p = sub.add_parser("serve", help="Start the pack reviewer web server")
    serve_p.add_argument("--host", default="127.0.0.1", help="Bind host (default: 127.0.0.1)")
    serve_p.add_argument("--port", type=int, default=8742, help="Bind port (default: 8742)")

    # version
    sub.add_parser("version", help="Print version")

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
        else:
            print("error: specify 'harvest observe browser <trace>'", file=sys.stderr)
            return 1
    elif args.command == "pack":
        if args.pack_cmd == "list":
            return cmd_pack_list(args)
        elif args.pack_cmd == "promote":
            return cmd_pack_promote(args)
        elif args.pack_cmd == "export":
            return cmd_pack_export(args)
        else:
            print("error: unknown pack subcommand", file=sys.stderr)
            return 1
    elif args.command == "stats":
        return cmd_registry_stats(args)
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
    elif args.command == "verify-chain":
        return cmd_verify_chain(args)
    elif args.command == "gc":
        return cmd_gc(args)
    elif args.command == "version":
        return cmd_version(args)
    else:
        parser.print_help()
        return 0


if __name__ == "__main__":
    sys.exit(main())
