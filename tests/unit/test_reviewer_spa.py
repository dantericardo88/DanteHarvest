"""
Phase 1 — Reviewer SPA tests.

Verifies:
1. Static build artifact exists (index.html in static/)
2. React SPA source files exist and are well-formed
3. server.py has /api/stats endpoint
4. server.py has /defer endpoint (already verified in phase0, belt-and-suspenders)
"""

from pathlib import Path
import json


STATIC = Path("harvest_ui/reviewer/static")
SPA_SRC = Path("harvest_ui/reviewer/spa/src")


def test_spa_index_html_exists():
    """Built SPA must have index.html."""
    assert (STATIC / "index.html").exists(), "Run: cd harvest_ui/reviewer/spa && npm run build"


def test_spa_assets_exist():
    """Built SPA must have JS and CSS assets."""
    assets = list((STATIC / "assets").glob("index-*.js"))
    assert assets, "No built JS assets found in static/assets/"


def test_spa_source_app_tsx_exists():
    """App.tsx source must exist."""
    assert (SPA_SRC / "App.tsx").exists()


def test_spa_source_pack_list_exists():
    """PackList.tsx must exist."""
    assert (SPA_SRC / "PackList.tsx").exists()


def test_spa_source_pack_card_exists():
    """PackCard.tsx must exist."""
    assert (SPA_SRC / "PackCard.tsx").exists()


def test_spa_source_review_buttons_exists():
    """ReviewButtons.tsx must exist."""
    assert (SPA_SRC / "ReviewButtons.tsx").exists()


def test_spa_source_api_ts_has_all_endpoints():
    """api.ts must export approvePack, rejectPack, deferPack."""
    src = (SPA_SRC / "api.ts").read_text(encoding="utf-8")
    assert "approvePack" in src
    assert "rejectPack" in src
    assert "deferPack" in src
    assert "/approve" in src
    assert "/reject" in src
    assert "/defer" in src


def test_spa_polls_api():
    """usePacks.ts must call setInterval for polling."""
    src = (SPA_SRC / "usePacks.ts").read_text(encoding="utf-8")
    assert "setInterval" in src


def test_server_has_stats_endpoint():
    """server.py must expose /api/stats."""
    server_src = Path("harvest_ui/reviewer/server.py").read_text(encoding="utf-8")
    assert "/api/stats" in server_src
    assert "registry.stats()" in server_src


def test_server_has_defer_endpoint():
    """server.py must have /defer endpoint (regression guard)."""
    server_src = Path("harvest_ui/reviewer/server.py").read_text(encoding="utf-8")
    assert "/defer" in server_src


def test_spa_confidence_badge_source():
    """ConfidenceBadge.tsx must handle all four bands."""
    src = (SPA_SRC / "ConfidenceBadge.tsx").read_text(encoding="utf-8")
    for band in ("GREEN", "YELLOW", "ORANGE", "RED"):
        assert band in src


def test_vite_config_outdir_points_to_static():
    """vite.config.ts must output to ../static."""
    cfg = Path("harvest_ui/reviewer/spa/vite.config.ts").read_text(encoding="utf-8")
    assert "../static" in cfg
