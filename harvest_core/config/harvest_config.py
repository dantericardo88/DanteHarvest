"""
HarvestConfig — .danteharvestrc TOML configuration loader with profiles.

Wave 3c: config_and_profiles — structured project config with profile switching.

Loads configuration from (in priority order):
1. Explicit path passed to HarvestConfig(path=...)
2. .danteharvestrc in current directory
3. ~/.danteharvestrc in home directory
4. Built-in defaults

Profiles: named sections in the TOML file (e.g. [profiles.production])
that can be activated via HarvestConfig(profile="production") or
HARVEST_PROFILE env var.

Constitutional guarantees:
- Fail-closed: missing file → defaults (never raises)
- Local-first: reads only from disk, never from network
- Zero-ambiguity: get() always returns a typed value (never KeyError)
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Dict, List, Optional


_DEFAULT_CONFIG: Dict[str, Any] = {
    "storage_root": "storage",
    "log_level": "INFO",
    "max_crawl_pages": 100,
    "use_js_rendering": False,
    "spa_mode": False,
    "extra_wait_ms": 0,
    "wait_until": "networkidle",
    "respect_robots": True,
    "use_sitemap": True,
    "chain_enabled": True,
    "dedup_threshold": 0.85,
    "chunk_size": 512,
    "chunk_strategy": "recursive",
    "embedding_model": None,
    "anthropic_model": "claude-haiku-4-5-20251001",
    "retention_days": 90,
    "webhook_secret": "harvest-webhook",
    "crawl_rps": 1.0,
    "crawl_timeout_s": 30.0,
    "crawl_max_depth": 3,
    "encrypt_at_rest": True,
    "key_rotation_days": 90,
    "audit_log_chain": True,
    "tui_refresh_interval": 2.0,
    "ocr_enabled": True,
    "ocr_engine": "tesseract",
    "observation_interval_s": 5.0,
}

# Built-in profiles: activate with HARVEST_PROFILE=dev|prod|staging
_BUILT_IN_PROFILES: Dict[str, Dict[str, Any]] = {
    "dev": {
        "log_level": "DEBUG",
        "crawl_rps": 5.0,
        "crawl_max_depth": 10,
        "encrypt_at_rest": False,
        "max_crawl_pages": 1000,
        "use_js_rendering": True,
    },
    "prod": {
        "log_level": "WARNING",
        "crawl_rps": 0.5,
        "crawl_max_depth": 3,
        "encrypt_at_rest": True,
        "key_rotation_days": 30,
        "audit_log_chain": True,
        "respect_robots": True,
    },
    "staging": {
        "log_level": "INFO",
        "crawl_rps": 2.0,
        "encrypt_at_rest": True,
        "max_crawl_pages": 500,
    },
}

# Environment variable → config key mapping
# Format: env_var → (config_key, type)
_ENV_OVERRIDES: Dict[str, tuple] = {
    "HARVEST_STORAGE_ROOT": ("storage_root", str),
    "HARVEST_LOG_LEVEL": ("log_level", str),
    "HARVEST_CRAWL_RPS": ("crawl_rps", float),
    "HARVEST_CRAWL_DEPTH": ("crawl_max_depth", int),
    "HARVEST_CRAWL_TIMEOUT": ("crawl_timeout_s", float),
    "HARVEST_CRAWL_PAGES": ("max_crawl_pages", int),
    "HARVEST_ENCRYPT": ("encrypt_at_rest", bool),
    "HARVEST_KEY_ROTATION_DAYS": ("key_rotation_days", int),
    "HARVEST_RETENTION_DAYS": ("retention_days", int),
    "HARVEST_CHUNK_SIZE": ("chunk_size", int),
    "HARVEST_CHUNK_STRATEGY": ("chunk_strategy", str),
    "HARVEST_OCR_ENABLED": ("ocr_enabled", bool),
    "HARVEST_OCR_ENGINE": ("ocr_engine", str),
    "HARVEST_ANTHROPIC_MODEL": ("anthropic_model", str),
    "HARVEST_AUDIT_CHAIN": ("audit_log_chain", bool),
    "HARVEST_JS_RENDERING": ("use_js_rendering", bool),
    "HARVEST_RESPECT_ROBOTS": ("respect_robots", bool),
    "HARVEST_DEDUP_THRESHOLD": ("dedup_threshold", float),
    "HARVEST_OBSERVATION_INTERVAL": ("observation_interval_s", float),
}

_SEARCH_PATHS = [
    Path.cwd() / ".danteharvestrc",
    Path.cwd() / ".danteharvestrc.toml",
    Path.home() / ".danteharvestrc",
    Path.home() / ".danteharvestrc.toml",
]


def _load_toml(path: Path) -> Dict[str, Any]:
    try:
        try:
            import tomllib  # Python 3.11+
            return tomllib.loads(path.read_text(encoding="utf-8"))
        except ImportError:
            try:
                import tomli  # type: ignore[import]
                return tomli.loads(path.read_text(encoding="utf-8"))
            except ImportError:
                # Manual fallback: parse simple key=value TOML (no arrays/nested)
                return _parse_simple_toml(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _parse_simple_toml(text: str) -> Dict[str, Any]:
    """Minimal TOML parser: handles [section] headers and key = value lines."""
    result: Dict[str, Any] = {}
    current_section: Dict[str, Any] = result
    current_key: List[str] = []

    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("["):
            # Section header like [profiles.prod]
            parts = line.strip("[]").split(".")
            current_section = result
            current_key = []
            for part in parts:
                current_section.setdefault(part, {})
                current_section = current_section[part]
                current_key.append(part)
        elif "=" in line:
            k, _, v = line.partition("=")
            k = k.strip()
            v = v.strip().strip('"').strip("'")
            # Coerce obvious types
            if v.lower() == "true":
                v = True  # type: ignore[assignment]
            elif v.lower() == "false":
                v = False  # type: ignore[assignment]
            else:
                try:
                    v = int(v)  # type: ignore[assignment]
                except ValueError:
                    try:
                        v = float(v)  # type: ignore[assignment]
                    except ValueError:
                        pass
            current_section[k] = v
    return result


class HarvestConfig:
    """
    Unified project configuration reader.

    Usage:
        cfg = HarvestConfig()
        storage = cfg.get("storage_root")          # "storage"
        model = cfg.get("anthropic_model")         # "claude-haiku-..."

        # With profile
        cfg = HarvestConfig(profile="production")
        model = cfg.get("anthropic_model")         # from [profiles.production]
    """

    def __init__(
        self,
        path: Optional[str] = None,
        profile: Optional[str] = None,
    ):
        raw = self._load_raw(path)
        self._profile = profile or os.environ.get("HARVEST_PROFILE", "")
        # Merge order: defaults → built-in profile → file base → file profile → env overrides
        self._base = {**_DEFAULT_CONFIG, **{k: v for k, v in raw.items() if k != "profiles"}}
        built_in_profile = _BUILT_IN_PROFILES.get(self._profile, {}) if self._profile else {}
        file_profile = raw.get("profiles", {}).get(self._profile, {}) if self._profile else {}
        env_overrides = self._load_env_overrides()
        self._merged = {**self._base, **built_in_profile, **file_profile, **env_overrides}

    def get(self, key: str, default: Any = None) -> Any:
        """Return config value for *key*, or *default* if not set."""
        return self._merged.get(key, default)

    def __getitem__(self, key: str) -> Any:
        return self._merged[key]

    def __contains__(self, key: str) -> bool:
        return key in self._merged

    def as_dict(self) -> Dict[str, Any]:
        return dict(self._merged)

    def available_profiles(self) -> List[str]:
        built_in = list(_BUILT_IN_PROFILES.keys())
        custom = [k for k in self._raw_profiles.keys() if k not in built_in]
        return built_in + custom

    def _load_env_overrides(self) -> Dict[str, Any]:
        overrides: Dict[str, Any] = {}
        for env_var, (config_key, typ) in _ENV_OVERRIDES.items():
            val = os.environ.get(env_var)
            if val is None:
                continue
            try:
                if typ is bool:
                    overrides[config_key] = val.lower() in ("1", "true", "yes")
                else:
                    overrides[config_key] = typ(val)
            except (ValueError, TypeError):
                pass
        return overrides

    @property
    def active_profile(self) -> str:
        return self._profile or "(default)"

    def _load_raw(self, explicit_path: Optional[str]) -> Dict[str, Any]:
        if explicit_path:
            p = Path(explicit_path)
            if p.exists():
                data = _load_toml(p)
                self._raw_profiles = data.get("profiles", {})
                return data
        for search_path in _SEARCH_PATHS:
            if search_path.exists():
                data = _load_toml(search_path)
                self._raw_profiles = data.get("profiles", {})
                return data
        self._raw_profiles = {}
        return {}


# Module-level singleton for convenience
_instance: Optional[HarvestConfig] = None


def get_config(profile: Optional[str] = None) -> HarvestConfig:
    """Return the global HarvestConfig instance (created on first call)."""
    global _instance
    if _instance is None or (profile and profile != _instance.active_profile):
        _instance = HarvestConfig(profile=profile)
    return _instance


def reset_config() -> None:
    """Reset the global config instance (useful in tests)."""
    global _instance
    _instance = None
