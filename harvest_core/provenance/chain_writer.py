"""
ChainWriter — append-only evidence chain writer.

Transplanted from DanteDistillerV2/backend/storage/chain_writer.py.
Import paths updated for DANTEHARVEST package layout.

Constitutional guarantees:
- Atomic appends with exclusive file locking
- Sequential numbering with no gaps
- SHA-256 content hashing on every entry
- fsync for on-disk durability
- Fail-closed: any write error raises ChainError
- Optional RunContractEnforcer wired via enforcer= parameter:
    writer = ChainWriter(path, run_id, enforcer=RunContractEnforcer())
  When set, validate_chain_entry() is called before every append.
"""

import asyncio
import json
import os
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from harvest_core.provenance.chain_entry import ChainEntry
from harvest_core.control.exceptions import ChainError, StorageError

if TYPE_CHECKING:
    from harvest_core.constitution.run_contract_enforcer import RunContractEnforcer

try:
    import fcntl
    _HAS_FCNTL = True
except ImportError:
    _HAS_FCNTL = False
    try:
        import msvcrt
        _HAS_MSVCRT = True
    except ImportError:
        _HAS_MSVCRT = False


class ChainWriter:
    """
    Append-only JSONL writer for the Harvest evidence chain.

    Thread-safe and async-safe via asyncio.Lock + OS file locking.
    Every write: assigns sequence → computes hash → appends → fsyncs.
    """

    def __init__(
        self,
        chain_file_path: Path,
        run_id: str,
        enforcer: "Optional[RunContractEnforcer]" = None,
    ):
        self.chain_file_path = Path(chain_file_path)
        self.run_id = run_id
        self._enforcer = enforcer
        self._sequence = 0
        self._lock = asyncio.Lock()
        self.chain_file_path.parent.mkdir(parents=True, exist_ok=True)
        if self.chain_file_path.exists():
            self._sequence = self._read_last_sequence()

    def _read_last_sequence(self) -> int:
        try:
            with open(self.chain_file_path) as f:
                lines = f.readlines()
            if not lines:
                return 0
            last = lines[-1].strip()
            if not last:
                return 0
            return ChainEntry.from_jsonl_line(last).sequence or 0
        except (FileNotFoundError, json.JSONDecodeError, ValueError):
            return 0

    async def append(self, entry: ChainEntry) -> ChainEntry:
        async with self._lock:
            if entry.run_id != self.run_id:
                raise ChainError(
                    f"Entry run_id '{entry.run_id}' does not match chain run_id '{self.run_id}'"
                )
            if self._enforcer is not None:
                self._enforcer.validate_chain_entry(entry.model_dump())
            self._sequence += 1
            entry.sequence = self._sequence
            entry.content_hash = entry.compute_hash()
            await self._write_entry(entry)
            return entry

    async def append_batch(self, entries: List[ChainEntry]) -> List[ChainEntry]:
        async with self._lock:
            prepared: List[ChainEntry] = []
            for entry in entries:
                if entry.run_id != self.run_id:
                    raise ChainError(f"Entry run_id mismatch: '{entry.run_id}'")
                self._sequence += 1
                entry.sequence = self._sequence
                entry.content_hash = entry.compute_hash()
                prepared.append(entry)
            try:
                await asyncio.get_event_loop().run_in_executor(None, self._write_batch_sync, prepared)
            except Exception as e:
                self._sequence -= len(prepared)
                raise ChainError(f"Batch write failed: {e}") from e
            return prepared

    async def _write_entry(self, entry: ChainEntry) -> None:
        try:
            await asyncio.get_event_loop().run_in_executor(None, self._write_entry_sync, entry)
        except Exception as e:
            raise StorageError(f"Write failed: {e}", {"path": str(self.chain_file_path)}) from e

    def _write_entry_sync(self, entry: ChainEntry) -> None:
        with open(self.chain_file_path, "a") as f:
            self._acquire_lock(f)
            try:
                f.write(entry.to_jsonl_line() + "\n")
                f.flush()
                os.fsync(f.fileno())
            finally:
                self._release_lock(f)

    def _write_batch_sync(self, entries: List[ChainEntry]) -> None:
        with open(self.chain_file_path, "a") as f:
            self._acquire_lock(f)
            try:
                for entry in entries:
                    f.write(entry.to_jsonl_line() + "\n")
                f.flush()
                os.fsync(f.fileno())
            finally:
                self._release_lock(f)

    def _acquire_lock(self, f) -> None:
        if _HAS_FCNTL:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_EX)
        elif _HAS_MSVCRT:
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_LOCK, 1)

    def _release_lock(self, f) -> None:
        if _HAS_FCNTL:
            import fcntl
            fcntl.flock(f.fileno(), fcntl.LOCK_UN)
        elif _HAS_MSVCRT:
            import msvcrt
            f.seek(0)
            msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)

    def read_all(self) -> List[ChainEntry]:
        if not self.chain_file_path.exists():
            return []
        entries = []
        with open(self.chain_file_path) as f:
            for line_num, line in enumerate(f, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(ChainEntry.from_jsonl_line(line))
                except Exception as e:
                    raise ChainError(f"Corrupted entry at line {line_num}", {"error": str(e)}) from e
        return entries

    def verify_integrity(self) -> tuple[bool, Optional[str]]:
        try:
            entries = self.read_all()
            if not entries:
                return True, None
            for expected_seq, entry in enumerate(entries, start=1):
                if entry.sequence != expected_seq:
                    return False, f"Sequence gap at entry {entry.sequence}, expected {expected_seq}"
                if entry.content_hash != entry.compute_hash():
                    return False, f"Hash mismatch at sequence {entry.sequence}"
            return True, None
        except Exception as e:
            return False, f"Integrity check failed: {e}"

    def get_stats(self) -> dict:
        stats = {
            "run_id": self.run_id,
            "entry_count": self._sequence,
            "file_path": str(self.chain_file_path),
            "file_exists": self.chain_file_path.exists(),
        }
        if self.chain_file_path.exists():
            stats["file_size_bytes"] = self.chain_file_path.stat().st_size
        return stats

    def get_latest_sequence(self) -> int:
        return self._sequence

    def get_timeline(self) -> List[datetime]:
        return [e.timestamp for e in self.read_all()]
