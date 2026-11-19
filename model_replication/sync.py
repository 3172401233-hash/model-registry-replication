"""Sync engine: resumable, budgeted, atomic version replication.

The engine copies the objects of one version from a source repository to a
target repository. Objects that already exist and verify correctly are
skipped; missing or corrupt objects are (re)transferred in chunks. A byte
budget caps each run; progress is persisted after every chunk so the next
run resumes exactly where the previous one stopped. The version pointer is
switched atomically only after every object passed SHA256 verification, so
readers never observe a half-published version.
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Callable, Dict, List, Optional

from .errors import (
    ManifestInconsistentError,
    ObjectCorruptError,
    ReplicationError,
    RollbackError,
    TransferError,
)
from .manifest import Manifest, ObjectEntry
from .repository import Repository, _atomic_write_bytes, hash_file
from .transport import LocalTransport, Transport

DEFAULT_CHUNK_SIZE = 256 * 1024


@dataclass
class SyncReport:
    """Outcome of a single :meth:`SyncEngine.sync_version` run."""

    version: str
    completed: bool
    transferred_bytes: int
    objects_transferred: int
    objects_skipped: int
    resumed: bool


class SyncEngine:
    """Replicates versions from a source repository to a target repository."""

    def __init__(
        self,
        source: Repository,
        target: Repository,
        transport: Optional[Transport] = None,
        budget_bytes: Optional[int] = None,
        bandwidth_bps: Optional[float] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        max_retries: int = 3,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        if chunk_size <= 0:
            raise ValueError("chunk_size must be positive")
        self.source = source
        self.target = target
        self.transport = transport or LocalTransport()
        self.budget_bytes = budget_bytes
        self.bandwidth_bps = bandwidth_bps
        self.chunk_size = chunk_size
        self.max_retries = max_retries
        self._clock = clock
        self._sleep = sleep
        self._spent = 0
        self._schedule = 0.0

    # -- public API ------------------------------------------------------

    def sync_version(self, version: str) -> SyncReport:
        """Transfer one version; may stop early when the budget runs out."""
        manifest = self.source.load_manifest(version)
        self._check_manifest(manifest)

        state = self._load_state(version)
        resumed = any(
            entry.get("received", 0) > 0 for entry in state["objects"].values()
        )
        self._spent = 0
        transferred = 0
        skipped = 0

        for obj in manifest.objects:
            if self.target.verify_object(obj.sha256):
                skipped += 1
                state["objects"].pop(obj.sha256, None)
                continue
            done = self._transfer_object(obj, state)
            if not done:
                self._save_state(version, state)
                return SyncReport(
                    version=version,
                    completed=False,
                    transferred_bytes=self._spent,
                    objects_transferred=transferred,
                    objects_skipped=skipped,
                    resumed=resumed,
                )
            transferred += 1
            state["objects"].pop(obj.sha256, None)
            self._save_state(version, state)

        self._publish(manifest)
        self._clear_state()
        return SyncReport(
            version=version,
            completed=True,
            transferred_bytes=self._spent,
            objects_transferred=transferred,
            objects_skipped=skipped,
            resumed=resumed,
        )

    def rollback(self) -> str:
        """Point the target back at the previously published version."""
        history = self.target.read_history()
        current = self.target.read_current()
        if current is None or current not in history:
            raise RollbackError("no published version to roll back from")
        index = history.index(current)
        if index == 0:
            raise RollbackError("no earlier version available for rollback")
        previous = history[index - 1]
        if not self.target.is_version_complete(previous):
            raise RollbackError(
                "previous version %r is incomplete, cannot roll back" % previous
            )
        self.target.write_current(previous)
        self.target.write_history(history[:index])
        return previous

    def delete_version(self, version: str) -> List[str]:
        """Delete an old version and garbage-collect unreferenced objects."""
        if version == self.target.read_current():
            raise ReplicationError(
                "cannot delete the currently published version %r" % version
            )
        if version not in self.target.list_versions():
            raise ReplicationError("version %r not found on target" % version)
        self.target.delete_version_files(version)
        history = [item for item in self.target.read_history() if item != version]
        self.target.write_history(history)
        return self.target.collect_garbage()

    # -- manifest validation ----------------------------------------------

    def _check_manifest(self, manifest: Manifest) -> None:
        problems = manifest.validate()
        if problems:
            raise ManifestInconsistentError(
                "manifest for %r is invalid: %s"
                % (manifest.version, "; ".join(problems))
            )
        for obj in manifest.objects:
            size = self.transport.object_size(self.source, obj.sha256)
            if size != obj.size:
                raise ManifestInconsistentError(
                    "object %s size mismatch: manifest says %d, source has %d"
                    % (obj.name, obj.size, size)
                )
            if not self.source.verify_object(obj.sha256):
                raise ManifestInconsistentError(
                    "object %s (%s) missing or corrupt in source"
                    % (obj.name, obj.sha256)
                )

    # -- transfer -----------------------------------------------------------

    def _budget_remaining(self) -> Optional[int]:
        if self.budget_bytes is None:
            return None
        return self.budget_bytes - self._spent

    def _transfer_object(self, obj: ObjectEntry, state: Dict) -> bool:
        """Transfer one object. Returns False when the budget ran out."""
        stale = self.target.object_path(obj.sha256)
        if os.path.isfile(stale):
            os.unlink(stale)

        staging = self.target.staging_path(obj.sha256)
        entry = state["objects"].get(obj.sha256, {})
        received = entry.get("received", 0)
        if not os.path.isfile(staging) or os.path.getsize(staging) != received:
            received = 0
        if received > obj.size:
            received = 0

        attempts = 0
        while True:
            with open(staging, "ab" if received else "wb") as out:
                while received < obj.size:
                    remaining = self._budget_remaining()
                    if remaining is not None and remaining <= 0:
                        state["objects"][obj.sha256] = {"received": received}
                        return False
                    want = min(self.chunk_size, obj.size - received)
                    if remaining is not None:
                        want = min(want, remaining)
                    chunk = self.transport.fetch(
                        self.source, obj.sha256, received, want
                    )
                    if not chunk:
                        raise TransferError(
                            "unexpected end of stream for object %s" % obj.name
                        )
                    out.write(chunk)
                    received += len(chunk)
                    self._spent += len(chunk)
                    self._throttle(len(chunk))
                    state["objects"][obj.sha256] = {"received": received}
                    self._save_state(state["version"], state)
            if hash_file(staging) == obj.sha256:
                break
            attempts += 1
            if attempts > self.max_retries:
                if os.path.isfile(staging):
                    os.unlink(staging)
                raise ObjectCorruptError(
                    "object %s failed SHA256 verification %d times"
                    % (obj.name, attempts)
                )
            received = 0
            state["objects"][obj.sha256] = {"received": 0}

        self.target.store_object(obj.sha256, staging)
        return True

    def _throttle(self, nbytes: int) -> None:
        if not self.bandwidth_bps:
            return
        now = self._clock()
        if self._schedule < now:
            self._schedule = now
        self._schedule += nbytes / float(self.bandwidth_bps)
        delay = self._schedule - now
        if delay > 0:
            self._sleep(delay)

    # -- publishing ---------------------------------------------------------

    def _publish(self, manifest: Manifest) -> None:
        for obj in manifest.objects:
            if not self.target.verify_object(obj.sha256):
                raise ObjectCorruptError(
                    "object %s missing or corrupt before publish" % obj.name
                )
        self.target.save_manifest(manifest)
        history = self.target.read_history()
        if manifest.version in history:
            history.remove(manifest.version)
        history.append(manifest.version)
        self.target.write_history(history)
        self.target.write_current(manifest.version)

    # -- resume state -------------------------------------------------------

    def _load_state(self, version: str) -> Dict:
        path = self.target.state_path
        if os.path.isfile(path):
            try:
                with open(path, "r", encoding="utf-8") as handle:
                    state = json.load(handle)
                if state.get("version") == version:
                    state.setdefault("objects", {})
                    return state
            except (ValueError, OSError):
                pass
        return {"version": version, "objects": {}}

    def _save_state(self, version: str, state: Dict) -> None:
        state["version"] = version
        payload = json.dumps(state, indent=2, sort_keys=True).encode("utf-8")
        _atomic_write_bytes(self.target.state_path, payload)

    def _clear_state(self) -> None:
        path = self.target.state_path
        if os.path.isfile(path):
            os.unlink(path)
