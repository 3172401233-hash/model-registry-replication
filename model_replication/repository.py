"""On-disk repository layout shared by source and target.

Layout::

    <root>/
        objects/<sha256>            content-addressed object store
        versions/<version>/manifest.json
        current                     pointer to the readable version
        history.json                publish order, used for rollback
        incoming/                   staging area for partial transfers
            state.json              resume state (target side only)
            <sha256>.part           partially received objects

All pointer/manifest updates go through atomic rename so a crash never
leaves a half-written file visible to readers.
"""

import hashlib
import json
import os
from typing import List, Optional, Set

from .errors import VersionNotFoundError
from .manifest import Manifest

_HASH_CHUNK = 1024 * 1024


def hash_file(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as handle:
        while True:
            block = handle.read(_HASH_CHUNK)
            if not block:
                break
            digest.update(block)
    return digest.hexdigest()


def _atomic_write_bytes(path: str, data: bytes) -> None:
    tmp_path = path + ".tmp.%d" % os.getpid()
    with open(tmp_path, "wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(tmp_path, path)


class Repository:
    """A model registry rooted at a local directory."""

    def __init__(self, root: str) -> None:
        self.root = os.path.abspath(root)
        for sub in ("objects", "versions", "incoming"):
            os.makedirs(os.path.join(self.root, sub), exist_ok=True)

    # -- paths ---------------------------------------------------------

    def object_path(self, sha256: str) -> str:
        return os.path.join(self.root, "objects", sha256)

    def version_dir(self, version: str) -> str:
        return os.path.join(self.root, "versions", version)

    def manifest_path(self, version: str) -> str:
        return os.path.join(self.version_dir(version), "manifest.json")

    @property
    def current_path(self) -> str:
        return os.path.join(self.root, "current")

    @property
    def history_path(self) -> str:
        return os.path.join(self.root, "history.json")

    @property
    def incoming_dir(self) -> str:
        return os.path.join(self.root, "incoming")

    @property
    def state_path(self) -> str:
        return os.path.join(self.incoming_dir, "state.json")

    def staging_path(self, sha256: str) -> str:
        return os.path.join(self.incoming_dir, sha256 + ".part")

    # -- objects -------------------------------------------------------

    def has_object(self, sha256: str) -> bool:
        return os.path.isfile(self.object_path(sha256))

    def verify_object(self, sha256: str) -> bool:
        """True when the object exists and its content matches its digest."""
        path = self.object_path(sha256)
        if not os.path.isfile(path):
            return False
        return hash_file(path) == sha256

    def store_object(self, sha256: str, staging_path: str) -> None:
        """Atomically move a fully received staging file into the store."""
        os.replace(staging_path, self.object_path(sha256))

    # -- manifests and versions ---------------------------------------

    def load_manifest(self, version: str) -> Manifest:
        path = self.manifest_path(version)
        if not os.path.isfile(path):
            raise VersionNotFoundError(
                "version %r not found in %s" % (version, self.root)
            )
        with open(path, "r", encoding="utf-8") as handle:
            return Manifest.from_json(handle.read())

    def save_manifest(self, manifest: Manifest) -> None:
        os.makedirs(self.version_dir(manifest.version), exist_ok=True)
        _atomic_write_bytes(
            self.manifest_path(manifest.version),
            manifest.to_json().encode("utf-8"),
        )

    def list_versions(self) -> List[str]:
        base = os.path.join(self.root, "versions")
        if not os.path.isdir(base):
            return []
        return sorted(
            name
            for name in os.listdir(base)
            if os.path.isfile(self.manifest_path(name))
        )

    def delete_version_files(self, version: str) -> None:
        path = self.manifest_path(version)
        if os.path.isfile(path):
            os.unlink(path)
        try:
            os.rmdir(self.version_dir(version))
        except OSError:
            pass

    # -- version pointer -----------------------------------------------

    def read_current(self) -> Optional[str]:
        if not os.path.isfile(self.current_path):
            return None
        with open(self.current_path, "r", encoding="utf-8") as handle:
            value = handle.read().strip()
        return value or None

    def write_current(self, version: str) -> None:
        _atomic_write_bytes(self.current_path, (version + "\n").encode("utf-8"))

    def read_history(self) -> List[str]:
        if not os.path.isfile(self.history_path):
            return []
        with open(self.history_path, "r", encoding="utf-8") as handle:
            data = json.load(handle)
        return [str(item) for item in data]

    def write_history(self, history: List[str]) -> None:
        _atomic_write_bytes(
            self.history_path, json.dumps(history, indent=2).encode("utf-8")
        )

    # -- completeness ----------------------------------------------------

    def is_version_complete(self, version: str) -> bool:
        """True when every manifest object is present and hash-correct."""
        try:
            manifest = self.load_manifest(version)
        except VersionNotFoundError:
            return False
        return all(self.verify_object(obj.sha256) for obj in manifest.objects)

    def referenced_digests(self) -> Set[str]:
        """Digests referenced by any stored manifest (used by GC)."""
        digests = set()
        for version in self.list_versions():
            manifest = self.load_manifest(version)
            digests.update(obj.sha256 for obj in manifest.objects)
        return digests

    def collect_garbage(self) -> List[str]:
        """Delete objects not referenced by any manifest. Returns digests."""
        keep = self.referenced_digests()
        removed = []
        objects_dir = os.path.join(self.root, "objects")
        for name in sorted(os.listdir(objects_dir)):
            if name not in keep:
                os.unlink(os.path.join(objects_dir, name))
                removed.append(name)
        return removed
