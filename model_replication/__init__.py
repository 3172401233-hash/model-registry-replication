"""Offline model registry replication library.

Replicates versioned model artifacts (params + weight shards + SHA256 manifest)
between two repositories with resumable transfers, byte budgets, bandwidth
limiting, atomic version publishing and rollback.
"""

from .errors import (
    ReplicationError,
    ManifestInconsistentError,
    TransferError,
    ObjectCorruptError,
    RollbackError,
    VersionNotFoundError,
)
from .manifest import Manifest, ObjectEntry
from .repository import Repository
from .transport import Transport, LocalTransport
from .sync import SyncEngine, SyncReport

__all__ = [
    "ReplicationError",
    "ManifestInconsistentError",
    "TransferError",
    "ObjectCorruptError",
    "RollbackError",
    "VersionNotFoundError",
    "Manifest",
    "ObjectEntry",
    "Repository",
    "Transport",
    "LocalTransport",
    "SyncEngine",
    "SyncReport",
]

__version__ = "0.1.0"
