"""Transport abstraction.

The sync engine only ever talks to a :class:`Transport`. The default
:class:`LocalTransport` reads objects from another on-disk repository,
which keeps the library fully testable offline. A production deployment
would implement the same two-method interface over SSH, HTTP or an
object-storage client without touching the sync engine.
"""

import os
from typing import TYPE_CHECKING

from .errors import TransferError

if TYPE_CHECKING:  # pragma: no cover - typing only
    from .repository import Repository


class Transport:
    """Interface between the sync engine and a remote repository."""

    def object_size(self, source: "Repository", sha256: str) -> int:
        """Return the size of an object in the source repository."""
        raise NotImplementedError

    def fetch(self, source: "Repository", sha256: str, offset: int,
              max_bytes: int) -> bytes:
        """Read up to ``max_bytes`` of object data starting at ``offset``."""
        raise NotImplementedError


class LocalTransport(Transport):
    """Transport backed by another repository on the local filesystem."""

    def object_size(self, source: "Repository", sha256: str) -> int:
        path = source.object_path(sha256)
        if not os.path.isfile(path):
            raise TransferError("object %s missing from source" % sha256)
        return os.path.getsize(path)

    def fetch(self, source: "Repository", sha256: str, offset: int,
              max_bytes: int) -> bytes:
        path = source.object_path(sha256)
        if not os.path.isfile(path):
            raise TransferError("object %s missing from source" % sha256)
        with open(path, "rb") as handle:
            handle.seek(offset)
            return handle.read(max_bytes)
