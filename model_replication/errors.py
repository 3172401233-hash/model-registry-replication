"""Exception hierarchy for the replication library."""


class ReplicationError(Exception):
    """Base class for all replication errors."""


class ManifestInconsistentError(ReplicationError):
    """The manifest does not match the objects available in the source."""


class TransferError(ReplicationError):
    """A transport-level failure interrupted an object transfer."""


class ObjectCorruptError(ReplicationError):
    """A transferred or stored object failed SHA256 verification."""


class RollbackError(ReplicationError):
    """Rollback was requested but no previous version is available."""


class VersionNotFoundError(ReplicationError):
    """The requested version does not exist in the repository."""
