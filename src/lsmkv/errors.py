"""Public storage errors."""


class CorruptionError(ValueError):
    """Stored bytes violate the supported format."""


class MissingManifestError(CorruptionError):
    """Existing directory is not a recognizable interrupted initialization."""


class UnsupportedFormatError(ValueError):
    """Valid metadata requires a milestone not implemented yet."""


class FailedStateError(RuntimeError):
    """The engine experienced a storage error and must be reopened."""


class ClosedError(RuntimeError):
    """The engine has been closed."""


class DurabilityUnavailableError(OSError):
    """A required storage durability or locking primitive is unavailable."""
