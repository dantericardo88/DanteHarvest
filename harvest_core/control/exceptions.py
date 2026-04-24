"""
Harvest exception hierarchy.

All Harvest errors are subclasses of HarvestError so callers can catch
the base type without enumerating every subclass.
"""


class HarvestError(Exception):
    """Base for all Harvest exceptions."""

    def __init__(self, message: str, context: dict | None = None):
        super().__init__(message)
        self.context = context or {}


class RightsError(HarvestError):
    """Raised when a rights or policy gate blocks an operation."""


class ConstitutionalError(HarvestError):
    """Raised when a hard constitutional rule is violated (e.g. robots.txt)."""


class ChainError(HarvestError):
    """Raised when the evidence chain append or verification fails."""


class StorageError(HarvestError):
    """Raised when artifact storage operations fail."""


class ManifestError(HarvestError):
    """Raised when manifest building or verification fails."""


class PackagingError(HarvestError):
    """Raised when evidence package creation fails."""


class EvaluationError(HarvestError):
    """Raised when replay or promotion gate evaluation fails."""


class NormalizationError(HarvestError):
    """Raised when OCR, transcription, or markdown conversion fails."""


class AcquisitionError(HarvestError):
    """Raised when file, URL, or browser acquisition fails."""
