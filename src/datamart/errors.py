"""Application failures shared by quality and database adapters."""


class PipelineQualityError(ValueError):
    """An invariant or a required quality check failed; the whole batch rolls back."""

    def __init__(self, message, reports=None):
        super().__init__(message)
        self.reports = reports or []
