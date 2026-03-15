class StockQuantError(Exception):
    """Base exception for stock-quant-oop."""


class ConfigurationError(StockQuantError):
    """Raised when configuration is invalid."""


class PipelineError(StockQuantError):
    """Raised when a pipeline fails."""


class SchemaError(StockQuantError):
    """Raised when schema creation or validation fails."""


class RepositoryError(StockQuantError):
    """Raised when database access fails."""


class ProviderError(StockQuantError):
    """Raised when a data provider fails."""


class ServiceError(StockQuantError):
    """Raised when an application service fails."""


class ValidationError(StockQuantError):
    """Raised when validation fails."""
