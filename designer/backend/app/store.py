"""Compatibility entry point for the shared YAML repository."""

from datamart.infrastructure.dag_store import DagDocument as DagDocument
from datamart.infrastructure.dag_store import DagStore as DagStore
from datamart.infrastructure.dag_store import MtimeConflictError as MtimeConflictError
