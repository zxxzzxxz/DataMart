"""Compatibility facade for the packaged sales application service."""

from pathlib import Path

from datamart.application.sales import ASSET_URI as ASSET_URI
from datamart.application.sales import LOCK_KEY as LOCK_KEY
from datamart.application.sales import SalesPipeline
from datamart.errors import PipelineQualityError as PipelineQualityError
from datamart.infrastructure.postgres.transaction import TransactionQueryHook as TransactionQueryHook
from datamart.infrastructure.postgres.transaction import transaction_cursor as transaction_cursor

DEFAULT_CONFIG_DIR = Path(__file__).resolve().parent.parent / "config" / "data_quality"


class WarehousePipeline(SalesPipeline):
    def __init__(self, connect, config_dir=DEFAULT_CONFIG_DIR, **kwargs):
        super().__init__(connect, config_dir, **kwargs)
