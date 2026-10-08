"""Request validation without a database; real data/fault checks live separately."""

import pytest
from warehouse_pipeline import WarehousePipeline


@pytest.mark.parametrize(
    "start,end",
    [
        (None, "2026-01-01"),
        ("bad", "2026-01-01"),
        ("2026-01-02", "2026-01-01"),
        ("2026-01-01", "2026-02-01"),
        (123, "2026-01-01"),
    ],
)
def test_invalid_backfill_fails_before_database_access(start, end):
    def forbidden_connect():
        raise AssertionError("Invalid request must not open a database connection")

    with pytest.raises(ValueError):
        WarehousePipeline(forbidden_connect).run("invalid", "backfill", start_date=start, end_date=end)


def test_31_day_backfill_boundary():
    assert WarehousePipeline._arguments("backfill", "2026-01-01", "2026-01-31") == {
        "start_date": "2026-01-01",
        "end_date": "2026-01-31",
    }
