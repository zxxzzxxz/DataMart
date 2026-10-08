"""Application configuration is resolved once at the composition root."""

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    dags_dir: str
    history_dir: str
    airflow_url: str
    airflow_username: str
    airflow_password: str
    warehouse_dsn: str

    @classmethod
    def from_env(cls):
        return cls(
            dags_dir=os.environ.get("DAGS_DIR", "/data/dags"),
            history_dir=os.environ.get("DESIGNER_HISTORY_DIR", "/data/history"),
            airflow_url=os.environ.get("AIRFLOW_API_URL", "http://airflow-apiserver:8080"),
            airflow_username=os.environ.get("AIRFLOW_API_USERNAME", "admin"),
            airflow_password=os.environ.get("AIRFLOW_API_PASSWORD", "admin123"),
            warehouse_dsn=os.environ.get(
                "WAREHOUSE_DSN", "postgresql://warehouse:warehouse@warehouse:5432/warehouse"
            ),
        )
