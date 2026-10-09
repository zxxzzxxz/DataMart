"""Typed operator parameters; UI and renderer share these validation contracts."""

import json
from typing import Literal

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictFloat,
    StrictInt,
    StrictStr,
    field_validator,
    model_validator,
)

TriggerRule = Literal[
    "all_success",
    "all_done",
    "all_failed",
    "none_failed",
    "one_success",
    "one_failed",
    "none_skipped",
]


class TaskParams(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    trigger_rule: TriggerRule = "all_success"
    retries: int | None = Field(default=None, ge=0, le=10)

    @model_validator(mode="before")
    @classmethod
    def omit_optional_blanks(cls, value):
        if not isinstance(value, dict):
            return value
        return {
            name: item
            for name, item in value.items()
            if not (
                name in cls.model_fields
                and not cls.model_fields[name].is_required()
                and (item is None or isinstance(item, str) and not item.strip())
            )
        }

    @field_validator("*", mode="after")
    @classmethod
    def non_blank_strings(cls, value):
        if isinstance(value, str) and not value.strip():
            raise ValueError("参数不能为空")
        return value


class SqlParams(TaskParams):
    sql: str
    conn_id: Literal["warehouse"] = "warehouse"


class SqlCheckParams(SqlParams):
    pass_value: StrictStr | StrictInt | StrictFloat


class NotifyParams(TaskParams):
    message: str


class BashParams(TaskParams):
    bash_command: str


class HttpParams(TaskParams):
    endpoint: str
    method: Literal["GET", "POST", "PUT", "PATCH", "DELETE"] = "GET"
    data: str | None = None
    headers: dict[str, str] | None = None

    @field_validator("headers", mode="before")
    @classmethod
    def parse_headers(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(value)
            except ValueError as exc:
                raise ValueError("headers 必须是 JSON 对象") from exc
        return value


class SensorSqlParams(SqlParams):
    poke_interval: int = Field(default=60, gt=0)
    timeout: int = Field(default=3600, gt=0)
    mode: Literal["poke", "reschedule"] = "reschedule"

    @field_validator("mode", mode="before")
    @classmethod
    def legacy_mode(cls, value):
        return "poke" if value == "normal" else value


class _LoadParamsBase(TaskParams):
    target_table: str
    mode: Literal["append", "truncate", "upsert"] = "append"
    upsert_key: str | None = None
    conn_id: Literal["warehouse"] = "warehouse"

    @model_validator(mode="after")
    def upsert_requires_key(self):
        if self.mode == "upsert" and not (self.upsert_key and self.upsert_key.strip()):
            raise ValueError("mode=upsert 时必须提供 upsert_key")
        return self


class ApiLoadParams(_LoadParamsBase):
    url: str
    method: Literal["GET", "POST"] = "GET"
    headers: dict[str, str] | None = None
    data_path: str | None = None

    @field_validator("headers", mode="before")
    @classmethod
    def parse_headers(cls, value):
        if isinstance(value, str):
            try:
                return json.loads(value) if value.strip() else None
            except ValueError as exc:
                raise ValueError("headers 必须是 JSON 对象") from exc
        return value


class FileLoadParams(_LoadParamsBase):
    source: str
    format: Literal["csv", "json", "jsonl"] = "csv"
    delimiter: str = ","
    header: bool = True


PARAM_MODELS = {
    "sql": SqlParams,
    "sql_check": SqlCheckParams,
    "notify": NotifyParams,
    "bash": BashParams,
    "http": HttpParams,
    "sensor_sql": SensorSqlParams,
    "empty": TaskParams,
    "api_load": ApiLoadParams,
    "file_load": FileLoadParams,
}
