"""可视化 DAG 设计器的渲染层：schema 校验 + dag-factory YAML 双向转换。

纯 Python 包（不依赖 Airflow），在以下环境均可直接 import：
- Airflow 运行时（plugins/ 目录经插件机制进入 sys.path）
- designer-api 容器（PLUGINS_DIR 注入 sys.path）
- 本地 pytest（tests/conftest.py 注入 sys.path）
"""

from dag_designer import catalog, convert, schema

__all__ = ["catalog", "convert", "schema"]
