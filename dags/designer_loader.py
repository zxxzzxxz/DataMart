"""dag-factory 引导：扫描 dags/designer/*.yaml，把可视化设计器生成的 YAML 渲染成真实 Airflow DAG。

- 目录不存在或为空时不报错（设计器还没产生任何 DAG 时静默跳过）；
- 单个 YAML 损坏时 dag-factory 内部按文件 try/except 隔离并记录日志，不影响其他文件；
- 需要 dag-factory 包（容器经 _PIP_ADDITIONAL_REQUIREMENTS 安装）。
"""

import logging
import os

log = logging.getLogger(__name__)

_DESIGNER_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "designer")

try:
    from dagfactory import load_yaml_dags
except ImportError:
    log.warning("dag-factory 未安装，跳过设计器 DAG 加载（dags/designer/*.yaml 不会生成 DAG）")
else:
    if os.path.isdir(_DESIGNER_DIR):
        load_yaml_dags(globals(), dags_folder=_DESIGNER_DIR)
    else:
        log.info("设计器 YAML 目录不存在，跳过：%s", _DESIGNER_DIR)
