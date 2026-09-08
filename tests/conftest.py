"""pytest 公共配置：把项目 plugins/ 目录加入 sys.path。

让 `import dag_designer`（可视化 DAG 设计器渲染层）在 pytest 下可用；
Airflow 自身的 DagBag 测试走 AIRFLOW__CORE__PLUGINS_FOLDER 机制，与本配置不冲突。
"""

import sys
from pathlib import Path

_PLUGINS_DIR = str(Path(__file__).resolve().parent.parent / "plugins")
if _PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLUGINS_DIR)
