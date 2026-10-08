"""pytest 公共配置：把项目 plugins/ 目录加入 sys.path。

让 `import dag_designer`（可视化 DAG 设计器渲染层）在 pytest 下可用；
Airflow 自身的 DagBag 测试走 AIRFLOW__CORE__PLUGINS_FOLDER 机制，与本配置不冲突。
"""

import os
import sys
from pathlib import Path

# Windows 宿主机兼容（Airflow 官方仅支持 POSIX；CI 与容器均为 Linux，不受影响）：
# apache-airflow-task-sdk 1.3.2 的 stats 模块在导入时无条件调用 os.register_at_fork，
# 该属性在 Windows 上不存在，导致 airflow.sdk 懒加载失败、本地 pytest 无法收集。
# 处理：先完成 stdlib 中对 register_at_fork 做 hasattr 判断且依赖 POSIX-only 锁方法的
# 模块（concurrent.futures.thread），再补 no-op shim 供 airflow 导入期使用。
# Windows 无 fork 语义，no-op 行为等价正确。
if not hasattr(os, "register_at_fork"):
    import concurrent.futures.thread  # noqa: F401  # 必须在 shim 之前完成其 hasattr 分支

    os.register_at_fork = lambda *args, **kwargs: None

import signal

# 同理：core 3.3.2 的 DAG 解析超时包装（dag_processing/importers/python_importer.py）
# 使用 POSIX 信号 SIGALRM/setitimer，Windows 上不存在。补桩后走代码内已有的
# ValueError 兜底分支（Windows 无 fork/SIGALRM 语义，no-op 行为等价正确）。
if not hasattr(signal, "setitimer"):
    signal.setitimer = lambda *args, **kwargs: 0
    signal.ITIMER_REAL = 0
if not hasattr(signal, "SIGALRM"):
    # 哑值；signal.signal(14, ...) 在 Windows 抛 ValueError，恰好被调用方兜底
    signal.SIGALRM = 14

_PLUGINS_DIR = str(Path(__file__).resolve().parent.parent / "plugins")
if _PLUGINS_DIR not in sys.path:
    sys.path.insert(0, _PLUGINS_DIR)

# test_designer_auth.py 依赖 designer/backend 的 app 包：宿主机/CI 上可收集；
# Airflow 容器内只挂载了 tests/（无 designer/backend），此时忽略该文件，
# 保证「容器内 pytest tests -q」命令对 DAG/插件测试仍然可用。
_DESIGNER_APP_DIR = str(Path(__file__).resolve().parent.parent / "designer" / "backend")
if (Path(_DESIGNER_APP_DIR) / "app").is_dir():
    if _DESIGNER_APP_DIR not in sys.path:
        sys.path.insert(0, _DESIGNER_APP_DIR)
else:
    collect_ignore = ["test_designer_auth.py", "test_designer_runtime.py"]

pytest_plugins = ["postgres_fixtures"]
