# Airflow 一体化镜像（在官方镜像基础上固化 Python 依赖）
#
# 官方 apache/airflow 镜像已自带 FAB provider（含 Authlib，SSO 用）与
# hashicorp provider（Vault secrets backend 用），此处只需固化：
#   - dag-factory 1.1.0：设计器 YAML -> DAG 渲染（版本与现网容器实测一致）
#   - pytest：保留「容器内跑测试套件」能力（tests/ 目录挂载进容器）
#
# 为什么用 root + /usr/python/bin/python3.13 装到全局 site-packages：
# 官方推荐 USER airflow 装到 /home/airflow/.local（user site），但本仓库容器
# 以 root 启动（挂载目录写权限），entrypoint 只对主进程降权到 airflow；
# exec 进入容器调试/运维时进程是 root，user site 解析到 /root/.local 就
# 找不到包装到全局 site-packages 后，airflow 用户与 root 的 sys.path 均可见。
FROM apache/airflow:3.3.2

USER root
# 以 airflow 用户的 pip 装入 user site（/home/airflow/.local），与官方镜像全家桶同处；
# 配合下方 PYTHONPATH，root（exec 运维）与 airflow（主进程）的 python 都能命中同一份包。
USER airflow
COPY --chown=airflow:0 pyproject.toml /tmp/datamart/pyproject.toml
COPY --chown=airflow:0 src /tmp/datamart/src
RUN pip install --no-cache-dir /tmp/datamart "dag-factory==1.1.0" "apache-airflow-providers-http==6.1.0" "boto3==1.43.109" "pytest"
# 官方镜像把 airflow 本体与全部 provider 装在 /home/airflow/.local（airflow 用户的
# user site）。容器以 root 启动时主进程被 entrypoint 降权到 airflow（不受影响），
# 但 exec 进入容器运维时进程是 root，user site 解析到 /root/.local 会找不到包。
# 显式 PYTHONPATH 让 root 的 python 同样命中该目录（单一份包，无版本双轨）。
ENV PYTHONPATH=/home/airflow/.local/lib/python3.13/site-packages
