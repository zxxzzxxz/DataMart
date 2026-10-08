# ==========================================================================
# Airflow FAB webserver 配置（由 FAB auth manager 加载）
#
# 加载路径：FAB 读取 airflow.cfg [fab] config_file，默认 {AIRFLOW_HOME}/webserver_config.py
# （本仓库镜像 AIRFLOW_HOME=/opt/airflow，compose 将本文件挂载到 /opt/airflow/webserver_config.py）。
#
# 认证模式由环境变量 AIRFLOW_OAUTH_ENABLED 控制：
#   - 缺省/false：AUTH_DB 本地账号登录（admin/admin123 等现有账号完全不受影响）；
#     此模式下 import 本文件零副作用——不 import Authlib、不连接 Keycloak。
#   - true：AUTH_OAUTH，启用 Keycloak SSO（OIDC），登录页出现 "Sign in with keycloak" 按钮。
#
# 双地址设计（不经 server_metadata_url，避免内网 hostname 写死进浏览器跳转）：
#   - authorize_url：外部地址 https://localhost/keycloak/...（浏览器经 nginx 可达）
#   - access_token_url / userinfo(api_base_url) / jwks_uri：内网地址 http://keycloak:8080/keycloak/...
#     （airflow 容器经 docker 内网直连 keycloak，不绕 nginx）
# 官方依据：apache-airflow-providers-fab stable 文档
#   auth-manager/webserver-authentication 的 Keycloak 示例（回调路径 /auth/oauth-authorized/keycloak）
# ==========================================================================
import os

from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH

_OAUTH_ENABLED = os.environ.get("AIRFLOW_OAUTH_ENABLED", "false").strip().lower() in (
    "1",
    "true",
    "yes",
    "on",
)

# 默认本地数据库认证：现有 admin/admin123 登录不受影响
AUTH_TYPE = AUTH_OAUTH if _OAUTH_ENABLED else AUTH_DB

if _OAUTH_ENABLED:
    # ---- Keycloak 端点（realm: datamart）----
    # 外部地址：浏览器跳转用，经 nginx TLS 终结（https://localhost/keycloak/）
    _KC_PUBLIC = "https://localhost/keycloak/realms/datamart/protocol/openid-connect"
    # 内网地址：airflow 容器服务端调用用，docker 内网直连 keycloak:8080
    _KC_INTERNAL = "http://keycloak:8080/keycloak/realms/datamart/protocol/openid-connect"

    # 首次 SSO 登录自动在 FAB 用户表建账号（无需预先 airflow users create）
    AUTH_USER_REGISTRATION = True
    # 每次登录都按 Keycloak 最新角色重新同步 FAB 角色（Keycloak 侧改角色下次登录即生效）
    AUTH_ROLES_SYNC_AT_LOGIN = True
    # Keycloak realm 角色 -> FAB 角色；两边都不匹配的用户仅落 Public（无任何权限）
    AUTH_ROLES_MAPPING = {
        "airflow-admin": ["Admin"],
        "airflow-viewer": ["Viewer"],
    }

    OAUTH_PROVIDERS = [
        {
            "name": "keycloak",
            "icon": "fa-key",
            "token_key": "access_token",
            "remote_app": {
                "client_id": os.environ.get("OAUTH_CLIENT_ID", "airflow-web"),
                # 开发占位符，与 docker/keycloak/realm-datamart.json 中 airflow-web 的 secret 一致；
                # 生产更换 Keycloak 侧 secret 后通过本环境变量注入
                "client_secret": os.environ.get("OAUTH_CLIENT_SECRET", "datamart-dev-client-secret"),
                # 浏览器重定向地址（外部）
                "authorize_url": f"{_KC_PUBLIC}/auth",
                # 服务端换 token（内网）
                "access_token_url": f"{_KC_INTERNAL}/token",
                # userinfo 基底（内网，自定义 SecurityManager 以 GET userinfo 拼接）
                "api_base_url": f"{_KC_INTERNAL}/",
                # JWKS（内网；供需要验签的组件使用，本配置本身不强制验签）
                "jwks_uri": f"{_KC_INTERNAL}/certs",
                "client_kwargs": {"scope": "openid profile email roles"},
                "request_token_url": None,
            },
        }
    ]

    # 自定义 SecurityManager：FAB 内置 keycloak 处理器从 userinfo 的 groups 声明取角色，
    # 而本 realm 的角色在 realm_access.roles 中，这里改为读 realm_access.roles。
    # 用户信息走 userinfo 端点（不本地解码/验签 JWT，容器内无需引 jwt/cryptography 依赖）。
    from airflow.providers.fab.auth_manager.security_manager.override import (
        FabAirflowSecurityManagerOverride,
    )

    class KeycloakSecurityManager(FabAirflowSecurityManagerOverride):
        def get_oauth_user_info(self, provider, resp):
            if provider == "keycloak":
                me = self.appbuilder.sm.oauth_remotes[provider].get("userinfo")
                me.raise_for_status()
                data = me.json()
                return {
                    "username": data.get("preferred_username", ""),
                    "first_name": data.get("given_name", ""),
                    "last_name": data.get("family_name", ""),
                    "email": data.get("email", ""),
                    "role_keys": data.get("realm_access", {}).get("roles", []),
                }
            return {}

    SECURITY_MANAGER_CLASS = KeycloakSecurityManager
