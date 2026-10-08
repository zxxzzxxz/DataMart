# Keycloak SSO（OIDC）接入指南

DataMart 平台支持可选开启的 Keycloak 单点登录。**默认关闭**（`AIRFLOW_OAUTH_ENABLED=false`），
关闭时保持 FAB 本地账号登录（admin/admin123），`config/webserver_config.py` 在关闭模式下
import 零副作用（不 import Authlib、不连接 Keycloak）。

## 架构与认证流

```
                     浏览器
                       │  HTTPS（统一入口，自签名证书，curl 需 -k）
                       ▼
              ┌─────────────────┐
              │  nginx :443     │  TLS 终结 + 反代（透传不改写路径）
              └───────┬─────────┘
          /           │           \keycloak/
         ▼            │            ▼
  airflow-apiserver:8080      keycloak:8080（start-dev，KC_HTTP_RELATIVE_PATH=/keycloak）
         │            OIDC 服务端调用（docker 内网直连，不经 nginx）     ▲
         └───────────────────────────────────────────────────────────┘
              token 换取 / userinfo / JWKS：http://keycloak:8080/keycloak/realms/datamart/...
```

标准授权码流程（OIDC Authorization Code Flow）：

1. 浏览器访问 `https://localhost/`，FAB 登录页出现 **Sign in with keycloak** 按钮；
2. 点击后浏览器被 302 到**外部** authorize 地址
   `https://localhost/keycloak/realms/datamart/protocol/openid-connect/auth?...&redirect_uri=https://localhost/auth/oauth-authorized/keycloak`；
3. 用户在 Keycloak 页面输入账号（如 sso-admin/admin123），Keycloak 带 code 回调
   `https://localhost/auth/oauth-authorized/keycloak`；
4. apiserver 用 code + client_secret 到**内网** token 地址
   `http://keycloak:8080/keycloak/realms/datamart/protocol/openid-connect/token` 换取 access_token；
5. apiserver 再向内网 `.../userinfo` 取用户信息（含 `realm_access.roles`），按
   `AUTH_ROLES_MAPPING` 映射为 FAB 角色并建立会话。

> 双地址设计：`authorize_url` 用外部地址（浏览器可达），`access_token_url`/`api_base_url`/`jwks_uri`
> 用内网地址（容器直连）。不走 `server_metadata_url` 自动发现，避免 Keycloak issuer 里的
> 内网 hostname 被写死进浏览器跳转。

## 组件与端点

| 项 | 值 |
| --- | --- |
| Keycloak 管理台 | `https://localhost/keycloak/`（admin / admin123） |
| Realm | `datamart`（首次启动由 `--import-realm` 导入 `docker/keycloak/realm-datamart.json`） |
| OIDC client | `airflow-web`（confidential，secret `datamart-dev-client-secret` 为**开发占位符**） |
| 回调地址 | `https://localhost/auth/oauth-authorized/keycloak`（validRedirectUris `https://localhost/*` 已覆盖） |
| 内置用户 | sso-admin / admin123（airflow-admin）、sso-viewer / viewer123（airflow-viewer） |
| authorize_url | `https://localhost/keycloak/realms/datamart/protocol/openid-connect/auth`（外部） |
| access_token_url | `http://keycloak:8080/keycloak/realms/datamart/protocol/openid-connect/token`（内网） |
| api_base_url(userinfo) | `http://keycloak:8080/keycloak/realms/datamart/protocol/openid-connect/`（内网） |
| jwks_uri | `http://keycloak:8080/keycloak/realms/datamart/protocol/openid-connect/certs`（内网） |

## 开启步骤

前置接线（compose/nginx 侧已完成）：keycloak 服务与 realm 自动导入、nginx `/keycloak/` 透传、
`./config/webserver_config.py` 挂载到 `/opt/airflow/webserver_config.py`、
`_PIP_ADDITIONAL_REQUIREMENTS` 含 Authlib。

1. 设置环境变量后重建 apiserver：

   ```bash
   # .env 或 shell 环境
   AIRFLOW_OAUTH_ENABLED=true
   # nginx 反代后必须开启 ProxyFix，否则 FAB 生成的 redirect_uri 是 http:// 开头，
   # 与 Keycloak 的 https validRedirectUris 不匹配
   AIRFLOW__FAB__ENABLE_PROXY_FIX=true
   ```

   ```bash
   docker compose up -d airflow-apiserver   # 环境变量变化会自动重建容器
   ```

2. 冒烟验证（宿主机 Git Bash）：

   ```bash
   scripts/test-sso.sh
   ```

3. 浏览器打开 `https://localhost/` → 登录页点 **Sign in with keycloak** →
   用 sso-admin/admin123 登录 → 回到 Airflow 且拥有 Admin 权限。

关闭 SSO：去掉/置回 `AIRFLOW_OAUTH_ENABLED=false`，再 `docker compose up -d airflow-apiserver`
即可回退本地账号登录，原有账号、会话外的数据均不受影响。

## 首次登录与角色映射

- `AUTH_USER_REGISTRATION = True`：首次 SSO 登录自动在 FAB 用户表创建账号，无需预先
  `airflow users create`。
- `AUTH_ROLES_SYNC_AT_LOGIN = True`：每次登录按 Keycloak 当前角色重新同步；在 Keycloak
  里把用户从 airflow-viewer 改为 airflow-admin，下次登录即生效。
- 映射表（`config/webserver_config.py`）：

  | Keycloak realm 角色 | FAB 角色 |
  | --- | --- |
  | airflow-admin | Admin |
  | airflow-viewer | Viewer |

- 角色来源：realm 中 `roles` client scope 的 `realm roles` mapper 把用户的 realm 角色写入
  token/userinfo 的 `realm_access.roles`；自定义 `KeycloakSecurityManager` 从 userinfo 读取该声明
  （FAB 内置 keycloak 处理器读的是 `groups` 声明，与本 realm 模型不符，故覆盖）。
- 两边角色都不匹配的用户：仍可登录，但只落 FAB `Public` 角色（无任何权限页面）。
- SSO 用户与本地用户按 username 区分，互不影响；**不要**把 SSO 用户名取成 `admin` 等与本地
  账号相同的名字（FAB 用户表 username 唯一，会产生不可预期的合并）。

## 生产要点

- **换 client secret**：`datamart-dev-client-secret` 只是开发占位符。生产在 Keycloak
  Admin Console → Clients → airflow-web → Credentials 重新生成，apiserver 侧用
  `OAUTH_CLIENT_SECRET` 注入（不要把真实 secret 提交进仓库/realm 文件）。
- **换掉 start-dev**：开发用 `start-dev`（内嵌 H2、宽松默认值）。生产改 `start` 并接外部数据库
  （`KC_DB=postgres` + `KC_DB_URL/KC_DB_USERNAME/KC_DB_PASSWORD`），固定
  `KC_HOSTNAME=https://<域名>/keycloak`。
- **realm 备份**：realm 配置即代码（`docker/keycloak/realm-datamart.json`）纳入 git 管理；
  运行期改动（新增用户/角色）用 `kcadm.sh` 或
  `docker compose exec keycloak /opt/keycloak/bin/kc.sh export --realm datamart --dir /tmp/export`
  定期导出备份。注意 `--import-realm` 只在数据卷为空时导入一次，改 JSON 后需清空 keycloak
  数据卷（或在 Admin Console 手动改）才重新生效。
- **收紧 client**：生产关掉 `directAccessGrantsEnabled`（它只为 `scripts/test-sso.sh` 冒烟验证
  服务）；`validRedirectUris` 从 `https://localhost/*` 收紧为精确回调地址。
- **对接企业账号源**：用 User Federation 接 LDAP/AD，替代 realm 文件里的内置用户。
- 同步修改 `KEYCLOAK_ADMIN_PASSWORD`、域名与证书（`scripts/gen-certs.sh` 仅用于开发自签名）。

## 常见问题

- **Keycloak 报错 `Invalid parameter: redirect_uri`**：FAB 生成的回调地址与 client 的
  validRedirectUris 不一致。最常见原因是没开 `AIRFLOW__FAB__ENABLE_PROXY_FIX=true`——
  nginx 终结 TLS 后，未开 ProxyFix 的 FAB 会生成 `http://localhost/...`（http 而非 https）。
  其次确认浏览器访问的是 `https://localhost/`（回调 host 跟随访问 host）。
- **登录后变成 Viewer 或没有任何权限（Public）**：token/userinfo 没带 `realm_access.roles`。
  检查 client `airflow-web` 的 default client scopes 含 `roles`；`roles` scope 的
  `realm roles` mapper 三项开关（ID token / Access token / Userinfo）均开启；用户确实被授予
  `airflow-admin`/`airflow-viewer` realm 角色。可用 `scripts/test-sso.sh` 第 2 步直接查看
  token payload 里的 `realm_access.roles`。
- **登录循环/反复跳回登录页**：多见于容器间**时间漂移**（token 签发/校验时间戳超差）——
  重启 Docker Desktop 或对时；少数情况是 cookie 被浏览器按 SameSite/Secure 策略拦截
  （确认全程 https、同一 host）。排查看 `docker compose logs airflow-apiserver`。
- **登录页没有 "Sign in with keycloak" 按钮**：`AIRFLOW_OAUTH_ENABLED=true` 没生效——
  确认 apiserver 容器内环境变量（`docker compose exec airflow-apiserver env | grep OAUTH`）、
  apiserver 已重建、`webserver_config.py` 确实挂载到了 `/opt/airflow/webserver_config.py`
  （FAB 只认 `[fab] config_file`，默认 `{AIRFLOW_HOME}/webserver_config.py`，
  挂到 `/opt/airflow/config/` 目录不会被读取）。
- **token 接口内网地址连不通**：apiserver 与 keycloak 需在同一 compose 网络；
  `docker compose exec airflow-apiserver curl -s http://keycloak:8080/keycloak/realms/datamart/.well-known/openid-configuration | head` 应返回 JSON。
