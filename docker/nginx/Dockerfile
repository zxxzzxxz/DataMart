# 统一入口 nginx：TLS 终结 + 反向代理
FROM nginx:1.31-alpine

# 用本项目统一入口配置整体替换镜像默认 nginx.conf。
# 注意：镜像默认 nginx.conf 会 include /etc/nginx/conf.d/*.conf（含默认 80 server），
# 本配置不含该 include，因此默认 server 自动失效，不会与 80 端口冲突。
COPY nginx.conf /etc/nginx/nginx.conf

# 证书无需 COPY：由 compose 以只读方式挂载
#   ./docker/nginx/certs:/etc/nginx/certs:ro
# 首次启动前先在宿主机执行 scripts/gen-certs.sh 生成开发证书。

EXPOSE 80 443 8443
