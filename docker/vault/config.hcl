# Vault 服务端配置（server 模式 + file 存储，非 dev 模式——密钥重启不丢）
# 代价：重启后处于密封（sealed）状态，需 scripts/vault-unseal.sh 手动解封（与平台
# 手动 failover 的运维哲学一致：关键操作人工确认）。
ui = true
disable_mlock = true
api_addr = "http://vault:8200"

storage "file" {
  path = "/vault/file"
}

listener "tcp" {
  address     = "0.0.0.0:8200"
  tls_disable = true   # 容器内网明文；对外暴露需自行加 TLS（当前不暴露宿主端口）
}
