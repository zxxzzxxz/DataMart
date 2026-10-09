#!/usr/bin/env python3
"""DataMart 告警接收中心 + 通知转发器。

- POST /alert  接收告警（Alertmanager webhook payload / Airflow 文本告警 / 任意 JSON），
  持久化到 /data/alerts.jsonl，并按配置转发到通知渠道
- GET  /       告警列表页面（最近 100 条）

转发配置（全部环境变量驱动，缺省即关闭，不影响落盘主链路）：
- ALERT_FORWARD_WECOM       企业微信群机器人 webhook URL（设置后按级别转发）
- ALERT_FORWARD_SEVERITIES  转发级别，逗号分隔（默认 critical；Watchdog 常鸣永不转发）
- ALERT_SMTP_HOST/PORT/USER/PASS/FROM/TO/TLS  邮件通知（host 与 to 同时配置才生效）

转发失败只记日志，绝不改变 POST /alert 的 200 响应（告警落盘永远优先）。
"""

import html
import json
import os
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, HTTPServer

DATA_DIR = "/data"
DATA_FILE = os.path.join(DATA_DIR, "alerts.jsonl")
os.makedirs(DATA_DIR, exist_ok=True)

WECOM_URL = os.environ.get("ALERT_FORWARD_WECOM", "")
FORWARD_SEVERITIES = {
    s.strip() for s in os.environ.get("ALERT_FORWARD_SEVERITIES", "critical").split(",") if s.strip()
}
SMTP = {
    "host": os.environ.get("ALERT_SMTP_HOST", ""),
    "port": int(os.environ.get("ALERT_SMTP_PORT", "465")),
    "user": os.environ.get("ALERT_SMTP_USER", ""),
    "password": os.environ.get("ALERT_SMTP_PASS", ""),
    "from": os.environ.get("ALERT_SMTP_FROM") or os.environ.get("ALERT_SMTP_USER", ""),
    "to": [x.strip() for x in os.environ.get("ALERT_SMTP_TO", "").split(",") if x.strip()],
    "tls": os.environ.get("ALERT_SMTP_TLS", "true").lower() != "false",
}
_SEVERITY_RANK = {"critical": 3, "warning": 2, "info": 1}


def log(message, *args):
    print(f"[forward] {time.strftime('%Y-%m-%d %H:%M:%S')} {message % args}", flush=True)


def _alertmanager_message(payload):
    """Alertmanager webhook payload → (severity, subject, text)；全为 Watchdog 时返回 None。"""
    status = payload.get("status", "firing")
    top_severity = "info"
    top_name = "alert"
    lines = []
    for alert in payload.get("alerts", []):
        labels = alert.get("labels", {})
        annotations = alert.get("annotations", {})
        name = labels.get("alertname", "unknown")
        severity = labels.get("severity", "info")
        if _SEVERITY_RANK.get(severity, 0) > _SEVERITY_RANK.get(top_severity, 0):
            top_severity, top_name = severity, name
        summary = annotations.get("summary") or name
        lines.append(
            f"[{status}] {name} ({severity})\n{summary}\n"
            f"job={labels.get('job', '-')} instance={labels.get('instance', '-')}"
        )
    if not lines:
        return None
    if all(a.get("labels", {}).get("alertname") == "Watchdog" for a in payload.get("alerts", [])):
        return None  # Watchdog 常鸣只进告警中心存档，不转发（防噪音）
    return top_severity, top_name, "\n\n".join(lines)


def build_forward_message(body):
    """统一两种告警形态 → (severity, subject, text) 或 None（不转发）。"""
    try:
        payload = json.loads(body)
    except ValueError:
        return "critical", "alert", body[:1500]  # 非 JSON 文本：按 critical 兜底转发
    if isinstance(payload, dict) and isinstance(payload.get("alerts"), list):
        return _alertmanager_message(payload)
    # Airflow 任务失败回调（plugins/alerting.py 的 msgtype=text 形态）
    if isinstance(payload, dict) and isinstance(payload.get("text"), dict):
        content = str(payload["text"].get("content", body))[:1500]
        return "critical", "task-failure", content
    return "critical", "alert", body[:1500]


def forward_wecom(text):
    if not WECOM_URL:
        return
    data = json.dumps({"msgtype": "text", "text": {"content": text}}, ensure_ascii=False).encode()
    req = urllib.request.Request(WECOM_URL, data=data, headers={"Content-Type": "application/json"})
    with urllib.request.urlopen(req, timeout=5) as resp:
        resp.read()
    log("wecom 已送达（%d 字节）", len(data))


def forward_email(subject, text):
    if not (SMTP["host"] and SMTP["to"] and SMTP["from"]):
        return
    import smtplib
    from email.mime.text import MIMEText

    msg = MIMEText(text, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = SMTP["from"]
    msg["To"] = ", ".join(SMTP["to"])
    with smtplib.SMTP(SMTP["host"], SMTP["port"], timeout=5) as server:
        if SMTP["tls"]:
            server.starttls()
        if SMTP["user"]:
            server.login(SMTP["user"], SMTP["password"])
        server.sendmail(SMTP["from"], SMTP["to"], msg.as_string())
    log("email 已送达 %s（via %s:%s）", SMTP["to"], SMTP["host"], SMTP["port"])


def forward(body):
    message = build_forward_message(body)
    if message is None:
        return
    severity, name, text = message
    if severity not in FORWARD_SEVERITIES:
        return
    try:
        forward_wecom(text)
    except Exception as exc:
        log("wecom 转发失败：%r", exc)
    try:
        forward_email(f"[DataMart {severity}] {name}", text)
    except Exception as exc:
        log("email 转发失败：%r", exc)


class AlertHandler(BaseHTTPRequestHandler):
    def do_POST(self):
        if self.path.split("?")[0] != "/alert":
            self.send_error(404)
            return
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8", "replace")
        record = {"ts": time.strftime("%Y-%m-%d %H:%M:%S"), "body": body}
        with open(DATA_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")
        forward(body)
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def do_GET(self):
        rows = []
        if os.path.exists(DATA_FILE):
            with open(DATA_FILE, encoding="utf-8") as f:
                rows = f.readlines()[-100:][::-1]
        trs = []
        for r in rows:
            rec = json.loads(r)
            trs.append(
                f"<tr><td>{html.escape(rec['ts'])}</td>"
                f"<td><pre style='margin:0'>{html.escape(rec['body'])}</pre></td></tr>"
            )
        page = (
            "<html><head><meta charset='utf-8'><title>DataMart 告警中心</title></head>"
            "<body style='font-family:sans-serif'><h1>DataMart 告警中心（最近 100 条）</h1>"
            f"<table border='1' cellspacing='0' cellpadding='6'>{''.join(trs)}</table>"
            "</body></html>"
        ).encode()
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(page)

    def log_message(self, *args):
        pass


if __name__ == "__main__":
    if WECOM_URL:
        log("企业微信转发已启用（级别：%s）", ",".join(sorted(FORWARD_SEVERITIES)))
    if SMTP["host"] and SMTP["to"]:
        log("邮件转发已启用：%s:%s → %s", SMTP["host"], SMTP["port"], SMTP["to"])
    HTTPServer(("0.0.0.0", 9099), AlertHandler).serve_forever()
