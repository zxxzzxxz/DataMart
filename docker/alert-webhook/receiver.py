#!/usr/bin/env python3
"""DataMart 告警接收中心（真实 webhook 通道）。

- POST /alert  接收告警（任意 JSON/文本 body），持久化到 /data/alerts.jsonl
- GET  /       告警列表页面（最近 100 条）

生产环境只需把 Airflow Variable `datamart_alert_webhook` 换成企业微信/飞书/钉钉的
机器人地址即可，本服务用于本地端到端验收告警链路。
"""

import html
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

DATA_DIR = "/data"
DATA_FILE = os.path.join(DATA_DIR, "alerts.jsonl")
os.makedirs(DATA_DIR, exist_ok=True)


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
    HTTPServer(("0.0.0.0", 9099), AlertHandler).serve_forever()
