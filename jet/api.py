"""HTTP 查詢／操作介面（DESIGN 第 7 節）。骨架：路由表。

讀（X-Jet-Read-Key）：GET /status /branches /branches/<name> /merged /todo /metrics 與人看的 /
寫（X-Jet-Write-Key，全部寫 audit）：POST /approve /revoke /priority /adhoc-run /pause /resume
預設只聽 127.0.0.1。"""
READ_ROUTES = ["/status", "/branches", "/branches/<name>", "/merged", "/todo", "/metrics", "/"]
WRITE_ROUTES = ["/approve", "/revoke", "/priority", "/adhoc-run", "/pause", "/resume"]
