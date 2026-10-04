"""HTTP 服务：页面 + 健康检查 + 标定批次 JSON API。

仅使用标准库。监听地址、端口与数据库路径均可通过环境变量配置：

* ``HOST``（默认 0.0.0.0）
* ``PORT``（默认 8080）
* ``DB_PATH``（默认 /data/calibration.db）
"""

from __future__ import annotations

import json
import os
import threading
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlsplit

from .store import DomainError, Store

WEB_DIR = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "web")

_STATIC = {
    "/": ("index.html", "text/html; charset=utf-8"),
    "/index.html": ("index.html", "text/html; charset=utf-8"),
    "/static/app.js": ("app.js", "application/javascript; charset=utf-8"),
    "/static/styles.css": ("styles.css", "text/css; charset=utf-8"),
}


class Handler(BaseHTTPRequestHandler):
    server_version = "DeepSpaceCal/1.0"
    store: Store  # 由工厂注入

    # ---- 基础工具 -----------------------------------------------------
    def log_message(self, fmt: str, *args: object) -> None:
        if os.environ.get("QUIET") != "1":
            super().log_message(fmt, *args)

    def _send_json(self, body: object, status: int = 200,
                   extra_headers: dict[str, str] | None = None) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        for k, v in (extra_headers or {}).items():
            self.send_header(k, v)
        self.end_headers()
        self.wfile.write(data)

    def _send_error_json(self, err: DomainError) -> None:
        self._send_json(err.to_dict(), err.http_status)

    def _read_json(self) -> object:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise DomainError("INVALID_BODY", "请求体为空，需要 JSON 对象", 400)
        if length > 1_000_000:
            raise DomainError("BODY_TOO_LARGE", "请求体超过 1MB 上限", 413)
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise DomainError(
                "INVALID_JSON", "请求体不是合法 JSON：%s" % exc, 400,
                hint="请设置 Content-Type: application/json 并检查语法")

    # ---- 路由 ---------------------------------------------------------
    def do_GET(self) -> None:  # noqa: N802 (stdlib 命名)
        path = urlsplit(self.path).path
        if path == "/healthz":
            self._send_json({"status": "ok", "service": "calibration-seal"},
                            HTTPStatus.OK)
            return
        if path == "/api/batches":
            self._send_json({"batches": self.store.list_batches()})
            return
        if path.startswith("/api/batches/"):
            batch_id = path[len("/api/batches/"):]
            if "/" in batch_id or not batch_id:
                self._send_json(DomainError(
                    "NOT_FOUND", "未知路径", 404).to_dict(), 404)
                return
            try:
                state = self.store.get_state(batch_id)
            except DomainError as err:
                self._send_error_json(err)
                return
            # ETag 以版本号为准，轮询可做条件请求；不影响封存态的权威展示。
            etag = 'W/"v%d-%s"' % (
                state["version"], state["status"][0])
            if self.headers.get("If-None-Match") == etag:
                self.send_response(HTTPStatus.NOT_MODIFIED)
                self.send_header("ETag", etag)
                self.end_headers()
                return
            self._send_json(state, extra_headers={"ETag": etag})
            return
        if path in _STATIC:
            self._serve_static(*_STATIC[path])
            return
        self._send_json(DomainError("NOT_FOUND", "未知路径：%s" % path, 404)
                        .to_dict(), 404)

    def do_POST(self) -> None:  # noqa: N802
        path = urlsplit(self.path).path
        try:
            payload = self._read_json()
            if path == "/api/batches":
                self._send_json(self.store.create_batch(payload),
                                HTTPStatus.CREATED)
                return
            if path.startswith("/api/batches/") and path.endswith("/votes"):
                batch_id = path[len("/api/batches/"):-len("/votes")]
                if not batch_id or "/" in batch_id:
                    raise DomainError("NOT_FOUND", "未知投票路径", 404)
                result = self.store.submit_vote(batch_id, payload)
                self._send_json({
                    "decision": result.decision,
                    "replayed": result.replayed,
                    "reason": result.reason,
                    "vote": result.vote,
                    "sealed_now": result.sealed_now,
                    "state": result.state,
                }, HTTPStatus.ACCEPTED if result.replayed
                    else HTTPStatus.CREATED)
                return
            self._send_json(DomainError("NOT_FOUND", "未知路径：%s" % path, 404)
                            .to_dict(), 404)
        except DomainError as err:
            self._send_error_json(err)
        except Exception as exc:  # pragma: no cover - 防御性兜底
            self._send_json({"code": "INTERNAL",
                             "message": "服务器内部错误：%s" % exc}, 500)

    def _serve_static(self, filename: str, content_type: str) -> None:
        full = os.path.join(WEB_DIR, filename)
        # WEB_DIR 是固定目录且文件名来自白名单，这里仍做一次前缀校验。
        if os.path.commonpath((os.path.abspath(full), WEB_DIR)) != WEB_DIR \
                or not os.path.isfile(full):
            self._send_json(DomainError("NOT_FOUND", "页面资源缺失", 404)
                            .to_dict(), 404)
            return
        with open(full, "rb") as fh:
            data = fh.read()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)


def build_server(host: str, port: int, db_path: str) -> ThreadingHTTPServer:
    store = Store(db_path)

    class _Bound(Handler):
        pass

    _Bound.store = store
    httpd = ThreadingHTTPServer((host, port), _Bound)
    httpd.store = store  # type: ignore[attr-defined]
    return httpd


def main() -> None:
    host = os.environ.get("HOST", "0.0.0.0")
    port = int(os.environ.get("PORT", "8080"))
    db_path = os.environ.get("DB_PATH", "/data/calibration.db")
    httpd = build_server(host, port, db_path)
    print("calibration-seal listening on %s:%d db=%s" % (host, port, db_path),
          flush=True)
    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        httpd.shutdown()
        httpd.server_close()
        httpd.store.close()  # type: ignore[attr-defined]


if __name__ == "__main__":
    main()
