"""Expone exclusivamente webhooks GitHub firmados para la rama main."""
import hashlib
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
from pathlib import Path
import urllib.error
import urllib.request

SECRET = Path("/run/secrets/github_webhook_secret").read_bytes().strip()


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, message):
        body = json.dumps({"status": message}).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        self.reply(404, "not found")

    def do_POST(self):
        if self.path != "/github-webhook/":
            return self.reply(404, "not found")
        try:
            size = int(self.headers.get("Content-Length", "0"))
        except ValueError:
            return self.reply(400, "invalid size")
        if not 0 < size <= 10 * 1024 * 1024:
            return self.reply(413, "invalid size")
        self.connection.settimeout(15)
        body = self.rfile.read(size)
        signature = "sha256=" + hmac.new(SECRET, body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(signature, self.headers.get("X-Hub-Signature-256", "")):
            return self.reply(403, "invalid signature")
        try:
            payload = json.loads(body)
            if not isinstance(payload, dict):
                raise ValueError()
        except (ValueError, UnicodeDecodeError):
            return self.reply(400, "invalid JSON")
        if payload.get("repository", {}).get("full_name") != "Bjonion/projectBD":
            return self.reply(403, "invalid repository")
        event = self.headers.get("X-GitHub-Event")
        if event not in {"push", "ping"}:
            return self.reply(202, "ignored event")
        if event == "push" and payload.get("ref") != "refs/heads/main":
            return self.reply(202, "ignored branch")
        headers = {"Content-Type": "application/json", "X-GitHub-Event": event,
                   "X-GitHub-Delivery": self.headers.get("X-GitHub-Delivery", ""),
                   "X-Hub-Signature-256": signature}
        upstream = urllib.request.Request("http://jenkins:8080/github-webhook/", data=body, headers=headers)
        try:
            with urllib.request.urlopen(upstream, timeout=15) as response:
                response.read()
        except (urllib.error.URLError, TimeoutError):
            return self.reply(502, "Jenkins unavailable")
        return self.reply(200, "accepted")

    def log_message(self, format, *args):
        # No se registran cabeceras, firmas, payloads ni credenciales.
        print("webhook", self.command, self.path.split("?")[0], flush=True)


if __name__ == "__main__":
    ThreadingHTTPServer(("0.0.0.0", 8090), Handler).serve_forever()
