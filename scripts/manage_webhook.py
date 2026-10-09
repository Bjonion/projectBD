"""Inicia/reinicia el túnel temporal y sincroniza su URL con GitHub."""
import argparse
import json
import os
from pathlib import Path
import re
import secrets
import subprocess
import time
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
API = "https://api.github.com/repos/Bjonion/projectBD"
STATE = ROOT / "secrets/github/webhook_state.json"


def docker(*args):
    return subprocess.run(["docker", "compose", *args], cwd=ROOT, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode()


def secret_file():
    directory = STATE.parent
    directory.mkdir(parents=True, exist_ok=True, mode=0o700)
    directory.chmod(0o700)
    path = directory / "webhook_secret"
    if not path.exists():
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(secrets.token_hex(32) + "\n")
    path.chmod(0o600)
    return path.read_text().strip()


def github_client():
    result = subprocess.run(["git", "credential", "fill"], cwd=ROOT,
                            input=b"protocol=https\nhost=github.com\n\n",
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True)
    credential = dict(line.split("=", 1) for line in result.stdout.decode().splitlines() if "=" in line)
    token = credential["password"]

    def call(path, method="GET", payload=None):
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(API + path, data=data, method=method, headers={
            "Authorization": "Bearer " + token, "Accept": "application/vnd.github+json",
            "Content-Type": "application/json", "X-GitHub-Api-Version": "2022-11-28",
        })
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read()
            return json.loads(raw) if raw else None
    return call


def tunnel_url():
    logs = docker("logs", "--no-color", "cloudflared")
    found = re.findall(r"https://[a-z0-9-]+\.trycloudflare\.com", logs)
    # Compose manda los logs a stdout; no se imprimen logs completos.
    return found[-1] if found else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("action", choices=["start", "restart", "status", "stop"], nargs="?", default="start")
    args = parser.parse_args()
    if args.action == "stop":
        docker("stop", "cloudflared", "webhook-gateway")
        print("Túnel detenido. Datos y servicios del proyecto conservados.")
        return
    if args.action == "status":
        print("URL actual:", tunnel_url() or "no disponible")
        if STATE.exists():
            state = json.loads(STATE.read_text())
            print("URL registrada en GitHub:", state["url"])
        print(docker("ps", "--format", "{{.Service}} {{.State}}", "cloudflared", "webhook-gateway"))
        return
    secret = secret_file()
    if args.action == "restart":
        docker("rm", "-sf", "cloudflared")
    print("Iniciando receptor y túnel…", flush=True)
    docker("up", "-d", "--build", "webhook-gateway", "cloudflared")
    url = None
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        url = tunnel_url()
        if url:
            break
        time.sleep(2)
    if not url:
        raise RuntimeError("No se obtuvo una URL del túnel")
    call = github_client()
    hook_id = json.loads(STATE.read_text())["hook_id"] if STATE.exists() else None
    payload = {"active": True, "events": ["push"], "config": {
        "url": url + "/github-webhook/", "content_type": "json", "secret": secret, "insecure_ssl": "0",
    }}
    if hook_id:
        try:
            hook = call(f"/hooks/{hook_id}", "PATCH", payload)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            hook_id = None
    if not hook_id:
        hook = call("/hooks", "POST", {"name": "web", **payload})
    fd = os.open(STATE, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, "w") as stream:
        json.dump({"hook_id": hook["id"], "url": payload["config"]["url"]}, stream, indent=2)
    call(f"/hooks/{hook['id']}/pings", "POST", {})
    print("Túnel:", url)
    print("Webhook GitHub actualizado; ping solicitado. ID:", hook["id"])
    print("Jenkins sigue disponible localmente en http://127.0.0.1:8080")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # Los errores HTTP y subprocess pueden contener secretos o payloads.
        print("No se completó la operación:", type(error).__name__, getattr(error, "code", ""))
        raise SystemExit(1)
