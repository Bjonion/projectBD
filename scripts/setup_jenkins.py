"""Completa el asistente local de Jenkins sin imprimir contraseñas.

Requiere únicamente Python estándar y los contenedores ya iniciados.
La contraseña generada queda en secrets/jenkins/admin_password (modo 600).
"""

import argparse
import base64
import http.cookiejar
import json
import os
from pathlib import Path
import secrets
import subprocess
import urllib.error
import urllib.parse
import urllib.request


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--username", default="Andres")
    parser.add_argument("--email", default="agomezp@correo.iue.edu.co")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    base = "http://127.0.0.1:8080"
    folder = root / "secrets" / "jenkins"
    folder.mkdir(parents=True, mode=0o700, exist_ok=True)
    folder.parent.chmod(0o700)
    folder.chmod(0o700)
    password_file = folder / "admin_password"
    if password_file.exists():
        password_file.chmod(0o600)
        password = password_file.read_text().strip()
    else:
        password = secrets.token_urlsafe(32)
        fd = os.open(password_file, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, "w") as stream:
            stream.write(password + "\n")

    opener = urllib.request.build_opener(
        urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar())
    )

    def call(path, username, key, fields=None, crumb=None):
        encoded = base64.b64encode((username + ":" + key).encode()).decode()
        headers = {"Authorization": "Basic " + encoded}
        if crumb:
            headers[crumb["crumbRequestField"]] = crumb["crumb"]
        data = None
        if fields is not None:
            data = urllib.parse.urlencode(fields).encode()
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        request = urllib.request.Request(base + path, data=data, headers=headers)
        with opener.open(request, timeout=30) as response:
            payload = response.read()
        return json.loads(payload) if payload else {}

    try:
        identity = call("/whoAmI/api/json", args.username, password)
        if identity.get("authenticated") and identity.get("name") == args.username:
            print("Administrador existente verificado; contraseña local conservada.")
            return
    except urllib.error.HTTPError as error:
        if error.code not in (401, 403):
            raise

    initial = subprocess.check_output(
        ["docker", "compose", "exec", "-T", "jenkins", "cat",
         "/var/jenkins_home/secrets/initialAdminPassword"],
        cwd=root, text=True, stderr=subprocess.DEVNULL,
    ).strip()
    crumb = call("/crumbIssuer/api/json", "admin", initial)
    result = call(
        "/setupWizard/createAdminUser", "admin", initial,
        fields={"username": args.username, "password1": password,
                "password2": password, "fullname": args.username,
                "email": args.email}, crumb=crumb,
    )
    if result.get("status") != "ok":
        raise RuntimeError("Jenkins no confirmó la creación del administrador")
    crumb = call("/crumbIssuer/api/json", args.username, password)
    for path, fields in (
        ("/setupWizard/configureInstance", {"rootUrl": base + "/"}),
        ("/setupWizard/completeInstall", {}),
    ):
        result = call(path, args.username, password, fields=fields, crumb=crumb)
        if result.get("status") != "ok":
            raise RuntimeError("Jenkins no confirmó la configuración del asistente")
    identity = call("/whoAmI/api/json", args.username, password)
    if not identity.get("authenticated") or identity.get("name") != args.username:
        raise RuntimeError("No se pudo verificar el administrador")
    print("Jenkins configurado. Contraseña guardada localmente; no se imprime.")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        # No imprimir respuestas, headers ni excepciones que puedan contener secretos.
        print("Configuración Jenkins fallida:", type(error).__name__)
        raise SystemExit(1)
