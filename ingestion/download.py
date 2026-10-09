"""Descarga completa mediante la API HTTPS de Kaggle; token recibido por stdin."""

import argparse
import hashlib
import json
from pathlib import Path
import shutil
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
import zipfile

DATASET = "sobhanmoosavi/us-accidents"
CSV_NAME = "US_Accidents_March23.csv"


class SafeRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        target = urllib.parse.urlparse(newurl)
        if target.scheme != "https":
            raise RuntimeError("Se rechazó una redirección sin HTTPS")
        redirected = super().redirect_request(req, fp, code, msg, headers, newurl)
        if redirected and target.hostname != urllib.parse.urlparse(req.full_url).hostname:
            redirected.remove_header("Authorization")
        return redirected


def sha256(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def download(token, directory):
    if not token or not token.startswith("KGAT_"):
        raise ValueError("No se recibió un token de acceso Kaggle válido por stdin")
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    archive_path = directory / "us-accidents.zip"
    csv_path = directory / CSV_NAME
    manifest_path = directory / "source.json"
    opener = urllib.request.build_opener(SafeRedirect())
    headers = {"User-Agent": "projectBD/1.0", "Authorization": "Bearer " + token}
    request = urllib.request.Request(
        "https://www.kaggle.com/api/v1/datasets/view/" + DATASET, headers=headers
    )
    with opener.open(request, timeout=60) as response:
        metadata = json.load(response)
    version = metadata.get("currentVersionNumber") or metadata.get("versionNumber")
    updated = metadata.get("lastUpdated")
    if manifest_path.exists() and archive_path.exists() and csv_path.exists():
        previous = json.loads(manifest_path.read_text())
        if (previous.get("last_updated") == updated
                and previous.get("version") == version
                and previous.get("zip_sha256") == sha256(archive_path)
                and previous.get("csv_sha256") == sha256(csv_path)):
            print("Descarga completa existente verificada mediante SHA-256.", flush=True)
            return previous
    url = "https://www.kaggle.com/api/v1/datasets/download/" + DATASET
    if version:
        url += "?" + urllib.parse.urlencode({"datasetVersionNumber": version})
    request = urllib.request.Request(url, headers=headers)
    partial = archive_path.with_suffix(".zip.part")
    started = time.perf_counter()
    downloaded = 0
    announced = 0
    with opener.open(request, timeout=120) as response, partial.open("wb") as output:
        for chunk in iter(lambda: response.read(4 * 1024 * 1024), b""):
            output.write(chunk)
            downloaded += len(chunk)
            if downloaded - announced >= 100 * 1024 * 1024:
                print("Descarga: %d MiB" % (downloaded // 1024**2), flush=True)
                announced = downloaded
    temporary_csv = csv_path.with_suffix(".csv.part")
    with zipfile.ZipFile(partial) as archive:
        matches = [item for item in archive.infolist() if item.filename == CSV_NAME]
        if len(matches) != 1 or matches[0].file_size < 1_000_000_000:
            raise ValueError("El ZIP no contiene el CSV completo esperado de al menos 1 GB")
        with archive.open(matches[0]) as source, temporary_csv.open("wb") as output:
            shutil.copyfileobj(source, output, length=4 * 1024 * 1024)
    # Se abre solo el miembro esperado; no se extraen rutas arbitrarias del ZIP.
    partial.replace(archive_path)
    temporary_csv.replace(csv_path)
    manifest = {
        "dataset": DATASET, "version": version, "last_updated": updated,
        "csv_name": CSV_NAME, "csv_bytes": csv_path.stat().st_size,
        "zip_bytes": downloaded, "zip_sha256": sha256(archive_path),
        "csv_sha256": sha256(csv_path),
        "download_extract_seconds": round(time.perf_counter() - started, 3),
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    print("Archivo completo descargado y extraído: %d bytes CSV." % manifest["csv_bytes"], flush=True)
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--directory", default="/data/raw")
    parser.add_argument("--token-stdin", action="store_true", required=True)
    args = parser.parse_args()
    try:
        download(sys.stdin.read().strip(), args.directory)
    except urllib.error.HTTPError as error:
        print("Descarga Kaggle fallida: HTTP %s" % error.code, file=sys.stderr)
        raise SystemExit(1)
    except Exception as error:
        # No se imprime el mensaje, URL firmada, headers ni credencial.
        print("Descarga Kaggle fallida: %s" % type(error).__name__, file=sys.stderr)
        raise SystemExit(1)


if __name__ == "__main__":
    main()
