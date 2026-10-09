"""Ejecuta dos configuraciones Dask y restaura los servicios normales."""
import base64
import json
from pathlib import Path
import subprocess
import urllib.error
import urllib.request

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.benchmark.yml"]


def run(*args):
    return subprocess.run(COMPOSE + list(args), cwd=ROOT, check=True,
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.decode()


def main():
    password = (ROOT / "secrets/jenkins/admin_password").read_text().strip()
    auth = "Basic " + base64.b64encode(("Andres:" + password).encode()).decode()
    for job in ["projectbd", "projectbd-ingestion"]:
        request = urllib.request.Request(f"http://127.0.0.1:8080/job/{job}/api/json", headers={"Authorization": auth})
        try:
            with urllib.request.urlopen(request, timeout=10) as response:
                status = json.load(response)
        except urllib.error.HTTPError as error:
            # En una instalación nueva puede no existir el job histórico.
            if job == "projectbd-ingestion" and error.code == 404:
                continue
            raise
        if status["inQueue"]:
            raise RuntimeError("Hay un pipeline en cola")
        if status["lastBuild"]:
            with urllib.request.urlopen(urllib.request.Request(status["lastBuild"]["url"] + "api/json", headers={"Authorization": auth}), timeout=10) as response:
                if json.load(response)["building"]:
                    raise RuntimeError("Hay un pipeline ejecutándose")
    print("Construyendo Dask y exportando la entrada común…", flush=True)
    run("build", "dask-scheduler")
    run("run", "--rm", "-T", "--no-deps", "ingestion", "python", "-m", "benchmark.snapshot")
    reports = []
    try:
        run("stop", "spark-master", "spark-worker", "dask-worker-3")
        run("up", "-d", "--force-recreate", "--wait", "dask-scheduler")
        for count in [2, 3]:
            selected = ["dask-worker-1", "dask-worker-2"] + (["dask-worker-3"] if count == 3 else [])
            print(f"Midiendo Dask con {count} workers…", flush=True)
            run("up", "-d", "--no-deps", "--force-recreate", *selected)
            output = run("run", "--rm", "-T", "--no-deps", "ingestion", "python", "-m", "benchmark.dask_grid", "--workers", str(count))
            report = json.loads(next(line.removeprefix("BENCH_RESULT ") for line in output.splitlines() if line.startswith("BENCH_RESULT ")))
            reports.append(report)
            print(json.dumps(report), flush=True)
        assert reports[0]["input_records_sha256"] == reports[1]["input_records_sha256"]
        assert reports[0]["output_sha256"] == reports[1]["output_sha256"]
        (ROOT / "docs/benchmark-dask.json").write_text(json.dumps(reports, indent=2) + "\n")
    finally:
        run("stop", "dask-worker-3")
        run("up", "-d", "--wait", "spark-master", "spark-worker")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Medición Dask interrumpida:", type(error).__name__)
        raise SystemExit(1)
