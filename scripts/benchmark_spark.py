"""Ejecuta dos configuraciones Spark sobre la exportación común de Dask."""
import base64
import json
from pathlib import Path
import subprocess
import time
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
    print("Construyendo Spark; se conserva la exportación usada por Dask…", flush=True)
    run("build", "spark-master")
    reports = []
    try:
        run("stop", "dask-worker-1", "dask-worker-2", "dask-worker-3", "spark-worker-2")
        run("up", "-d", "--no-deps", "--force-recreate", "--wait", "spark-master")
        for count in [1, 2]:
            selected = ["spark-worker"] + (["spark-worker-2"] if count == 2 else [])
            print(f"Midiendo Spark con {count} worker(s)…", flush=True)
            run("up", "-d", "--no-deps", "--force-recreate", "--wait", *selected)
            for attempt in range(30):
                with urllib.request.urlopen("http://127.0.0.1:8081/json/", timeout=10) as response:
                    active = [w for w in json.load(response)["workers"] if w["state"] == "ALIVE"]
                if len(active) == count:
                    if any(w["cores"] != 1 or w["memory"] != 1024 for w in active):
                        raise RuntimeError("Recursos Spark inesperados")
                    break
                time.sleep(1)
            else:
                raise RuntimeError("Workers Spark no registrados")
            output = run("run", "--rm", "-T", "--no-deps", "--use-aliases", "spark-benchmark", "/app/benchmark/spark_grid.py", "--workers", str(count))
            report = json.loads(next(line.removeprefix("BENCH_RESULT ") for line in output.splitlines() if line.startswith("BENCH_RESULT ")))
            peaks = {service: int(run("exec", "-T", service, "python", "-c", "from pathlib import Path; print(Path('/sys/fs/cgroup/memory.peak').read_text())")) for service in selected}
            report.update(worker_peak_bytes=peaks, sum_worker_peaks_bytes=sum(peaks.values()), worker_container_limit_bytes=1536 * 1024**2)
            # Guardar también el reporte completo en el volumen compartido.
            subprocess.run(COMPOSE + ["run", "--rm", "-T", "--no-deps", "ingestion", "python", "-c",
                           f"from pathlib import Path; import sys; Path('/data/benchmark/spark-{count}.json').write_text(sys.stdin.read())"],
                           cwd=ROOT, input=(json.dumps(report, indent=2) + "\n").encode(), check=True,
                           stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            reports.append(report)
            print(json.dumps(report), flush=True)
        assert reports[0]["input_records_sha256"] == reports[1]["input_records_sha256"]
        assert reports[0]["output_sha256"] == reports[1]["output_sha256"]
        (ROOT / "docs/benchmark-spark.json").write_text(json.dumps(reports, indent=2) + "\n")
    finally:
        run("stop", "spark-worker-2")
        run("up", "-d", "--wait", "dask-worker-1", "dask-worker-2")


if __name__ == "__main__":
    try:
        main()
    except subprocess.CalledProcessError as error:
        print(error.stderr.decode()[-6000:])
        raise SystemExit(1)
    except Exception as error:
        print("Medición Spark interrumpida:", type(error).__name__)
        raise SystemExit(1)
