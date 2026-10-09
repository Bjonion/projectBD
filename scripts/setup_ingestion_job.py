"""Crea el job de ingesta con el Jenkinsfile; permite validar código local sin commit.

La opción --local-source copia únicamente archivos de código conocidos al
workspace Jenkins y omite checkout solo en la ejecución de desarrollo.
"""

import argparse
import base64
import http.cookiejar
import io
import json
from pathlib import Path
import subprocess
import tarfile
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET

JOB = "projectbd-ingestion"


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--local-source", action="store_true")
    parser.add_argument("--run", action="store_true")
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    password = (root / "secrets/jenkins/admin_password").read_text().strip()
    auth = "Basic " + base64.b64encode(("Andres:" + password).encode()).decode()
    opener = urllib.request.build_opener(urllib.request.HTTPCookieProcessor(http.cookiejar.CookieJar()))
    base = "http://127.0.0.1:8080"

    def call(path, data=None, content_type=None):
        headers = {"Authorization": auth}
        if data is not None:
            request = urllib.request.Request(base + "/crumbIssuer/api/json", headers=headers)
            with opener.open(request, timeout=30) as response:
                crumb = json.load(response)
            headers[crumb["crumbRequestField"]] = crumb["crumb"]
        if content_type:
            headers["Content-Type"] = content_type
        with opener.open(urllib.request.Request(base + path, data=data, headers=headers), timeout=30) as response:
            return response.read()

    if args.local_source:
        allowed = [".dockerignore", "docker-compose.yml", "Jenkinsfile", "docker", "ingestion", "tests"]
        token = (root / "secrets/kaggle/access_token").read_bytes().strip()
        buffer = io.BytesIO()
        with tarfile.open(fileobj=buffer, mode="w") as archive:
            for name in allowed:
                path = root / name
                files = sorted(path.rglob("*")) if path.is_dir() else [path]
                for file in files:
                    if file.is_file() and "__pycache__" not in file.parts:
                        if token in file.read_bytes():
                            raise RuntimeError("Se detectó un secreto en los archivos de código")
                        archive.add(file, arcname=str(file.relative_to(root)), recursive=False)
        command = ["docker", "compose", "exec", "-T", "jenkins", "sh", "-c",
                   "mkdir -p /var/jenkins_home/workspace/" + JOB
                   + " && tar -x -C /var/jenkins_home/workspace/" + JOB]
        subprocess.run(command, input=buffer.getvalue(), cwd=root, check=True)

    flow = ET.Element("flow-definition", plugin="workflow-job")
    ET.SubElement(flow, "description").text = "Descarga e ingesta de projectBD; código definido en Jenkinsfile."
    properties = ET.SubElement(flow, "properties")
    definitions = ET.SubElement(ET.SubElement(properties, "hudson.model.ParametersDefinitionProperty"), "parameterDefinitions")
    checkout = ET.SubElement(definitions, "hudson.model.BooleanParameterDefinition")
    ET.SubElement(checkout, "name").text = "CHECKOUT_REPOSITORY"
    ET.SubElement(checkout, "defaultValue").text = "true"
    branch = ET.SubElement(definitions, "hudson.model.StringParameterDefinition")
    ET.SubElement(branch, "name").text = "GIT_BRANCH"
    ET.SubElement(branch, "defaultValue").text = "Andres"
    definition = ET.SubElement(flow, "definition", {"class": "org.jenkinsci.plugins.workflow.cps.CpsFlowDefinition", "plugin": "workflow-cps"})
    ET.SubElement(definition, "script").text = (root / "Jenkinsfile").read_text()
    ET.SubElement(definition, "sandbox").text = "true"
    ET.SubElement(flow, "disabled").text = "false"
    xml = ET.tostring(flow, encoding="utf-8")
    try:
        call("/job/" + JOB + "/api/json")
        endpoint = "/job/" + JOB + "/config.xml"
    except urllib.error.HTTPError as error:
        if error.code != 404:
            raise
        endpoint = "/createItem?name=" + JOB
    call(endpoint, data=xml, content_type="application/xml; charset=utf-8")
    print("Job Jenkins configurado:", JOB)
    if args.run:
        parameters = {"CHECKOUT_REPOSITORY": "false" if args.local_source else "true", "GIT_BRANCH": "Andres"}
        call("/job/" + JOB + "/buildWithParameters", data=urllib.parse.urlencode(parameters).encode(),
             content_type="application/x-www-form-urlencoded")
        print("Ejecución solicitada. Fuente:", "código local revisable" if args.local_source else "GitHub")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("Configuración del job fallida:", type(error).__name__)
        raise SystemExit(1)
