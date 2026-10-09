FROM python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 PYTHONPATH=/app
WORKDIR /app
COPY docker/dask/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && useradd --create-home --uid 1000 app \
    && mkdir -p /data && chown app:app /app /data
COPY ingestion /app/ingestion
COPY benchmark /app/benchmark
COPY tests /app/tests
USER app
EXPOSE 8786 8787
CMD ["dask", "scheduler", "--host", "0.0.0.0"]
