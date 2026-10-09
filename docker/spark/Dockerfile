FROM python:3.11-slim-bookworm@sha256:0a310eeecf4e1f5a0743f9a6520c90c88d089c903ca5fd283f501e3a805f5f89
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/java-17-openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:$PATH PYTHONPATH=/app
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && ln -s /usr/lib/jvm/java-17-openjdk-$(dpkg --print-architecture) /usr/lib/jvm/java-17-openjdk \
    && rm -rf /var/lib/apt/lists/*
COPY docker/spark/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt \
    && ln -s /usr/local/lib/python3.11/site-packages/pyspark /opt/spark \
    && useradd --create-home --uid 1000 app \
    && mkdir -p /app /tmp/spark /opt/spark/work \
    && chown -R app:app /app /tmp/spark /opt/spark/work
COPY docker/spark/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY processing /app/processing
COPY tests /app/tests
WORKDIR /app
USER app
EXPOSE 7077 8080 8081
CMD ["/opt/spark/bin/spark-class", "org.apache.spark.deploy.master.Master", "--host", "spark-master"]
