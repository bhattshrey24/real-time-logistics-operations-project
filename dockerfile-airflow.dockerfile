FROM apache/airflow:2.9.2

USER root
# Install Docker CLI so BashOperator can run 'docker exec' to submit Spark jobs
# into the spark-master container (JARs are pre-baked there, so no --packages needed)
RUN apt-get update \
    && apt-get install -y --no-install-recommends docker.io \
    && rm -rf /var/lib/apt/lists/*
USER airflow
