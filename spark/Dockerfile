FROM apache/spark:4.0.0

USER root

# Install Python packages required by Spark jobs
RUN pip install --no-cache-dir psycopg2-binary

# Pre-download all required JARs into /opt/spark/jars/ so spark-submit needs no --packages flag.
# We run a minimal PySpark job with --packages so Spark's Ivy resolver fetches everything
# including transitive dependencies, then copy the resolved JARs into the classpath directory.
RUN echo "from pyspark.sql import SparkSession; SparkSession.builder.getOrCreate().stop()" > /tmp/warm.py \
    && HOME=/tmp /opt/spark/bin/spark-submit \
        --master "local[1]" \
        --conf spark.jars.ivy=/tmp/.ivy \
        --packages "io.delta:delta-spark_2.13:4.0.0,\
org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0,\
org.apache.hadoop:hadoop-aws:3.4.0,\
com.amazonaws:aws-java-sdk-bundle:1.12.720" \
        /tmp/warm.py \
    && cp /tmp/.ivy/jars/*.jar /opt/spark/jars/ \
    && rm -rf /tmp/.ivy /tmp/warm.py

USER spark
