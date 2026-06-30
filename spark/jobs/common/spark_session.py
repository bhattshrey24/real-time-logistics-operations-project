# ─────────────────────────────────────────────────────────────────────────────
# SparkSession factory used by every Spark job in this project.
# Pre-configures Delta Lake extensions and MinIO as the S3-compatible store.
# ─────────────────────────────────────────────────────────────────────────────

from pyspark.sql import SparkSession
import config


def get_spark(app_name: str) -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)

        # Delta Lake
        .config("spark.sql.extensions",            "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",  "org.apache.spark.sql.delta.catalog.DeltaCatalog")

        # MinIO — path-style access required (MinIO doesn't use virtual-hosted buckets)
        .config("spark.hadoop.fs.s3a.endpoint",          config.MINIO_ENDPOINT)
        .config("spark.hadoop.fs.s3a.access.key",        config.MINIO_ACCESS_KEY)
        .config("spark.hadoop.fs.s3a.secret.key",        config.MINIO_SECRET_KEY)
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl",              "org.apache.hadoop.fs.s3a.S3AFileSystem")

        .getOrCreate()
    )
