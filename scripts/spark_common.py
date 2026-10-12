"""Sesion Spark + Iceberg + Nessie + MinIO (usada por DAG y notebooks)."""
import os
from pyspark.sql import SparkSession

MINIO = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
NESSIE = os.getenv("NESSIE_URI", "http://nessie:19120/api/v2")
BUCKET = os.getenv("S3_BUCKET", "lakehouse")
# ponytail: contra el master del compose para verse en Spark UI; local si no responde
MASTER = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
# ponytail: jars segun Spark real (3.4 stack activo, 3.5 compose entrega)
SV = os.getenv("SPARK_VERSION", "3.5")
PKGS = (f"org.apache.iceberg:iceberg-spark-runtime-{SV}_2.12:1.5.2,"
        f"org.projectnessie.nessie-integrations:nessie-spark-extensions-{SV}_2.12:0.102.0,"
        "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262")


def get_spark(app="lakehouse"):
    base = (SparkSession.builder.appName(app)
            .config("spark.sql.extensions",
                    "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                    "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")
            .config("spark.sql.catalog.nessie", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
            .config("spark.sql.catalog.nessie.uri", NESSIE)
            .config("spark.sql.catalog.nessie.ref", "main")
            .config("spark.sql.catalog.nessie.warehouse", f"s3a://{BUCKET}/warehouse/")
            .config("spark.sql.catalog.nessie.s3.endpoint", MINIO)
            .config("spark.sql.catalog.nessie.s3.path-style-access", "true")
            .config("spark.hadoop.fs.s3a.endpoint", MINIO)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("MINIO_USER", "admin"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("MINIO_PASSWORD", "admin123"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.jars.packages", PKGS))
    if MASTER and MASTER != "local":
        try:
            return base.master(MASTER).config("spark.executor.memory", "2g").getOrCreate()
        except Exception as e:  # master caido -> local, el DAG no se cae
            print(f"master {MASTER} no responde ({e}); modo local")
    return base.master("local[*]").getOrCreate()


def init_schema(spark):
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.silver")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS nessie.gold")
