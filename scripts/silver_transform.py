"""Silver Tarea 2: Bronze users -> nessie.silver.users_hist (Iceberg MERGE + fecha_cargue)."""
from pyspark.sql.functions import current_date, col, trim
from spark_common import get_spark, init_schema
import os


def _str(c):
    return trim(col(c).cast("string")).alias(c)


def run_silver():
    spark = get_spark("silver-users-hist")
    init_schema(spark)
    bronze = f"s3a://{os.getenv('S3_BUCKET', 'lakehouse')}/bronze/users/"
    raw = spark.read.parquet(bronze + "2020/", bronze + "2021/")
    n_in = raw.count()
    # ponytail: la muestra trae BLOB en textos -> cast a string; tipos BIGINT reales
    df = raw.select(col("Id").cast("bigint").alias("Id"),
                    col("Reputation").cast("string").alias("Reputation"),
                    col("CreationDate").cast("timestamp").alias("CreationDate"),
                    _str("DisplayName"),
                    col("LastAccessDate").cast("timestamp").alias("LastAccessDate"),
                    col("Views").cast("bigint").alias("Views"),
                    col("UpVotes").cast("bigint").alias("UpVotes"),
                    col("DownVotes").cast("bigint").alias("DownVotes"),
                    col("Location").cast("string").alias("Location"),
                    col("AccountId").cast("bigint").alias("AccountId"))
    df = df.dropDuplicates(["Id"]).filter(col("Id").isNotNull())
    df = df.withColumn("fecha_cargue", current_date())
    n_clean = df.count()
    print(f"DQ silver: leidas={n_in} limpias={n_clean} dups_nulos={n_in - n_clean}")
    df.createOrReplaceTempView("src")
    spark.sql("""CREATE TABLE IF NOT EXISTS nessie.silver.users_hist (
      Id BIGINT, Reputation STRING, CreationDate TIMESTAMP, DisplayName STRING,
      LastAccessDate TIMESTAMP, Views BIGINT, UpVotes BIGINT, DownVotes BIGINT,
      Location STRING, AccountId BIGINT, fecha_cargue DATE)
      USING iceberg PARTITIONED BY (years(CreationDate))""")
    spark.sql("""MERGE INTO nessie.silver.users_hist t USING src s ON t.Id = s.Id
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    print("users_hist merge OK:",
          spark.sql("SELECT count(*) FROM nessie.silver.users_hist").collect()[0][0])
    spark.stop()


def build_post_hist_manual():
    """Corre UNA vez antes de la demo: post 2020+2021 -> post_hist."""
    spark = get_spark("silver-post-hist-manual")
    init_schema(spark)
    b = f"s3a://{os.getenv('S3_BUCKET', 'lakehouse')}/bronze/post/"
    raw = spark.read.parquet(b + "2020/", b + "2021/")
    from pyspark.sql.functions import current_date
    df = raw.select(col("Id").cast("bigint").alias("Id"),
                    col("PostTypeId").cast("bigint").alias("PostTypeId"),
                    col("CreationDate").cast("timestamp").alias("CreationDate"),
                    col("Score").cast("bigint").alias("Score"),
                    col("ViewCount").cast("bigint").alias("ViewCount"),
                    col("OwnerUserId").cast("bigint").alias("OwnerUserId"),
                    col("Title").cast("string").alias("Title"),
                    col("Tags").cast("string").alias("Tags"),
                    col("AnswerCount").cast("bigint").alias("AnswerCount"),
                    col("CommentCount").cast("bigint").alias("CommentCount"))
    df = df.dropDuplicates(["Id"]).filter("Id IS NOT NULL")
    df = df.withColumn("fecha_cargue", current_date())
    df.createOrReplaceTempView("src")
    spark.sql("""CREATE TABLE IF NOT EXISTS nessie.silver.post_hist (
      Id BIGINT, PostTypeId BIGINT, CreationDate TIMESTAMP, Score BIGINT,
      ViewCount BIGINT, OwnerUserId BIGINT, Title STRING, Tags STRING,
      AnswerCount BIGINT, CommentCount BIGINT, fecha_cargue DATE) USING iceberg""")
    spark.sql("""MERGE INTO nessie.silver.post_hist t USING src s ON t.Id = s.Id
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    print("post_hist OK:",
          spark.sql("SELECT count(*) FROM nessie.silver.post_hist").collect()[0][0])
    spark.stop()


if __name__ == "__main__":
    run_silver()
