"""Gold Tarea 3: silver.post_hist + silver.users_hist -> gold.cant_post_x_user_hist (MERGE)."""
from pyspark.sql.functions import (current_date, col, sum as _sum, count,
                                   explode, split, regexp_replace, trim)
from spark_common import get_spark, init_schema


def run_gold():
    spark = get_spark("gold-metrics")
    init_schema(spark)
    posts = spark.table("nessie.silver.post_hist").filter(col("OwnerUserId").isNotNull())
    users = spark.table("nessie.silver.users_hist")
    # ponytail: por usuario POSTEADOR (posts agrupados + nombre desde users); coincide con Trino-replica
    agg = (posts.groupBy("OwnerUserId")
           .agg(count("*").alias("cant_posts"),
                _sum((col("PostTypeId") == 1).cast("int")).alias("cant_preguntas"),
                _sum((col("PostTypeId") == 2).cast("int")).alias("cant_respuestas"))
           .withColumnRenamed("OwnerUserId", "user_id"))
    gold = (agg.join(users.select(col("Id").alias("user_id"), "DisplayName"),
                     on="user_id", how="left")
            .select(col("user_id"), col("DisplayName").alias("display_name"),
                    "cant_posts", "cant_preguntas", "cant_respuestas")
            .withColumn("fecha_cargue", current_date()))
    gold.createOrReplaceTempView("src")
    spark.sql("""CREATE TABLE IF NOT EXISTS nessie.gold.cant_post_x_user_hist (
      user_id BIGINT, display_name STRING, cant_posts BIGINT, cant_preguntas BIGINT,
      cant_respuestas BIGINT, fecha_cargue DATE) USING iceberg""")
    spark.sql("""MERGE INTO nessie.gold.cant_post_x_user_hist t USING src s ON t.user_id = s.user_id
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    print("gold OK:",
          spark.sql("SELECT count(*), sum(cant_posts) FROM nessie.gold.cant_post_x_user_hist").collect()[0])
    # milla extra: top_tags gratis con mismo post_hist (Tags trae <t> o |t| segun espejo)
    # ponytail: split+trim con funciones (no SQL en string: el escape '\\\\s+' rompia el
    # split, dejaba espacios pegados -> tags duplicados -> MERGE_CARDINALITY_VIOLATION en rerun)
    spark.sql("""CREATE TABLE IF NOT EXISTS nessie.gold.top_tags
      (tag STRING, n BIGINT, fecha_cargue DATE) USING iceberg""")
    (spark.table("nessie.silver.post_hist").filter(col("Tags").isNotNull())
     .select(explode(split(regexp_replace(col("Tags"), "[<>|]", " "), "\\s+")).alias("tag"))
     .select(trim(col("tag")).alias("tag")).filter("tag <> ''")
     .groupBy("tag").count().withColumnRenamed("count", "n")
     .orderBy(col("n").desc()).limit(50)
     .withColumn("fecha_cargue", current_date())
     .createOrReplaceTempView("tags"))
    spark.sql("""MERGE INTO nessie.gold.top_tags t USING tags s ON t.tag = s.tag
      WHEN MATCHED THEN UPDATE SET * WHEN NOT MATCHED THEN INSERT *""")
    print("top_tags OK")
    spark.stop()


if __name__ == "__main__":
    run_gold()
