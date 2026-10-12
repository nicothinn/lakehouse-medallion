FROM jupyter/pyspark-notebook:spark-3.5.0
# ponytail: una sola imagen para jupyter + airflow worker (pyspark, dlt, iceberg)
USER root
RUN pip install --no-cache-dir "dlt[s3,parquet]" pyiceberg[s3,sql] minio clickhouse-connect duckdb pandas "pyarrow==17.0.0" boto3 trino pyspark==3.5.0 2>&1 | tail -1
# Airflow standalone liviano para demo (mismo contenedor base)
RUN pip install --no-cache-dir "apache-airflow==2.9.3" --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt" 2>&1 | tail -1
# ponytail: constraints deja typing_extensions viejo y pydantic_core nuevo lo rompe
RUN pip install --no-cache-dir -U typing_extensions 2>&1 | tail -1
USER ${NB_UID}
ENV SPARK_EXTRA_PACKAGES="org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.102.0,org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
