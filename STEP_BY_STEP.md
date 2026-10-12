# Paso a paso — correrlo desde cero (cualquier persona)

Tiempo estimado: ~30 min (la mitad es esperar que suban los servicios).

## 0. Prerrequisitos

* Docker Desktop con **16 GB de RAM** asignada (con 8 GB Spark se pega).
* Puertos libres: `9000, 9001, 19120, 7077, 8081, 8083, 8082, 8888, 9047, 31010`.
* Git y Python 3.11 (solo para `dremio_setup.py`, que es stdlib).

## 1. Clonar y configurar secrets

```bash
git clone https://github.com/nicothinn/lakehouse-medallion.git
cd lakehouse-medallion
git checkout develop
cp .dlt/secrets.toml.example .dlt/secrets.toml   # NO se commitea
```

Qué configura cada cosa (no hay más):
| Qué | Dónde | Default |
|---|---|---|
| Creds MinIO para dlt/boto3 | `.dlt/secrets.toml` | admin / admin123 |
| Endpoint, Nessie, bucket, Spark | variables en `docker-compose.yml` | `minio:9000`, `nessie:19120`, `lakehouse` |
| Catálogo Trino → Nessie | `trino-catalog/iceberg.properties` | API v1 (Trino 446 la exige) |

## 2. Levantar el stack

```bash
docker compose up -d --build
docker ps --format "{{.Names}} {{.Status}}"
```

Espera a ver `healthy` en `p2-minio`, `p2-jupyter`, `p2-trino`, `p2-airflow` (Airflow tarda ~2 min la primera vez). Dremio (`p2-dremio`) tarda ~1 min en responder en `:9047`.

## 3. Subir Bronze a MinIO (crea el bucket solo)

Los `data/*.parquet` ya vienen en el repo; si faltaran se regeneran con `python scripts/sample_bronze.py` (necesita `duckdb` + internet, ~10 min).

```bash
docker exec p2-jupyter bash -c "MINIO_ENDPOINT=http://minio:9000 python /home/jovyan/scripts/upload_bronze.py"
```

Esperado: `bucket creado: lakehouse` + 4 líneas `OK bronze/...` + `BRONZE UPLOAD DONE`. Verifica en MinIO `:9001` que el bucket tenga `bronze/post/2020|2021` y `bronze/users/2020|2021`.

## 4. Silver `post_hist` (manual, una sola vez)

```bash
docker exec p2-airflow python /opt/airflow/scripts/_run_posthist.py
```

Esperado: `post_hist OK: 300000`. Es idempotente (MERGE): re-correr no duplica.

## 5. Play en Airflow (el pipeline en vivo)

1. Abre `:8083` (admin/admin), activa el DAG `lakehouse_medallion` y dale **Play**.
2. Verás en orden: `bronze_users_2021` (DLT, 100000 filas, overwrite) → `silver_users_hist` (Spark MERGE, 200000) → `gold_metrics` (Spark MERGE, 180188/SUM 300000 + `top_tags` 50).
3. Todo verde en ~2 min. Los jobs se ven corriendo en Spark UI `:8081` (app `gold-metrics`, etc.).

Si Airflow fallara, el respaldo son los notebooks en orden: `bronze_ingest.ipynb` → `silver_transform.ipynb` → `gold_agg.ipynb`.

## 6. Consumo: Trino y Dremio (deben dar lo mismo)

```bash
python scripts/dremio_setup.py   # crea admin + source nessie en Dremio, corre checks
```

* Trino `:8082`: corre `sql/trino_replicate_gold.sql` → `180188,300000`.
* Dremio `:9047` (admin/Admin123!): corre `sql/dremio_gold_checks.sql` → mismos números.

Si Trino dice `API version mismatch`, revisa que el catálogo use `/api/v1` (ver `trino-catalog/iceberg.properties`).

## 7. Apagar

```bash
docker compose down        # apaga, conserva datos (volúmenes)
docker compose down -v     # apaga y BORRA todo (demo desde cero)
```
