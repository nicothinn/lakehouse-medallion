# Proyecto 2 — Lakehouse Medallion (StackOverflow)

StackOverflow 2020-2021: Bronze Parquet (overwrite) → Silver Iceberg (merge) → Gold Iceberg (merge).
Pipeline en clase: `users_2021` por Airflow; resto precargado manual (ver rúbrica, última sección).

## Estado verificado (stack activo, 2026-10-12)

| Capa | Tabla / ruta | Filas | Prueba |
|---|---|---|---|
| Bronze | `s3://lakehouse/bronze/{post,users}/{2020,2021}/` | 150k+150k posts, 100k+100k users | PUT overwrite, rerun OK |
| Silver | `silver.post_hist` / `silver.users_hist` (+`fecha_cargue`) | 300000 / 200000 | rerun → 0 nuevas (merge, no duplica) |
| Gold | `gold.cant_post_x_user_hist` (+`fecha_cargue`) | 180188, SUM=300000 | réplica desde silver **match exacto** |
| Gold extra | `gold.top_tags` | 50 únicas, 0 dups (python 20397, javascript 14727, …) | rerun 2× OK (fix split+trim 2026-10-12) |

Nota muestreo: `users_hist` solo trae usuarios *creados* 2020-2021; los top posteadores son
más viejos → `display_name` NULL en esos casos. Los conteos cuadran igual (replica==gold).

## Quickstart entrega (compose propio)

```bash
docker compose up -d --build
# MinIO :9001 (admin/admin123): crear bucket `lakehouse`
cp .dlt/secrets.toml.example .dlt/secrets.toml  # secrets dlt (no se commitea)
python scripts/sample_bronze.py   # muestra Bronze a data/
python scripts/upload_bronze.py   # sube a s3://lakehouse/bronze/ (overwrite)
# Airflow :8080 -> Play DAG `lakehouse_medallion` (bronze DLT -> silver MERGE -> gold MERGE)
```

## Demo sobre el stack ya activo (sin tumbar P1)

Puertos ocupados por P1 (mismos que el compose): no se levanta segundo stack.
Se reutilizan MinIO/Nessie/Jupyter activos (`admin`/`password`, `http://minio:9000`,
`http://nessie:19120/iceberg/main/`). Equivalente PyIceberg del pipeline:

```bash
docker cp scripts/build_silver_gold.py jupyter:/tmp/b.py
docker exec jupyter python /tmp/b.py          # silver + gold (merge, idempotente)
docker cp scripts/verify_gold.py jupyter:/tmp/v.py
docker exec jupyter python /tmp/v.py          # replica silver == gold (match: True)
docker cp scripts/bronze_ingest.py jupyter:/tmp/bi.py
docker exec jupyter python -c "import sys; sys.path.insert(0,'/tmp'); from bronze_ingest import run_bronze; run_bronze()"
```

En el compose de entrega los mismos pasos corren con PySpark (`SPARK_VERSION=3.5`;
en el stack activo Spark es 3.4 → `SPARK_VERSION=3.4`).

## Puertos

| Servicio | URL | Creds |
|---|---|---|
| MinIO API/Consola | :9000/:9001 | admin/admin123 (compose) · admin/password (stack P1) |
| Nessie | :19120 | sin auth |
| Airflow | :8080 | admin/admin |
| Jupyter | :8888 | token en logs |
| Spark UI | :8081 | — |
| Dremio | :9047 | primer login crea admin |
| Trino | :8082 | — |

## Consumo

- Trino (`sql/trino_replicate_gold.sql`): replica gold desde silver → 180188 filas, SUM 300000.
  Verificado en P2 `:8082`: gold y réplica dan `180188,300000` (match exacto).
  Ojo: `trino-catalog/iceberg.properties` debe apuntar a `http://nessie:19120/api/v1`
  (Trino 446 pide API v1; con `/api/v2` falla `API version mismatch`).
- Dremio (`sql/dremio_gold_checks.sql`): gold directo, mismos números.
  Verificado en P2 `:9047` (admin/Admin123!): source `nessie` → gold `180188,300000`,
  top_tags igual que Trino. Setup automático idempotente: `python scripts/dremio_setup.py`.
  Nota: Dremio v24 no acepta `storageType` en el config Nessie (se omite); el S3 se
  apunta al endpoint interno `minio:9000` con `dremio.s3.compat=true`.
- Sin Trino/Dremio a la mano: `scripts/verify_gold.py` corre la misma lógica (match: True).

## Fallback sin Airflow

`notebooks/bronze_ingest.ipynb` → `silver_transform.ipynb` → `gold_agg.ipynb`, en orden.

## Sustentación (preguntas fijas del profe, respuesta en una línea)

- ¿Por qué Parquet en Bronze? Columnar + comprimido para el crudo analítico; overwrite reescribe el día.
- ¿Por qué Iceberg en Silver/Gold y no Parquet? Sin table format no hay tabla SQL/ACID/merge; Iceberg = lista de archivos + metadatos en Nessie.
- ¿Dónde están los secrets? `.dlt/secrets.toml` (montado en Jupyter/Airflow, nunca en código).
- ¿ML de qué capa, BI de qué capa? ML de Silver, BI de Gold.
- ¿Re-correr duplica? No: Bronze `replace`/overwrite, Silver/Gold `MERGE` + `fecha_cargue` (verificado 2×).
- ¿Los jobs corren en el clúster? Sí: driver en Airflow, executors en `spark-master:7077` (se ven en Spark UI `:8081`); `SPARK_MASTER_URL=local` como fallback.
- ¿Trino y Dremio coinciden? Sí: réplica Silver→Gold en Trino == Gold en Dremio (`180188,300000`).

## Troubleshooting

- Puertos ocupados (P1 usa 9000/8888/19120): apaga P1 antes de `up`, o usa la vía stack-activo.
- Dremio pide RAM: mínimo 8GB en Docker.
- DLT remoto es lento (~20 min): `run_bronze` usa `data/*.parquet` local si existe.
- Re-correr no duplica: Bronze overwrite, Silver/Gold MERGE + `fecha_cargue`.
- `MERGE_CARDINALITY_VIOLATION` en `top_tags`: era el split sobre-escapado
  (`'\\\\s+'` en SQL-string no parte por espacios y no hacía trim) → fuente con
  tags duplicados. Ya fijo con funciones `split`+`trim` y `groupBy` antes del `limit`.
