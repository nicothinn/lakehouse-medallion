<a id="readme-top"></a>

<br />
<div align="center">
  <a href="https://github.com/nicothinn/lakehouse-medallion">
    <img src="docs/arquitectura.png" alt="Logo" width="680">
  </a>

  <h3 align="center">Lakehouse Medallion — StackOverflow 2020-2021</h3>

  <p align="center">
    Bronze Parquet (overwrite) → Silver Iceberg (merge) → Gold Iceberg (merge), orquestado con Airflow y consultable en Trino + Dremio.
    <br />
    <a href="#usage"><strong>Ver demo »</strong></a>
    ·
    <a href="sql/trino_replicate_gold.sql">Réplica Trino</a>
    ·
    <a href="sql/dremio_gold_checks.sql">Checks Dremio</a>
  </p>

  ![Docker](https://img.shields.io/badge/docker-compose-up-2496ED?logo=docker&logoColor=white)
  ![Spark](https://img.shields.io/badge/spark-3.5.0-E25A1C?logo=apachespark&logoColor=white)
  ![Python](https://img.shields.io/badge/python-3.11-3776AB?logo=python&logoColor=white)
  ![Status](https://img.shields.io/badge/DAG_3_tareas-SUCCESS-2ea043)
</div>

<details>
  <summary>Tabla de contenido</summary>
  <ol>
    <li><a href="#about-the-project">About The Project</a></li>
    <li><a href="#getting-started">Getting Started</a></li>
    <li><a href="#usage">Usage</a></li>
    <li><a href="#roadmap">Roadmap</a></li>
    <li><a href="#sustentación">Sustentación</a></li>
    <li><a href="#troubleshooting">Troubleshooting</a></li>
    <li><a href="#acknowledgments">Acknowledgments</a></li>
  </ol>
</details>

## About The Project

[![Pipeline](docs/arquitectura.png)](docs/arquitectura.mmd)

Lakehouse open source local para el Proyecto 2 (corte 2): ingesta StackOverflow 2020-2021 con arquitectura medallón completa y consultable. Un Play de Airflow corre `users_2021` de punta a punta; el resto va precargado manual.

| Capa | Tabla / ruta | Filas | Prueba |
|---|---|---|---|
| Bronze | `s3://lakehouse/bronze/{post,users}/{2020,2021}/` | 150k+150k posts, 100k+100k users | PUT overwrite, rerun OK |
| Silver | `silver.post_hist` / `silver.users_hist` (+`fecha_cargue`) | 300000 / 200000 | rerun → 0 nuevas (merge, no duplica) |
| Gold | `gold.cant_post_x_user_hist` (+`fecha_cargue`) | 180188, SUM=300000 | réplica desde silver **match exacto** |
| Gold extra | `gold.top_tags` | 50 únicas, 0 dups (python 20397, javascript 14727, …) | rerun 2× OK |

Nota muestreo: `users_hist` solo trae usuarios *creados* 2020-2021; los top posteadores son más viejos → `display_name` NULL en esos casos. Los conteos cuadran igual (replica==gold).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

### Built With

* [![MinIO][MinIO]][MinIO-url] — object storage S3 (`:9000`/`:9001`, admin/admin123)
* [![Nessie][Nessie]][Nessie-url] — catálogo versionado (`:19120`, ref `main`, backend Postgres)
* [![Spark][Spark]][Spark-url] — master + worker 3.5.0, driver en Airflow, UI `:8081`
* [![Airflow][Airflow]][Airflow-url] — DAG `lakehouse_medallion`, 3 tareas (`:8083`, admin/admin)
* [![Dremio][Dremio]][Dremio-url] — lectura directa de Gold (`:9047`)
* [![Trino][Trino]][Trino-url] — réplica Gold desde Silver (`:8082`)
* dlt + PyArrow + PyIceberg + Jupyter + DuckDB (verificación)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Getting Started

### Prerequisites

* Docker Desktop con **mínimo 16 GB de RAM** asignada (con 8 GB Spark se pega).
* Puertos libres: `9000/9001/19120/8081/7077/8888/8083/8082/9047/31010`.

### Installation

```bash
docker compose up -d --build
# MinIO :9001 (admin/admin123): crear bucket `lakehouse`
cp .dlt/secrets.toml.example .dlt/secrets.toml  # secrets dlt (no se commitea)
python scripts/sample_bronze.py   # muestra Bronze a data/
python scripts/upload_bronze.py   # sube a s3://lakehouse/bronze/ (overwrite)
# Airflow :8083 -> Play DAG `lakehouse_medallion` (bronze DLT -> silver MERGE -> gold MERGE)
```

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Usage

**Pipeline (1 Play):** Airflow `:8083` → DAG `lakehouse_medallion` → `bronze_users_2021` (DLT, `replace`) → `silver_users_hist` (Spark MERGE) → `gold_metrics` (Spark MERGE + `top_tags`). Los jobs corren en el clúster (`spark-master:7077`, visibles en Spark UI `:8081`); `SPARK_MASTER_URL=local` como fallback.

**Consumo (mismos números en ambos motores):**

* Trino (`sql/trino_replicate_gold.sql`): réplica Gold desde Silver → `180188,300000`. Ojo: `trino-catalog/iceberg.properties` apunta a `http://nessie:19120/api/v1` (Trino 446 pide API v1; con `/api/v2` falla `API version mismatch`).
* Dremio (`sql/dremio_gold_checks.sql`): Gold directo → `180188,300000` + top_tags. Setup idempotente: `python scripts/dremio_setup.py` (crea admin + source `nessie`; Dremio v24 no acepta `storageType`, S3 al endpoint interno `minio:9000` con `dremio.s3.compat=true`).
* Sin Trino/Dremio a la mano: `scripts/verify_gold.py` corre la misma lógica (match: True).

**Fallback sin Airflow:** `notebooks/bronze_ingest.ipynb` → `silver_transform.ipynb` → `gold_agg.ipynb`, en orden (importan los mismos `scripts/`, nunca divergen).

**Puertos / accesos:**

| Servicio | URL | Creds |
|---|---|---|
| MinIO API/Consola | :9000/:9001 | admin/admin123 |
| Nessie | :19120 | sin auth |
| Airflow | :8083 | admin/admin |
| Jupyter | :8888 | token en logs |
| Spark UI | :8081 | — |
| Dremio | :9047 | admin/Admin123! (o primer login en instalación limpia) |
| Trino | :8082 | — |

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Roadmap

- [x] Bronze Parquet overwrite (DLT `replace`)
- [x] Silver Iceberg MERGE + `fecha_cargue`
- [x] Gold Iceberg MERGE + `fecha_cargue` (2 tablas)
- [x] Trino replica == Dremio gold
- [ ] Time-travel / branch Nessie en vivo (milla extra, 5 min)
- [ ] Capa visual Power BI (corte 3)

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Sustentación

Preguntas fijas del profe, respuesta en una línea:

* ¿Por qué Parquet en Bronze? Columnar + comprimido para el crudo analítico; overwrite reescribe el día.
* ¿Por qué Iceberg en Silver/Gold y no Parquet? Sin table format no hay tabla SQL/ACID/merge; Iceberg = lista de archivos + metadatos en Nessie.
* ¿Dónde están los secrets? `.dlt/secrets.toml` (montado en Jupyter/Airflow, nunca en código).
* ¿ML de qué capa, BI de qué capa? ML de Silver, BI de Gold.
* ¿Re-correr duplica? No: Bronze `replace`/overwrite, Silver/Gold `MERGE` + `fecha_cargue` (verificado 2×).
* ¿Los jobs corren en el clúster? Sí: driver en Airflow, executors en `spark-master:7077` (Spark UI `:8081`); `SPARK_MASTER_URL=local` como fallback.
* ¿Trino y Dremio coinciden? Sí: réplica Silver→Gold en Trino == Gold en Dremio (`180188,300000`).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Troubleshooting

* Puertos ocupados: libera `9000/8888/19120` antes de `up` (los usa otro stack si lo tienes arriba).
* Dremio pide RAM: mínimo 8GB en Docker.
* DLT remoto es lento (~20 min): `run_bronze` usa `data/*.parquet` local si existe.
* Re-correr no duplica: Bronze overwrite, Silver/Gold MERGE + `fecha_cargue`.
* `MERGE_CARDINALITY_VIOLATION` en `top_tags`: era el split sobre-escapado (`'\\s+'` en SQL-string no partía por espacios y no hacía trim) → fuente con tags duplicados. Fijo con funciones `split`+`trim` y `groupBy` antes del `limit`.
* `InvalidClassException: RDD` entre driver y clúster: fijar misma versión Spark en compose y driver (aquí 3.5.0 en ambos).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

## Acknowledgments

* Prof. Óscar Jiménez — arquitectura medallón y criterios del curso.
* Dataset StackOverflow (ClickHouse) 2020-2021.
* Plantilla README: [othneildrew/Best-README-Template](https://github.com/othneildrew/Best-README-Template).

<p align="right">(<a href="#readme-top">back to top</a>)</p>

[MinIO]: https://img.shields.io/badge/minio-S3-000000?logo=minio&logoColor=white
[MinIO-url]: https://min.io/
[Nessie]: https://img.shields.io/badge/nessie-catalog-2ea043
[Nessie-url]: https://projectnessie.org/
[Spark]: https://img.shields.io/badge/spark-3.5.0-E25A1C?logo=apachespark&logoColor=white
[Spark-url]: https://spark.apache.org/
[Airflow]: https://img.shields.io/badge/airflow-2.9-017CEE?logo=apacheairflow&logoColor=white
[Airflow-url]: https://airflow.apache.org/
[Dremio]: https://img.shields.io/badge/dremio-9047-6E56CF
[Dremio-url]: https://www.dremio.com/
[Trino]: https://img.shields.io/badge/trino-446-DD00A1
[Trino-url]: https://trino.io/
