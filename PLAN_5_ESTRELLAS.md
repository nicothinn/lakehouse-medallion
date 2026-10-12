# Proyecto 2 Lakehouse — Plan para 5.0 + milla extra

**Fuente:** `Proyecto 2 - Frameworks y herramientas para big data.pdf` + `Rubrica Proyecto 2.xlsx`
**Objetivo:** Lakehouse MinIO + Iceberg + Spark + Nessie + Airflow + DLT + Dremio/Trino, Medallion Bronze→Silver→Gold con StackOverflow (ClickHouse).

## 1. Estrategia mínima que da el 5 (alcance simplificado oficial)

No cargues todo por pipeline. Solo esto va por Airflow en clase, el resto precargado manual:

| Capa | Qué | Cómo |
|------|-----|------|
| **Bronze** (Parquet, overwrite) | `post_2020.parquet`, `post_2021.parquet` | **manual** (subir a `s3://lakehouse/bronze/post/2020/`, `/2021/`) |
| | `users_2020.parquet` | **manual** |
| | `users_2021.parquet` | **pipeline ★** Tarea 1 DAG (DLT) |
| **Silver** (Iceberg, merge + `fecha_cargue`) | `post_hist` (2020+2021) | **script manual** lee ambos Parquet Bronze → consolida |
| | `users_hist` (2020+2021) | **pipeline ★** Tarea 2 DAG (PySpark + merge) |
| **Gold** (Iceberg, merge + `fecha_cargue`) | `cant_post_x_user_hist` (cruza post_hist + users_hist) | **pipeline ★** Tarea 3 DAG (PySpark + agregación) |

DAG en Airflow con un solo Play: `bronze_users_2021 (DLT) → silver_users_hist (Spark) → gold_metrics (Spark)`.

> Todo lo manual se carga **antes** de la demo. En clase solo das Play y corre lo marcado ★.

## 2. Mapeo rúbrica → qué te da cada punto

**1. Entregables 20% (6 ítems × 3.3%)**
- [ ] `docker-compose.yml` — un archivo, todos los servicios.
- [ ] `README.md` — despliegue, comandos, puertos/accesos.
- [ ] `dags/lakehouse_medallion.py` — 3 tareas.
- [ ] `notebooks/bronze_ingest.ipynb`, `silver_transform.ipynb`, `gold_agg.ipynb` — independientes y ejecutables.
- [ ] `sql/trino_replicate_gold.sql` — consultas que replican gold desde silver.
- [ ] `docs/arquitectura.png` + `arquitectura.mmd` — diagrama.

**2. Infraestructura 30% (15% + 15%)**
- [ ] `docker compose up -d` deja todo en verde: MinIO, Nessie, Spark, Airflow, Jupyter, Dremio, Trino.
- [ ] Healthchecks + README con URLs/creds. Si algo no arranca en clase, pierdes 30% de una.

**3. Pipeline 30% (10+10+10)**
- [ ] Bronze: Parquet + overwrite. DLT con `write_disposition="replace"`.
- [ ] Silver: Iceberg + `MERGE INTO` (no overwrite) + `fecha_cargue`.
- [ ] Gold: Iceberg + `MERGE INTO` + agregación + `fecha_cargue`.
- [ ] Fallback: si Airflow falla, corres los 3 notebooks y te salvan el 30%.

**4. Consumo 20% (10+10)**
- [ ] Trino: `SELECT` a silver + query que replica `cant_post_x_user_hist`.
- [ ] Dremio: `SELECT` directo a gold. Mismo número de filas en ambos = prueba de que replican.

## 3. Estructura de carpetas (créala tal cual)

```
proyecto2/
├── docker-compose.yml
├── README.md
├── .env
├── dags/lakehouse_medallion.py
├── notebooks/bronze_ingest.ipynb
├── notebooks/silver_transform.ipynb
├── notebooks/gold_agg.ipynb
├── scripts/silver_post_hist_manual.py  # construye post_hist una vez
├── sql/trino_replicate_gold.sql
├── sql/dremio_gold_checks.sql
├── docs/arquitectura.mmd
└── data/  # solo muestras locales, lo real vive en MinIO
```

## 4. Fase 0 — Preparación (1 tarde)

1. Instalar Docker Desktop + WSL2, 12-16GB RAM asignada.
2. Descargar StackOverflow de ClickHouse **limitado**: solo `posts` y `users` 2020-2021, sample si es gigante:
   ```sql
   -- Ejemplo ClickHouse → export parquet
   SELECT * FROM stackoverflow.posts WHERE toYear(CreationDate) IN (2020,2021) LIMIT 200000;
   ```
   Guardar como `post_2020.parquet`, `post_2021.parquet`, `users_2020.parquet`, `users_2021.parquet`.
3. Fijar versiones (no uses `latest` en todo): `minio/minio`, `projectnessie/nessie:0.102`, `apache/spark:3.5.1`, `apache/airflow:2.9.3`, `dremio/dremio-oss:latest`, `trinodb/trino:446`, `jupyter/pyspark-notebook:spark-3.5.1`.
4. Crear bucket `lakehouse` en MinIO con carpetas `bronze/post/`, `bronze/users/`, `warehouse/`.

## 5. Fase 1 — Docker Compose (lo más riesgoso, hazlo primero)

Servicios mínimos: `minio`, `nessie`, `spark-master`, `spark-worker`, `airflow` (standalone, `LocalExecutor` para ahorrar RAM), `jupyter`, `dremio`, `trino`, `postgres` (backend de Airflow + Nessie si lo pide).

Puertos sugeridos (documenta estos en README):
- MinIO API:9000 / Consola:9001 (admin/admin123)
- Nessie:19120
- Airflow:8080 (admin/admin)
- Jupyter:8888
- Spark UI:8081
- Dremio:9047
- Trino:8082

Tips que evitan el fallo en clase:
- `healthcheck` en minio y nessie; airflow `depends_on` con `condition: service_healthy`.
- Red única `lakehouse`, volúmenes nombrados para `minio-data`, `dremio-data`, `pg-data`.
- Un `make up / make down` o `up.sh` para demo en un comando.
- Prueba `docker compose up -d && docker ps` hasta que todo diga `healthy/running`.

## 6. Fase 2 — Bronze (DLT + Parquet + overwrite)

`bronze_ingest.ipynb` y Tarea 1 del DAG hacen lo mismo:
- Fuente: ClickHouse `stackoverflow.users WHERE year=2021` (o CSV/parquet ya descargado).
- DLT: `pipeline(destination="filesystem", dataset_name="bronze")` con `write_disposition="replace"` → `s3://lakehouse/bronze/users/2021/users_2021.parquet`.
- Idempotente: re-correr sobrescribe, nunca duplica. Log con conteo de filas.
- Lo manual (`post_2020`, `post_2021`, `users_2020`): súbelo con `mc cp` o consola MinIO a su ruta antes de la demo.

## 7. Fase 3 — Silver (PySpark + Iceberg + merge)

Tablas: `nessie.silver.post_hist`, `nessie.silver.users_hist`. Schema con `fecha_cargue DATE`.

- `silver_post_hist_manual.py` (una sola vez): lee `bronze/post/2020/*.parquet` + `2021/*.parquet`, deduplica por `Id`, agrega `fecha_cargue=current_date()`, `MERGE INTO silver.post_hist`.
- Tarea 2 DAG / `silver_transform.ipynb`: lee `bronze/users/2020/` + `2021/`, normaliza (cast tipos, trim DisplayName, filtra Id nulo), deduplica, `MERGE INTO silver.users_hist ON target.Id = source.Id WHEN MATCHED THEN UPDATE ... WHEN NOT MATCHED THEN INSERT`.
- Clave: **nunca `overwrite` en Silver**. Demuestra merge corriendo 2 veces sin duplicar (muestra `count(*)` igual).

## 8. Fase 4 — Gold (PySpark + Iceberg + merge + KPI)

Tabla: `nessie.gold.cant_post_x_user_hist` (`user_id, display_name, cant_posts, cant_respuestas, fecha_cargue`).

- Tarea 3 DAG / `gold_agg.ipynb`: lee `silver.post_hist` + `silver.users_hist`, `groupBy OwnerUserId`, separa preguntas (`PostTypeId=1`) vs respuestas (`PostTypeId=2`), join con users, `MERGE INTO gold.cant_post_x_user_hist`.
- Validación: `SELECT COUNT(*), SUM(cant_posts) FROM gold...` debe cuadrar con Trino-replica.

## 9. Fase 5 — Airflow DAG (un solo Play)

`dags/lakehouse_medallion.py`: 3 `PythonOperator` (o `SparkSubmitOperator` si ya lo tienes) en cadena lineal. Sin sensores, sin ramas, sin schedule complejo (`schedule=None`, trigger manual). Cada tarea importa la misma función que el notebook correspondiente → notebooks y DAG nunca divergen.

```python
bronze = PythonOperator(task_id="bronze_users_2021", python_callable=run_bronze)
silver = PythonOperator(task_id="silver_users_hist", python_callable=run_silver)
gold = PythonOperator(task_id="gold_metrics", python_callable=run_gold)
bronze >> silver >> gold
```

Prueba: `airflow dags test lakehouse_medallion <date>` luego Play en UI y captura screenshot para el README.

## 10. Fase 6 — Consumo (Trino + Dremio)

- **Trino (10%):** configura catálogo Iceberg→Nessie (`etc/catalog/iceberg.properties` con `iceberg.catalog.type=nessie`, `nessie.uri`). Queries en `sql/trino_replicate_gold.sql`:
  ```sql
  -- replica gold desde silver (debe dar mismo conteo que Dremio)
  SELECT u.Id, u.DisplayName, COUNT(p.Id) AS cant_posts
  FROM silver.users_hist u LEFT JOIN silver.post_hist p ON p.OwnerUserId = u.Id
  GROUP BY 1,2;
  ```
- **Dremio (10%):** agrega fuente Nessie, abre `gold.cant_post_x_user_hist`, guarda `sql/dremio_gold_checks.sql` con 3 selects de verificación.
- En demo abre ambas UIs lado a lado con mismos números.

## 11. Milla extra (para excelente, bajo costo)

Haz solo estas 4, son las que más impresionan por menos trabajo:
1. **Calidad de datos visible:** en Silver/Gold imprime tabla DQ (filas leídas, nulos, duplicados eliminados, filas mergeadas) + columna `fecha_cargue`. 15 líneas, parece profesional.
2. **2 tablas Gold extra gratis:** `top_tags` y `user_engagement` reutilizando `post_hist` (mismo código Gold, otro `groupBy`). El profe pidió 1, entregar 3 = excelente.
3. **Time-travel / branch Nessie:** crea branch `demo` y muestra `SELECT ... AT BRANCH` o rollback tras re-correr merge. 5 min y nadie más lo hará.
4. **README + diagrama de 10:** tabla de puertos/accesos, `quickstart` en 5 comandos, sección troubleshooting (puertos ocupados, RAM), screenshots de Airflow verde + Trino vs Dremio con mismos conteos, diagrama Mermaid en `docs/arquitectura.mmd`.

No hagas: Kubernetes, Terraform, dbt, Great Expectations completo, streaming — sobra para este alcance.

## 12. Orden de ejecución sugerido + ensayo

1. Compose → MinIO bucket → subir manuales → script `post_hist` → notebooks en orden → DAG Play → Trino/Dremio checks.
2. Ensayo: borra `users_2021` de Bronze y `users_hist`, corre DAG de cero en limpio y cronometra (debe caber en la clase).
3. Lleva plan B: notebooks + `docker compose logs` + screenshots por si no hay internet en clase.

## 13. Checklist final antes de entregar

- [ ] `docker compose up -d` en máquina limpia funciona
- [ ] DAG corre completo con un Play (screenshot)
- [ ] 3 notebooks corren top-to-bottom sin editar
- [ ] `fecha_cargue` existe en Silver y Gold
- [ ] Merge no duplica (corrido 2 veces, mismo count)
- [ ] Trino replica Gold desde Silver con mismo conteo que Dremio
- [ ] README + compose + DAG + notebooks + sql + diagrama entregados
