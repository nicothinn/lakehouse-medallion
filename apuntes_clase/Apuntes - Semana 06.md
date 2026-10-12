---
curso: Big Data
semana: 6
fecha: 2026-09-09
duracion: miércoles, sesión de ~2 h 29 min (grabación 00:00:01–02:29:06)
tipo: apuntes
tema: "Sustentación del proyecto del corte 1 por los 7 grupos (data stack con Jupyter + MinIO + Nessie + ClickHouse y 4 pipelines en dlt), feedback en vivo del profe y anuncios del corte 2 (motor SQL → Spark → orquestación; proyecto 2 = pipeline en vivo con arquitectura Medallón)"
profesor: Óscar Andrés Jiménez Villanueva
tags:
  - big-data
  - semana-6
  - apuntes
  - sustentación
  - proyecto
  - data-stack
  - dlt
  - iceberg
  - nessie
  - medallion
---

# Apuntes — Semana 6 (9 de Septiembre de 2026)

---

## 1. Resumen cronológico de la clase

> [!note] Cómo leer esta tabla
> Los timestamps son los **marcadores reales de la grabación** (00:00:01–02:29:06, ≈ 2 h 29 min). **Esta semana no hubo diapositivas**: la clase completa fue la **sustentación del proyecto del corte 1** con feedback en vivo. El orden real de presentación fue **G5 → G4 → G2 → G6 → G7 → G1 → G3** (el orden anunciado al inicio quedó enredado con la lista, ver 00:02:25–00:03:45 y 00:06:03–00:06:10). Los números de grupo son los definitivos de la [[Big Data/Apuntes/Apuntes - Semana 05|Semana 05]].

| Franja (en la grabación) | Qué pasó |
| --- | --- |
| 00:00:01–00:04:25 | **Apertura y reglas de la dinámica.** El profe llega tarde por trancón desde Cali; avisa que hay que **cerrar el corte 1**. Reglas: **15–20 min por grupo**, mostrar el **Contenedor y el Compose**, **explicar cada script** y **ejecutar al menos uno** (o haberlo ejecutado **ese mismo día**); **cámara encendida**; aviso a los 5 min. Anuncia que revisará que los datos hayan quedado escritos en **Azure** y recuerda que la **asesoría del lunes** fue solo para los grupos que escribieron. El orden de la lista se enreda: empieza el **G5** y se decide "después del 5 sigue el 4". |
| 00:04:25–00:24:03 | **GRUPO 5.** Compose con los 4 servicios (Jupyter, MinIO, Nessie, ClickHouse) y los 4 scripts, **todo procesado por lotes** por límites de RAM del equipo de un integrante. Namespace `corte 1` y tabla Yellow Trip Data; conteo en ClickHouse **3.475.226**. Profe: MinIO quedó **en memoria** (sin volumen) y falta el **volumen `.dlt`** en Jupyter; pide ver los **secrets** y el **contenedor**. Feedback: corregir **persistencia del catálogo** y **secrets desde `.dlt`**; elogia el trabajo con limitantes de memoria; él ejecutará el proyecto en local. |
| 00:24:16–00:42:54 | **GRUPO 4.** Compose con volúmenes completos (notebooks, requirements, `.dlt`); MinIO con volumen; **Nessie persistente con RocksDB**; ClickHouse. Scripts 1–4 (PyArrow/PyIceberg con namespace, tabla y snapshots; MinIO→Azure con **3 archivos** y verificación por PowerShell; MinIO→ClickHouse con `replace`, `SHOW TABLES` y conteo **3.48 M**). El profe revisa los **secrets** (00:31:51) y ordena un **`docker compose restart`** (00:39:53): verifica en vivo la **persistencia de Nessie** ✔. Explica por qué importan los secrets (Key Vault en la nube; también aplica a **Airflow** en el corte 2) y que ejecutará el proyecto en local. |
| 00:43:02–01:03:40 | **GRUPO 2 (entrega: Sebastián).** Corren el stack desde **otro PC por SSH** (más RAM; aclaran que **no es un VPS**) y muestran Docker por foto. Compose con **init** de MinIO (buckets crudo + Iceberg) y volumen por permisos; Nessie **RocksDB**; Dockerfile reproducible con `requirements.txt`; `config.py` con `os.getenv`; **REST catalog** de Nessie + líneas S3 para MinIO. Notebooks 1–4 con `stream=True`, `replace`, `iter_batches`, 20 columnas y verificación contra el catálogo. El profe pide ver MinIO/Nessie y advierte: **cada ejecución genera una carpeta nueva → corregir (se tendrá en cuenta para el corte 2)**. Restart con problema de ruta (`cd`) resuelto sobre la marcha y pendiente. |
| 01:03:40–01:24:19 | **GRUPO 6 (entrega: David Melo).** Compose con **healthchecks** (orden de servicios), volúmenes de workspace y **evidencias**; MinIO con chequeo; Nessie **RocksDB** persistente; ClickHouse. Scripts 2–4: tabla Iceberg con tipado propio (**18 parquet + metadata JSON**, **3.475.226**), MinIO→Azure con `tomllib`/PyArrow/s3fs/PyIceberg y `secrets.toml` (verificación de tamaño), MinIO→ClickHouse (prueba de 50.000 + carga real, `SHOW TABLES`, primera muestra y conteo). El profe revisa la **carga de secrets** (la leen directo del `secrets.toml` ✔) y deja feedback: no llamarlos por **variables de entorno** en la importación; el resto, bien. |
| 01:24:41–01:45:09 | **GRUPO 7 (entrega: Juan Esteban; "solo es clonarlo y `docker compose up`").** Explicación teórica (generaciones Data Warehouse/Data Lake/Lakehouse; piezas: Parquet, Iceberg, MinIO, Nessie, ClickHouse, dlt; capas bronce/silver/gold). Compose: puertos 9000/9001, red `LakeHouseNet`, Nessie **RocksDB**, **ClickHouse remapeado al 9002** (choca con MinIO), Jupyter instalando `requirements`. Scripts 1–4: fsspec→MinIO con `replace`; PyArrow+s3fs → tabla `demo.taxis`; `S3_to_ADLS` a Azure (**138 MB**); `Demo.Taxis` → ClickHouse por streaming; conteo **3.475.226**. Feedback: "todo les funcionó bien"; revisará el repo; **Nicole deberá explicar parte de los scripts** la próxima vez. Restart pendiente (Alejandro). |
| 01:45:17–02:02:27 | **GRUPO 1 (entrega: Nicolás).** Repo privado: comparten acceso al profe **en vivo**. Persistencia de Nessie con **Postgres** (dependencia *healthy*); dlt/secrets corregido (antes user/password quemados). Notebooks 1–4: HTTPS→MinIO con verificación de filas; parquet→Iceberg (namespace **TRIPS**, append/overwrite, table scan); parquet→Azure (destino `abfss`, verificación con **Azure Storage Explorer** — el profe explica `account_key` vs **SAS token**); MinIO→ClickHouse (base `trips`). Restart final y persistencia. Feedback: les faltó el tema de los **secrets**; "de resto todo bien"; es "incremental y va mejorando para el otro corte". |
| 02:02:27–02:23:08 | **GRUPO 3 (entrega: Samuel García).** **Cinco servicios** (Nessie, Jupyter, Postgres, ClickHouse, MinIO). Compose revisado punto a punto y scripts explicados **línea por línea** (`dlt.resource`, `table_name=yellow_trip_data`, `replace`, `parquet_url`, PyArrow en RAM, `yield` por pedazos, `pipeline.run`, `secrets.toml`, destino filesystem, `load info`); script 2 con `table_format=iceberg`, **lotes de 100.000** y forzado de tipos a float; script 3 a Azure (3.475.226); script 4 por lotes, **un solo worker** y `replace` con verificación en ClickHouse (puerto 8123). El profe valida su enfoque (mapear la carpeta "contenedor" completa: "más práctico") y pide restart con **Postgres** → persistencia ✔. |
| 02:23:08–02:28:02 | **Cierre y anuncios.** Balance: a todos les funcionó; los "temitas" puntuales fueron **secrets** y **catálogo persistente** en algunos grupos. Elogia explicar el *por qué* de las decisiones del pipeline (no "pegar código") y el uso de IA con entendimiento. **Segundo corte más retador**: se despliega la arquitectura **lakehouse**; las próximas 3 clases: **motor SQL → procesamiento (Spark) → orquestación (Airflow)**; el **proyecto 2 será un pipeline en vivo con arquitectura Medallón**; ajustó cantidades de datos de los ejemplos. "Nos vemos la otra semana" (= ==miércoles 16 de septiembre==). |
| 02:28:26–02:29:06 | **Cola (pendientes sueltos).** Con el G7: aclara que lo del "**buen tres**" al G1 era **molestando**. Con el G1: su único pendiente es **dlt/secrets** ("es la mitad, la mitad, la verdad"). |

> [!important] Tareas, entregas y fechas (lo que dejó esta clase)
> - ==**Los 4 scripts debían ejecutarse ese mismo día**== (antes de la presentación): el profe revisa las **fechas de carga** en MinIO y Azure (00:00:37–00:00:58, 00:01:21).
> - ==**El profe ejecutará cada proyecto en local**==: levanta la carpeta con `docker compose up` (hay `README`) y corre **al menos un script**; luego comenta cómo le fue (00:22:30, 00:23:20).
> - ==**Pendientes para el corte 2 (los dos "temitas"):** 1) **persistencia del catálogo Nessie** (RocksDB o Postgres; en varios grupos quedó in-memory) y 2) **secrets de dlt desde `.dlt/secrets.toml`** (sin variables de entorno ni credenciales quemadas)== (00:23:38–00:23:50, 02:27:11).
> - ==**Próximas 3 clases: motor SQL → procesamiento (Spark) → orquestación (Airflow)**==; el corte 2 es "más retador" y despliega la arquitectura **lakehouse** (02:25:11–02:25:57).
> - ==**Proyecto 2: pipeline en vivo con arquitectura Medallón**== — debe correr completo (guardar → transformar → consultar) y **explicar la arquitectura** en la presentación (02:26:43–02:27:02).
> - **Ajuste de cantidades de datos:** el profe redujo los volúmenes de los ejemplos (los datasets grandes tumbaban los PC); mensaje de **eficiencia y optimización** (02:26:15–02:26:34).
> - ==**Próxima clase: miércoles 16 de septiembre de 2026**== ("nos vemos la otra semana", 02:27:25).
> - **Sin tarea escrita nueva**: la entrega de esta semana fue la presentación del proyecto 1.

> [!note] Fuentes de estos apuntes
> - **Transcripción** — [[Big Data/Apuntes/Recursos/Semana 06/Transcripción - 9 de Septiembre]] (fuente principal; 638 segmentos, 00:00:01–02:29:06).
> - **Sin diapositivas:** la clase fue 100 % sustentaciones; no hay deck de la semana 6 (no hay embeds de PDF).
> - **Consigna del proyecto presentado** — [[Big Data/Trabajos/Corte 1/Proyecto 1 - Data Stack/Consigna - Proyecto 1]].
> - **Reglas y socialización del proyecto** — [[Big Data/Apuntes/Apuntes - Semana 05]] (dinámica, 4 pipelines, conteo esperado, entregables).

---

## 2. Sustentaciones del proyecto del corte 1 (por grupo)

Esta clase fue la **sustentación del proyecto del corte 1**: cada grupo mostró el **Contenedor → Compose → los 4 pipelines** y el profe dio feedback en vivo. El stack evaluado: **Jupyter + MinIO + Nessie + ClickHouse** con `http-to-bucket`, `parquet-to-iceberg`, `minio-to-azure` y `minio-to-clickhouse` sobre `yellow_tripdata_2025-01.parquet` (**3.475.226 registros**). Detalle de la consigna: [[Big Data/Trabajos/Corte 1/Proyecto 1 - Data Stack/Consigna - Proyecto 1]].

> [!note] Sobre los nombres
> La diarización automática de la transcripción mezcla y duplica voces, así que las intervenciones se atribuyen como "un integrante del grupo" salvo cuando el nombre es inequívoco (quien entregó el trabajo o cuando el profe lo nombra directamente). La lista oficial de integrantes por grupo está en la foto que el profe subió al campus en la [[Big Data/Apuntes/Apuntes - Semana 05|Semana 05]].

### Grupo 5 (00:04:25–00:24:03)

**Qué mostraron**

- Compose con los 4 servicios (**Jupyter, MinIO, Nessie y ClickHouse**) y los **4 scripts**; el profe reconoció que los crearon "ahí en línea" (en el momento).
- Todo el procesamiento fue **"por lotes"** a causa de una limitación real de hardware: "el equipo que tenía en ese momento **no daba la memoria y siempre explotaba**... decidimos dejarlo así en lotes" (00:10:12, 00:12:39). Lo sostuvieron incluso con "miedo" a duplicar archivos.
- **Script 1:** descarga el parquet de la fuente HTTP y lo sube al bucket (credenciales de MinIO; puerto 9000).
- **Script 2:** conecta MinIO con Nessie, crea el bucket, lee el parquet con **PyArrow en lotes de 250 mil registros** ("permite procesar más o menos 3 millones de registros sin cargar todo el archivo de memoria", 00:13:59), crea con **PyIceberg** el namespace `corte 1` y la tabla **Yellow Trip Data** en el bucket de warehouse; cada lote se agrega y se valida contra los archivos parquet/metadata (00:13:36–00:15:10).
- **Script 3:** ingesta a Azure; credenciales leídas directamente del `secrets` (00:17:04); usan `replace` para que una re-ejecución "solamente suba nuevos datos" y no se dupliquen los archivos (00:17:48–00:18:10).
- **Script 4:** toma el catálogo, lee por lotes, convierte las columnas de fecha a texto y carga a ClickHouse con dlt; consultas con `SHOW TABLES` y `SELECT COUNT` (00:19:56). **Conteo: 3.475.226** (leído dígito a dígito en vivo, 00:20:43; el profe bromeó: "se les perdió un registro. Menos 0.5. No mentiras", 00:20:47).
- Mostraron el **contenedor de Docker** al final (00:21:20).

> [!warning] Feedback del profe — pendientes
> - **Nessie/MinIO sin persistencia:** "ahí no veo [el volumen]: el MinIO está **en memoria**... acuérdense que ahí tiene que estar persistente" (00:08:55); tampoco vio el **volumen de `.dlt`** en Jupyter (00:09:19).
> - **Secrets mal patrón:** estaban leyendo el archivo de secrets y pasándolo a **variables de entorno**; la idea es que los secrets vivan en una ruta del despliegue de Jupyter y el script los lea directo: "no tendrías ni que quemar datos de conexión de MinIO, ni el Azure... ya el script los carga directamente. Como eso es lo que les había comentado varias veces" (00:22:46–00:23:03).
> - **Persistencia del catálogo para el corte 2:** "si ustedes le van a bajar a todo eso y vuelven a subir el Compose, **pierden todas las rutas**... si lo piensan en un entorno productivo, se les pierde toda la ruta del catálogo de sus tablas y cuando las quieran ir a consultar, no van a tener dónde acceder a ellas" (00:23:38–00:23:50).
> - El grupo reconoció que vieron "el tema del in-memory" (opciones **RocksDB o Postgres**), dudaron de cuál pesaba menos en RAM y "se nos olvidó completamente" (00:22:00–00:22:28).

> [!tip] Feedback del profe — lo que quedó bien
> - "En general, bien... Todo está bien" (00:21:30). El único faltante marcado: "el tema del **Nessie persistente**".
> - El **manejo de restricciones de memoria** (procesar por lotes) fue elogiado después como mensaje general para todos (00:25:14).
> - "Queda pendiente que yo ejecute el proyecto... les comento si me funcionó" (00:22:30).

### Grupo 4 (00:24:16–00:42:54)

**Qué mostraron**

- El profe ya había validado sus cargas en Azure: "aquí yo ya validé que el Iceberg lo cargaron en el Azure... Grupo 4, MinIO a Azure también cargaron" (00:24:21).
- Compose con **volúmenes completos en Jupyter**: carpeta de notebooks, `requirements` y `.dlt`; comando que reinstala dependencias al levantar el contenedor; red interna (00:28:08–00:28:38).
- MinIO con credenciales en `environment`, **volumen en `data`** y `entrypoint`; **Nessie persistente con RocksDB** (ruta `var/lib` + warehouse en S3/"MyBucket" de MinIO); ClickHouse con puerto, credenciales y volumen (00:28:57–00:30:52).
- **Script 1** ejecutado mostrando carpetas de dlt loads/pipelines, versión, y parquets/JSON; mostraron los **secrets** cuando el profe los pidió (00:31:06, 00:31:51).
- **Script 2:** conexión al catálogo; namespace con `if not exists`; crea la tabla Iceberg, la registra en Nessie y escribe datos generando **snapshot** y actualización del puntero; busca el parquet dinámicamente en el bucket ("más robusto que escribir el nombre exacto", 00:33:31); celdas de limpieza para las pruebas (00:35:16).
- **Script 3:** MinIO→Azure con dlt; la ejecución tardó (~7 minutos) y subió **3 archivos**; verificación por **PowerShell** y conteo final en ClickHouse **3.48 millones** (00:36:17–00:37:37).
- **Script 4:** MinIO→ClickHouse (tabla `parquetminio`, `replace`), `SHOW TABLES` y conteo; ejecución mostrada en el puerto de ClickHouse (00:38:22–00:39:51).
- `docker compose restart` en vivo por orden del profe (00:39:53): al volver a subir, revisó Nessie → **"Persistente"** (00:41:25). Esta fue la demostración de persistencia que el profe quería ver.

> [!warning] Feedback del profe — pendientes
> - "Queda pendiente de ejecutarlo yo local. Que corra. He visto lo otro: los scripts, el paso a paso, **sobre todo el cargue también de los secrets**" (00:41:38).
> - **Por qué importan los secrets:** en Databricks esto se resuelve con un **Key Vault**, pero "aquí en estos despliegues no se tiene ese servicio"; "todas esas variables del entorno y todos esos secrets **no tienen que estar expuestos de ningún lado**"; las aplicaciones deben leerlos por sí mismas, "específicamente desde Jupyter y desde **Apache Airflow**, que lo vamos a ver en el segundo corte... sin necesidad de dejarlas quemadas, por temas de seguridad" (00:42:04–00:42:35).

> [!tip] Feedback del profe — lo que quedó bien
> - La **persistencia con RocksDB** funcionó tras el restart; el profe lo celebró en el momento ("Persistente").
> - Corrección de término en vivo: "El término es **persistencia**. Un poquito más técnico ahí al menos" (00:29:43).

### Grupo 2 (00:43:02–01:03:40)

**Qué mostraron**

- Corrieron el stack **desde otro PC vía SSH** (más RAM; aclaran que **no es un VPS**: "es otro computador... lo tengo al lado", 00:43:40) y mostraron Docker por **foto** tomada minutos antes (00:43:09). Entrega: **Sebastián** ("Yo entrego un ZIP", 00:43:55).
- Compose: MinIO con **contenedor `init`** que crea los buckets (**crudo + Iceberg**) y un **volumen init por permisos** (antes les fallaba: "como siempre se ejecuta en el root, tocarle permisos a ciertos usuarios", 00:44:51); **Nessie con RocksDB** (persistente en disco); ClickHouse como warehouse; Jupyter **dependiente del orden de servicios** (00:44:10–00:46:30).
- **Entorno reproducible:** imagen base tipo `minimal-notebook` + `requirements.txt` (extras de dlt: filesystem/MinIO, adlfs/Azure y driver de ClickHouse); **cero `pip install` dentro de notebooks**: "quien recibe esta carpeta realmente puede correr `docker compose up` y tiene exactamente las mismas versiones" (00:47:31–00:47:55).
- Los 4 pipelines en **dos formatos** (`.py` y notebooks, mismo código y misma configuración; los notebooks con resultados como evidencia) + `config.py` con `os.getenv` (todo parametrizado: "si queremos cargar otro mes de datos, no hay que tocar el código, solo la variable de source URL", 00:49:13).
- **REST catalog:** "usamos REST catalog, no un catálogo Nessie, porque ese tipo no existe en Iceberg; Nessie habla **protocolo REST de Iceberg**"; las 4 líneas de S3 para MinIO (compatible con S3 pero no AWS: hay que darle el endpoint explícito o el cliente resuelve el bucket como subdominio de Amazon y falla) (00:49:13–00:49:54).
- **Notebook 1:** `requests` + `stream=True` ("descarga por pedazos en vez de traer las **72 megas**", 00:51:11); `write_disposition=replace` → idempotencia ("se puede correr unas 10 veces y el resultado siempre va a ser el mismo", 00:51:48); `iter_batches` con PyArrow; destino filesystem→MinIO; validación final de filas. Resultado: **3.475.226** (00:50:24–00:53:00).
- **Notebook 2:** tabla Iceberg de **20 columnas** registrada en Nessie (data + metadata en el segundo bucket); quitan las columnas internas de dlt (`_dlt_load_id`, `_dlt_id`) para que la tabla sea fiel a la original; namespace `if not exists`; borran y recrean la tabla para evitar duplicación; verifican **preguntándole al catálogo** cuántas filas tiene (no releyendo archivos) (00:53:00–00:54:59).
- **Notebooks 3 y 4:** parten de los parquets respaldados en la tabla Iceberg y mueven a **Azure** y a **ClickHouse** con dlt; usan una función de ruta/prefijo consultada a Nessie ("si mañana cambia el bucket, el nombre de la tabla o la estructura de la carpeta, sigue funcionando... **Nessie es la fuente de verdad sobre dónde vive cada tabla**", 00:56:57–00:57:13); validación del `bucket_url` de los secrets; `replace`; `SHOW TABLES`; notebook extra de verificación de la ingesta a Azure (00:57:29–01:00:56).
- **Incidente en vivo:** al pedirles el `docker compose restart` (01:00:59), la terminal nueva (por SSH) no tenía la ruta del proyecto (`cd`); el profe: "búscala y ahorita que termine el grupo" (01:03:00) y "ahorita nos mostrará el restart, tranquilo" (01:03:40) → **quedó pendiente**.

> [!warning] Feedback del profe — pendientes
> - **Carpeta nueva en cada ejecución:** al mostrar el bucket, el profe notó que se genera una carpeta nueva cada vez que se corre el script: "Eso hay que corregirlo... **para el segundo corte sí lo va a tener en cuenta**" (00:55:21–00:56:02). La ruta de escritura de dlt queda fija; lo que se sobrescribe son los archivos parquet.
> - Le pidió ponerle el **nombre del grupo** a la entrega (01:03:23).
> - Quedó el **restart pendiente** (se les olvidó la ruta del proyecto en la terminal).

> [!tip] Feedback del profe — lo que quedó bien
> - El **Dockerfile/entorno reproducible** y los scripts que no dependen de variables de entorno: el profe lo puso de ejemplo para los demás grupos: "así como creo que fue el **grupo 2**, que no usaron nada de importaciones... el script corre y no tiene que saber de dónde están llamando las variables" (01:23:44).
> - El contenedor **init** de MinIO (buckets) + volumen por permisos y la explicación del **REST catalog**.

### Grupo 6 (01:03:40–01:24:19)

**Qué mostraron**

- Entrega: **David Melo** (el profe pidió ponerle el **nombre del grupo** a la entrega, 01:03:23). Al comenzar hubo una pregunta sobre la entrega por **enlace del repositorio**; el profe: "cualquiera de los dos; si lo hacen de una vez desde el repo, pues mejor" (01:04:05).
- Compose: Jupyter **dependiente del orden de servicios** (buena práctica); dos volúmenes: `workspace` (scripts) y carpeta de **evidencias** (logs de las ejecuciones); MinIO con **healthcheck**; **Nessie persistente con RocksDB** condicionado al health de MinIO; ClickHouse; todos en la misma red (se usa el nombre del servicio en vez de `localhost`); orden de arranque MinIO → Nessie → ClickHouse → Jupyter (01:04:43–01:06:25).
- **Script 1:** descarga desde la URL fuente por lotes, con creación **idempotente** del bucket en MinIO y verificación del total en la web: **3.475.226 registros** (01:07:23–01:08:26).
- **Script 2:** tipado propio traducido de PyArrow a Iceberg (string, double, timestamp); namespace **proyecto**; carga con `append` **en lotes de 200.000** con PyArrow; "cada append genera un nuevo **snapshot** y queda versionado en Nessie; así no perderíamos el historial de los cambios" (01:11:29–01:11:55); verificación contra el catálogo: snapshot vigente, total, namespace y listado de archivos → **18 archivos parquet + metadata JSON** y **3.475.226** (01:12:22).
- **Script 3:** `tomllib` para leer el `secrets.toml`; PyArrow + **s3fs** + PyIceberg + cliente de Azure; verificación de la conexión y de la carpeta del profe; carga completa a Azure y verificación de tamaño y éxito (01:14:33–01:18:29).
- **Script 4:** de la tabla del catálogo a ClickHouse; **prueba con 50.000 registros** (la dejaron como evidencia) y luego la carga real; `SHOW TABLES` y conteo **3.475.226**; muestra de los primeros 100 registros con sus 20 columnas (01:18:41–01:22:10).
- Restart + `docker ps` + recarga de Nessie persistente (01:24:00–01:24:19).

> [!warning] Feedback del profe — pendiente
> - **Secrets en la importación:** aunque leían el `secrets.toml`, en la **importación** los estaban llamando como **variables de entorno**: "la idea era que no tuvieran que usar absolutamente ni llamarlos... el script corre y no tiene que saber de dónde están llamando las variables" (01:23:07–01:23:41). Fue lo único que les quedó faltando: "el tema de la **carga de los secrets**" (01:23:07).
> - El profe revisó el cargue de secrets en vivo; al inicio pidió "sube un momentico, al inicio... es que no veo dónde están cargando los secrets" y confirmó que los leen directo del `.dlt` (01:09:01–01:09:22).

> [!tip] Feedback del profe — lo que quedó bien
> - **Todo lo demás bien:** healthchecks y orden de arranque, persistencia RocksDB, tabla Iceberg coherente (**18 parquet + metadata**), conteo y evidencias, `S3_to_ADLS` (nombre por confirmar [¿?]) y verificación de la carga en Azure.

### Grupo 7 (01:24:41–01:45:09)

**Qué mostraron**

- Entrega: **Juan Esteban**; la entrega es el **repo**: "solo es clonarlo y `docker compose up` y ya funciona" (01:25:13).
- Explicación teórica antes del código (un integrante del grupo; la diarización no permite atribuir con seguridad): las **tres generaciones** de plataformas de datos — **Data Warehouse** (ordenado y rápido, pero rígido, caro y solo estructurados), **Data Lake** (guarda lo que sea, crudo y barato, pero sin tanta gobernanza) y **Data Lakehouse** ("el mejor de los dos": almacenamiento de objetos barato + garantías y analítica de warehouse; **separa almacenamiento de cómputo** y se paga por lo que se usa) (01:27:06–01:28:29).
- Las piezas del stack: **Parquet** (columnar, comprimido, autodescriptivo), **Apache Iceberg** (convierte parquets sueltos en tabla: esquema, **ACID**, **time travel**), **MinIO** (un S3 corriendo local), **Nessie** (el catálogo que sabe qué tablas existen y dónde viven: "sin catálogo el Iceberg está ciego"), **ClickHouse** (columnar, orientado a **OLAP**) y **dlt** como "pegamento" en Python (01:28:29–01:29:36).
- Arquitectura **Medallón** en el flujo: **bronce** (dato crudo, trazabilidad y reproceso), **silver** (dato gobernado: esquema y catálogo) y **gold** (listo para consumo analítico); aclararon que es un **ejercicio** (no un pipeline de producción) cuyo objetivo es desplegar los servicios, hacerlos comunicar y demostrar que **no se pierde ni un registro** (01:29:43–01:31:08).
- Compose: MinIO en **9000/9001**; todos conectados en la red **`LakeHouseNet`**; Nessie **persistente con RocksDB** ("si apagamos el contenedor, se pierden todas las tablas ya hechas"); **ClickHouse remapeado al 9002** porque de forma nativa también escucha en 9000 y "se sobrepone al MinIO" (lo calificaron de intrascendente); Jupyter instalando `requirements.txt` al levantar (01:31:12–01:33:27).
- **Script 1 (bronce):** descarga el parquet con **fsspec** ("por un túnel binario y sin ocupar espacio en el disco") y lo sube a MinIO con dlt (credenciales desacopladas en el secrets); `disposition=replace` para no acumular (01:34:08–01:36:19).
- **Script 2 (silver):** PyArrow + **s3fs**; namespace `demo`; tabla **`demo.taxis`** en Iceberg con `IcebergTable.append` (data + metadata); verificación de filas (01:36:30–01:38:52).
- **Script 3:** `S3_to_ADLS` [¿?] con dlt: lee los parquets de MinIO (source filesystem) y los escribe en la **carpeta del grupo en Azure**; notaron que el mismo código "serviría para S3 real o para Google Cloud Storage, lo único que tocaría cambiar es la configuración"; verificación con **adlfs** (tamaño real esperado: **138 MB**); credenciales de Azure desacopladas (01:38:54–01:40:22).
- **Script 4:** vuelve a Nessie (`Demo.Taxis` [¿?]) y carga a ClickHouse con dlt; decisión técnica por RAM: **streaming/por lotes** (`record batches`); le cambiaron el nombre a la tabla (o "cambio de columna" — por confirmar [¿?]) para que funcionara la consulta, con un `if` para re-ejecutar sin error; conteo directo en ClickHouse: **3.475.226** (01:40:22–01:43:27).
- Mostraron los **buckets** (carga cruda y `demo.taxis` con data + metadata), el **Nessie** y las consultas en **ClickHouse** (01:42:06–01:43:27).

> [!warning] Feedback del profe — pendientes
> - El **restart de Compose quedó pendiente**: se acabó el tiempo de la franja ("ya acabó el tiempo", 01:43:56); quedó mencionado Alejandro para hacerlo (01:43:32–01:44:19).
> - "Tengo que revisar el repo con los comments extra" (01:44:32).
> - ==**Para la próxima presentación, Nicole debe explicar parte de los scripts, no solo el resumen**==: "en el segundo corte, en varias partes, y por favor, para que lo tengan en cuenta" (01:44:32–01:45:03).

> [!tip] Feedback del profe — lo que quedó bien
> - "todo les funcionó bien" (01:44:32).
> - La **explicación conceptual** de lakehouse y del papel de cada pieza (Parquet, Iceberg, MinIO, Nessie, ClickHouse, dlt) y la conexión con la arquitectura Medallón que se usará en el corte 2.

### Grupo 1 (01:45:17–02:02:27)

**Qué mostraron**

- Entrega: **Nicolás**. Repo **privado**: tuvieron que darle acceso al profe **en vivo** ("pequeño detalle: compartirle al repo, profe", 01:46:54); el profe verificó el último commit (~5 min antes, solo README/nombres) (01:45:53–01:47:53).
- **Persistencia con Postgres:** Nessie depende de que Postgres esté **healthy** "para que no vayan a haber errores con el catálogo al reiniciar" (01:48:34–01:48:55).
- Ajuste de **dlt/secrets**: antes tenían el user y el password "quemados"; lo corrigieron para que se cargue desde el `secrets` (01:49:20–01:50:04). En la sustentación lo presentaron como corregido; en el feedback final el profe igual lo marcó como el pendiente del grupo.
- **Notebook 1:** dlt + PyArrow + fsspec; descarga por HTTPS, guarda y lee el parquet (Arrow → pandas, DataFrame en memoria); define destino desde la configuración/secrets; carga en parquet; **verificación de filas** contra la carpeta en MinIO (01:50:26–01:52:49).
- **Notebook 2:** parquet → tabla Iceberg registrada en el catálogo Nessie; crea el bucket si no existe (SDK de MinIO); namespace **TRIPS**; **append si la tabla está vacía / overwrite si ya tiene datos**; **table scan** final para validar el total de filas contra la fuente (01:52:56–01:54:15).
- **Notebook 3:** parquet de MinIO → **Azure (carpeta del grupo)** con dlt (destino filesystem con ruta `abfss`, credenciales de Azure, origen `*.parquet` de MinIO, `replace`); verificación visual en **Azure Storage Explorer** (contenedor `clase 4 dlt`) (01:54:19–01:57:02).
- **Notebook 4:** PyIceberg + `tomllib` para mapear los secrets; decorador que accede a la tabla Iceberg y la pasa a pandas; pipeline dlt con destino **ClickHouse**, base `trips`, nombre `minio-to-clickhouse`; print de resultado (01:58:15–01:59:49).
- Conteo y **restart final** con persistencia (Postgres) (02:00:01–02:00:12).
- Incidente menor: la máquina con la CPU al 100 % hizo demorar notebooks ("cuando está en rojo, demoran como seiscientos, setecientos segundos", 02:00:55); el profe: "más que todo es en **RAM**" (02:01:08).

> [!tip] Explicación del profe: `account_key` vs **SAS token**
> Al ver la verificación en Azure Storage Explorer: "aquí lo que yo les estoy pasando es el **`account_key`**... generalmente lo que les pasarían sería un **SAS token**, que tiene permisos limitados a nivel del bucket o la ruta del directorio específicamente y para cierto rango de tiempo" (01:57:08–01:57:31).

> [!warning] Feedback del profe — pendientes
> - "Ahí lo que les faltó fue el tema de los **secrets**. De resto todo bien" (02:01:23). El grupo intentó defenderse ("no están quemados, se leen desde secrets") y el profe insistió en que la idea es que no tengan que cargar nada (02:01:27–02:01:54).
> - "Esto es **incremental y va mejorando para el otro corte**" (02:01:56).
> - En la cola, el profe confirmó que su único pendiente es **dlt/secrets**: "solo fue lo de dlt. Sí, lo del secret... es la mitad, la mitad, la verdad" (02:29:06). Y aclaró que lo del "buen tres" había sido broma (02:28:39).

> [!note] Aclaración del cierre
> El profesor dijo al terminar el G1: "un buen tres, se ganaron un buen tres, no se preocupen" (02:01:56); en la cola, al preguntarle el grupo, respondió: "sí, era **molestando**" (02:28:39). No era una nota real.

### Grupo 3 (02:02:27–02:23:08)

**Qué mostraron**

- Entrega: **Samuel García** (02:02:45).
- **Cinco servicios en el contenedor**: Nessie, Jupyter, **Postgres**, ClickHouse y MinIO (02:03:24).
- Compose revisado punto a punto: MinIO como almacenamiento compatible con S3; Jupyter por volúmenes "para que no se pierda el flujo de los datos"; **Nessie con Postgres** como método de persistencia; ClickHouse para consultas; dlt/secrets (02:03:46–02:04:39).
- **Explicación "línea por línea"** de los scripts (lo que el profe destacaría luego en el balance general):
  - `dlt.resource` y por qué el decorador es obligatorio ("sin este decorador dlt no lo va a poder reconocer"); `table_name=yellow_trip_data` como **etiqueta interna** (no tiene que ver con el nombre del bucket); `replace`; `parquet_url` (la misma URL del enunciado); lectura con **PyArrow** como una "hoja de cálculo gigante cargada en la RAM"; **`yield`** para entregar por pedazos y pausar la función; `pipeline.run` + `loader_file_format`; el **nombre del pipeline** como pista para que dlt busque las credenciales (aquí usan la sección general del `destination filesystem`); `dataset_name` como carpeta dentro del bucket; el **`load info`** (`finished_at`, `load package`) como resumen de la ejecución (02:05:29–02:11:07).
- **Script 2:** `table_format=iceberg` para que dlt cree la tabla completa con metadatos (no un parquet suelto); `replace` "para que no se cree un bucle infinito de archivos"; carga en **lotes de 100.000 filas**; forzaron a **float** dos columnas (`passenger_count` y `ratecode` [¿?]) por incompatibilidad del esquema de Iceberg; validación de filas leyendo solo la metadata del parquet antes y consultando la tabla después; si no coinciden, el notebook se detiene con error (02:12:21–02:14:48).
- **Script 3:** datos de Iceberg → Azure **GRUPO 3**; conexión a Nessie, uso de secrets sin quemar credenciales, lectura de la tabla (3.475.226), recurso dlt, pipeline con destino Azure en formato parquet y verificación sin fallas (02:16:05–02:17:13). El profe: "listo, eso ya lo validé" (02:17:13).
- **Script 4:** MinIO→ClickHouse; **lotes** para mejorar el consumo de memoria y no sobrecargar la máquina; pipeline con **un solo worker** para limitar procesos simultáneos; `replace` contra duplicados; verificación directa en ClickHouse (mencionan el puerto **8123**) (02:17:59–02:19:54).
- Restart con **Postgres** pedido por el profe y verificación de **persistencia** en Nessie (02:20:28–02:23:08).
- Incidente técnico en vivo con el compartir pantalla (varios intentos para mostrar el restart; resolvieron cambiando quién compartía) (02:20:41–02:22:36).

> [!tip] Feedback del profe — lo que quedó bien
> - Su enfoque de mapear la **carpeta completa "contenedor"** en Jupyter: "no es que esté diferente o malo, sino que es **diferente**... mapearon completamente una carpeta que se llama 'contenedor' y ahí están el script y los secrets... es como también un poco **más práctico**" (02:11:30–02:12:05); por eso pidió correr los scripts ahí para confirmar que los secrets se cargaban.
> - Persistencia verificada con el **restart + Postgres**.
> - En el balance general quedó el elogio a explicar el **por qué** de las decisiones: su presentación "línea por línea" fue de las más alineadas con eso (02:23:43).

---

## 3. Lecciones y feedback recurrente del profe

### 3.1 Persistencia del catálogo (pendiente #1)

> [!def] Qué es la persistencia del catálogo y por qué el profe insiste (00:23:38–00:23:50)
> Nessie, por defecto, guarda el catálogo **en memoria**: si se bajan los contenedores y se vuelven a subir, **se pierden todas las rutas** y las tablas ya no se pueden consultar. Persistencia = guardar ese estado en disco (volúmenes de Docker) con un *store* como **RocksDB** o **Postgres**. "Mejorar en el tema del Nessie para el segundo corte, porque el segundo corte también se va a seguir usando... si le bajan a todo eso y vuelven a subir el Compose, pierden todas las rutas"; "en un entorno productivo se les pierde toda la ruta del catálogo de sus tablas y cuando las quieran ir a consultar, no van a tener dónde acceder".

- Opciones usadas en la clase: **RocksDB** (G2, G4, G6, G7) y **Postgres** (G1, G3). La elección quedó a criterio del grupo: en el G5 dudaron de cuál pesaba menos en RAM y lo aplazaron hasta olvidarlo.
- Demostraciones en vivo: G4 con `docker compose restart` → **"Persistente"** (00:41:25); G3 con Postgres (02:22:36–02:23:08).
- Término correcto: "El término es **persistencia**. Un poquito más técnico ahí al menos" (00:29:43).

### 3.2 Secrets de dlt desde `.dlt/secrets.toml` (pendiente #2)

> [!warning] La regla de los secrets (repetida en casi todos los grupos)
> Las credenciales de dlt se leen **desde `.dlt/secrets.toml`** (montado en una ruta del despliegue de Jupyter). **No** se pasan a variables de entorno, **no** se queman en el código y el script "no tiene que saber de dónde están llamando las variables". Sin esto, las credenciales quedan expuestas.

- Razón (G4, 00:42:04–00:42:35): en Databricks eso se resuelve con un **Key Vault**, pero los despliegues locales no tienen ese servicio; "todas esas variables del entorno y todos esos secrets no tienen que estar expuestos de ningún lado". Aplica también a **Apache Airflow** en el corte 2.
- Buen ejemplo: **G2**, con scripts que no hacen ninguna importación de variables (01:23:44).
- Casos señalados: **G5** (secrets → variables de entorno), **G6** (los llamaban como variables de entorno en la importación), **G1** (les faltó el tema de los secrets: "es la mitad").
- Cómo se ve en la práctica (mostrado por G2/G4/G6/G3): un `secrets.toml` montado en Jupyter con los bloques por pipeline (ejemplo de la consigna: `[s3_to_adls.destination.filesystem]`) y scripts que corren sin cargar variables por su cuenta (el detalle exacto por repositorio queda por confirmar).

### 3.3 Cargas por lotes y máquinas con recursos limitados (lo que el profe elogió)

> [!tip] "Eso me pareció bueno: trabajar con esas limitantes" (00:25:14–00:26:08)
> Mensaje general que el profe dio antes del G4: no todo es nube escalable; "posiblemente les toque trabajar con máquinas con limitantes y es muy bueno que aprendan a identificar el **movimiento por lotes**". Anticipa lo que viene: en **Spark** habrá que definir los recursos de los clústeres ("me tocó aprender a las malas en un proyecto", con una EC2 fija de AWS), a diferencia de Databricks, donde el clúster se autoescala.

- En la práctica: `iter_batches`/streaming por lotes (250 filas en el G2; 250 mil en el G5; 100.000 en el G3; 200.000 en el G6), `yield` por pedazos y `stream=True` en descargas para no traer las 72 MB de golpe.

### 3.4 Cuidado con carpetas duplicadas al re-ejecutar

> [!warning] Aviso del profe (00:55:21–00:56:02)
> "Eso es que se le genere una **carpeta nueva** cada vez que ejecuten el script. **Eso hay que corregirlo**". La ruta de escritura de dlt queda fija; lo que se sobrescribe (con `replace`) son los archivos parquet. "Para el segundo corte sí lo va a tener en cuenta" — se calificó como caso del G2, pero el aviso fue para todos.

### 3.5 Ejecutar los scripts el mismo día

> [!warning] Regla de la sustentación (se mantiene)
> "La idea es que ejecuten al menos uno... o si lo ejecutaron al menos **hoy** para ver las ejecuciones al menos hoy" (00:00:37–00:00:58). El profe revisó las **fechas de carga** en MinIO y Azure (00:01:21). ==Los 4 scripts debían correrse el mismo 9 de septiembre.==

### 3.6 Explicar los scripts, no solo el resumen

> [!tip] Rigor al presentar
> - Caso puntual en el **G7**: ==Nicole deberá explicar parte de los scripts en la próxima presentación==, "no solo el resumen" (01:44:32–01:45:03).
> - Balance general: "lo que me gustó fue que la mayoría hicieron una **descripción teórica** de por qué se tomaron ciertas cosas dentro del pipeline. No es como que pegamos este código y listo" (02:23:43).
> - Sobre la IA: "así sea que lo generen... con IA, pero que se tomen el trabajo de **entender**... controlen lo que están entregando" (02:24:15–02:24:33); "si mi máquina está corta de recursos, de una vez ya sé que tengo que pasar a mover eso por lotes" (02:24:54).

### 3.7 Balance del profe y anuncios del corte 2

- "En general, eso fue bien... vimos los temitas puntuales de los **secrets** y **catálogo persistente** en algunos. En general, a todo el mundo les funcionó, que es lo importante" (02:23:08).
- "**Segundo corte más retador**": desplegar la **arquitectura lakehouse**; las próximas tres clases: **motor SQL → procesamiento → orquestación** (02:25:11–02:25:22).
  - **Motor SQL:** herramientas como **Dremio [y] Trino [¿?]** para consultar las tablas de Iceberg (02:25:36).
  - **Procesamiento:** **Spark** (y el uso de **DuckDB [¿?]**); definir clústeres de Spark con recursos que aguanten los computadores locales (02:25:51–02:25:57).
  - **Orquestación:** **Airflow** (también mencionado en el feedback del G4).
- **Ajuste de cantidades de datos:** "en el primer corte todo el mundo quiere trabajar con grandes cantidades... los datos de ejemplo de Stack Overflow... la mayoría se les moría el computador, entonces me tocó empezar como a ajustar eso" (02:26:15–02:26:34) → mensaje de **eficiencia y optimización**.
- ==**Proyecto 2 = pipeline en vivo con arquitectura Medallón**==: "guardar los datos, transformarlos y finalmente poderlos consultar"; la presentación "se está reduciendo a que ejecuten todo eso y corra y también **expliquen la arquitectura total**" (02:26:43–02:27:02).
- Pendientes para tener listos: **persistencia del catálogo** y **secrets**; "ya la otra semana empezamos a agregar más herramientas" (02:27:11).
- Cierre: "me voy contento con lo que lograron... **nos vemos la otra semana**" (02:27:25) → ==miércoles 16 de septiembre de 2026==. Y: "lo que vimos ahorita es el primer corte, continúa aplicándose. **Las mismas tablas de Iceberg se van a seguir usando**" (02:27:39).

---

## 4. Correcciones de transcripción

La transcripción automática confundió varios términos. Correcciones aplicadas (inferidas del contexto):

| En la transcripción | Correcto |
| --- | --- |
| locker compose | Docker Compose |
| Minion / Minium / Minio / Mino / Minus | MinIO |
| Nessy / Nessi / NISI / NECI / Messi / Nexy | Nessie |
| Jupiter | Jupyter |
| Cliphouse / Clickhouse / ClickAus / Lightbox | ClickHouse |
| DLT / DLTA / DLG | dlt (Data Load Tool) |
| RockDB / roxdb / DNC-ROCK-SDB | RocksDB |
| Postgre | Postgres |
| parque / parqueado / parquetes / parket | parquet / parquets / `.parquet` |
| buque / pocket | bucket |
| IceBirdTable.appen | `IcebergTable.append` |
| payarro / PayArrow / PyArrows / Pai Arrow | PyArrow |
| Piceberg / Pai Iceberg / "País Iceberg" / "paisaje" | PyIceberg |
| Pandora / Pandas | pandas |
| impotente | idempotente |
| StreamTrue | `stream=True` |
| WriteDispositionReplace / "Disposition es Replay" | `write_disposition=replace` |
| ShowTables / SelectCount | `SHOW TABLES` / `SELECT COUNT` |
| iter batches | `iter_batches` |
| pv install | pip install |
| S3FS / S3F | s3fs |
| ADLFS | adlfs |
| fssps / FaceSpeak / FCP / "access spec" | fsspec |
| TomLib | tomllib |
| SecretStomp / "Secret Home" / secrets.tolm | `secrets.toml` |
| "los ciclos de LT" | "los secrets de dlt" [¿?] |
| Ladinfo | load info |
| "minuto azure" | MinIO a Azure |
| "minuto ClickHouse" | MinIO a ClickHouse / `minio-to-clickhouse` |
| pylon / piling / Pilem / packline / panline | pipeline |
| line space / linespace / InSpace | namespace |
| DataRig | Databricks |
| "la integración de C3" | la integración de S3 |
| "el destino del AST" | el destino del ADLS |
| Start Overflow | Stack Overflow |
| Lighthouse | lakehouse |
| Sparkles | Spark |
| "orientado al lab" | orientado a OLAP |
| SaaS token | SAS token |
| aconkey | `account_key` |
| repel | repo |
| "Yo entrego un SIP" | "Yo entrego un ZIP" |
| Ronald Cells | `Run All Cells` |
| "el restaurante con post" | el restart con Postgres |
| baches | batches |
| "se cedió como el buque" | se creó el bucket |
| "El if no exist" | El `if not exists` |
| "la fusión a pen" / "cada pen" | la función append / cada append [¿?] |
| "la cerda 3A" | la celda 3A |
| "celda I1" | celda 11 [¿?] |
| "BRA" / "bar, lib" | var, lib |
| "el Jero Trip Data" | Yellow Trip Data |
| "Trit data en 2501" | Yellow Trip Data 2025-01 [¿?] |
| "S3 barra baja tu ALS" | `S3_to_ADLS` [¿?] (pipeline del G6) |
| 3.465.226 / 3.335.226 | 3.475.226 |
| "3, 4, 7, 5, 2, 6" | "3, 4, 7, 5, 2, 2, 6" (lectura dígito a dígito de 3.475.226) |
| "3.475.200 industrial" | 3.475.226 [¿?] |
| "70/72 megas" | 72 MB (tamaño del parquet) |
| "3.48 millones" / "3,4 millones" | se mantienen (redondeos hablados) |
| Toker / The Booker Compose Restart | `docker compose restart` |
| Docker Compost App | `docker compose up` |
| inmemory | in-memory |
| WDB | DuckDB (por confirmar) |
| Dremio Neutrino | Dremio [y] Trino (por confirmar) |
| #SPEAKER_XX (471 casos) | Profesor / Estudiante (diarización no fiable 1:1) |

---

## 5. Recursos de la semana

### Recursos de la clase

- [[Big Data/Apuntes/Recursos/Semana 06/Transcripción - 9 de Septiembre]] — transcripción completa de la clase (fuente principal: 00:00:01–02:29:06; 638 segmentos con diarización).
- **Sin diapositivas nuevas:** esta semana no hubo deck; la clase fue la sustentación del proyecto (no hay PDF que embeber).

### Contexto

- [[Big Data/Trabajos/Corte 1/Proyecto 1 - Data Stack/Consigna - Proyecto 1]] — consigna del proyecto presentado (stack, 4 pipelines, accesos, entregables).
- Las **reglas con las que se presentó** (dinámica, conteo esperado 3.475.226, volumen `.dlt` y persistencia de Nessie) están en [[Big Data/Apuntes/Apuntes - Semana 05]].

---

## 6. Pendientes y dudas del estudiante

- [ ] Corregir la **persistencia del catálogo Nessie** (RocksDB o Postgres) y probar bajar/subir el compose sin perder el catálogo — requisito del corte 2.
- [ ] Ajustar la **carga de secrets de dlt desde `.dlt/secrets.toml`** (sin variables de entorno ni credenciales quemadas) — el pendiente propio de la sustentación ("es la mitad").
- [ ] Evitar que cada re-ejecución genere una **carpeta nueva** en el bucket (el profe lo tendrá en cuenta para el corte 2).
- [ ] Revisar el **campus**: comentarios del profe tras ejecutar cada proyecto en local y la **rúbrica/nota** del corte 1 (el "buen tres" del G1 fue broma).
- [ ] Preparar insumos para el corte 2: **motor SQL**, luego **Spark** y luego **Airflow**; el **proyecto 2** es un **pipeline en vivo con arquitectura Medallón**.
- [ ] (G7) Nicole debe **explicar parte de los scripts** en la próxima presentación, no solo el resumen.
- [ ] ==Fecha a recordar: próxima clase **miércoles 16 de septiembre de 2026**.==
- [ ] Dudas para el profe:
  - ¿Los comentarios del proyecto (tras ejecutarlo en local) quedan por grupo en el campus o se dan en clase?
  - ¿Cuál es el dataset y los volúmenes de datos definitivos para los ejemplos del corte 2? (anunció "ajustes en las cantidades de datos" y "eso ya lo veremos allá").
  - ¿Habrá fechas/rúbrica oficiales del proyecto 2 y de los próximos parciales? (en esta clase no se subió nada nuevo).
- [ ] Nota: esta semana **no quedó tarea escrita nueva**; la entrega fue la presentación del proyecto 1 (carpeta/repo en el campus).
