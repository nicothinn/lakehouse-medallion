"""Bronze Tarea 1: DLT users_2021 -> MinIO Parquet (overwrite).

Fuente rapida: data/users_2021.parquet local (muestra ya descargada).
Solo si no existe, cae al espejo remoto (lento: evitar en clase).
"""
import os
from pathlib import Path

BUCKET = os.getenv("S3_BUCKET", "lakehouse")
MINIO_EP = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
REMOTE = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet/users.parquet"


def _secrets():
    """Creds MinIO desde .dlt/secrets.toml (el profe los pide ahi, no en codigo)."""
    p = Path(__file__).resolve().parent.parent / ".dlt" / "secrets.toml"
    try:
        import tomllib
        return tomllib.loads(p.read_text()).get("minio", {})
    except Exception:
        return {}


SEC = _secrets()
S3_USER = SEC.get("user", os.getenv("MINIO_USER", "admin"))
S3_PASS = SEC.get("password", os.getenv("MINIO_PASSWORD", "admin123"))
MINIO_EP = SEC.get("endpoint", MINIO_EP)


def _load_rows(year, limit):
    import duckdb
    local = os.getenv("BRONZE_LOCAL", f"data/users_{year}.parquet")
    if os.path.exists(local):
        q = f"SELECT * FROM read_parquet('{local}') LIMIT {limit}"
    else:  # fallback lento, solo fuera de clase
        con0 = duckdb.connect()
        con0.execute("INSTALL httpfs; LOAD httpfs;")
        q = f"SELECT * FROM read_parquet('{REMOTE}') WHERE year(CreationDate) = {year} LIMIT {limit}"
        rows = con0.execute(q).fetchall()
        return rows, [d[0] for d in con0.description]
    con = duckdb.connect()
    rows = con.execute(q).fetchall()
    return rows, [d[0] for d in con.description]


def run_bronze(year: int = 2021, limit: int = 100000):
    import dlt
    rows, cols = _load_rows(year, limit)
    data = [dict(zip(cols, r)) for r in rows]
    stage = os.path.abspath(f"bronze_staging/users_{year}")
    try:  # dlt>=1.x exige bucket_url
        dest = dlt.destinations.filesystem(bucket_url="file://" + stage)
        pipe = dlt.pipeline(pipeline_name="bronze_users", destination=dest,
                            dataset_name=f"users_{year}", dev_mode=False)
    except TypeError:  # dlt 0.7 (imagen P2)
        pipe = dlt.pipeline(pipeline_name="bronze_users", destination="filesystem",
                            dataset_name=f"users_{year}", dev_mode=False)
    info = pipe.run(data, table_name=f"users_{year}",
                    write_disposition="replace",  # overwrite: rerunnable, no duplica
                    loader_file_format="parquet")
    print(info)
    # Poner el parquet DLT en la ruta oficial Bronze (overwrite)
    import glob
    import boto3
    files = sorted(glob.glob(f"bronze_users/users_{year}/*.parquet")
                   + glob.glob(f"{stage}/*.parquet")
                   + glob.glob(f"{stage}/**/*.parquet", recursive=True))
    key = f"bronze/users/{year}/users_{year}.parquet"
    if files:
        s3 = boto3.client("s3", endpoint_url=MINIO_EP,
                          aws_access_key_id=S3_USER,
                          aws_secret_access_key=S3_PASS,
                          region_name="us-east-1")
        biggest = max(files, key=os.path.getsize)
        s3.upload_file(biggest, BUCKET, key)  # PUT = overwrite
        print(f"Bronze s3://{BUCKET}/{key}: {len(data)} filas (overwrite OK)")
    else:
        print(f"Bronze local users_{year}: {len(data)} filas (sin MinIO a la vista)")
    return len(data)


if __name__ == "__main__":
    run_bronze()
