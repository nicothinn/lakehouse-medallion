"""Silver+Gold via PyIceberg REST (stack activo P1: minio admin/password).
Equivale a silver_transform.py + gold_agg.py: lee Bronze Parquet de MinIO,
cura, MERGE (solo claves nuevas, nunca overwrite) + fecha_cargue.
Uso dentro de jupyter: python /tmp/build_silver_gold.py
"""
import datetime
import re
from collections import Counter

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as fs
import pyarrow.parquet as pq
from pyiceberg.catalog import load_catalog
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, LongType, StringType, TimestamptzType, DateType

MINIO_EP = 'http://minio:9000'
CAT = dict(type='rest', uri='http://nessie:19120/iceberg/main/',
           **{'s3.endpoint': MINIO_EP, 's3.access-key-id': 'admin',
              's3.secret-access-key': 'password', 's3.path-style-access': 'true'})
BUCKET = 'lakehouse'
HOY = datetime.date.today()

s3fs = fs.S3FileSystem(access_key='admin', secret_key='password',
                       endpoint_override=MINIO_EP)


def read_bronze(prefix):
    ds = pq.ParquetDataset(f'{BUCKET}/bronze/{prefix}', filesystem=s3fs)
    return ds.read()  # pyarrow>=14 devuelve Table directo


def b2s(arr, name=''):
    """binary -> utf8 string (las muestras traen BLOB)."""
    if pa.types.is_binary(arr.type) or pa.types.is_large_binary(arr.type):
        return pc.cast(arr, pa.large_string() if 'large' in str(arr.type) else pa.string())
    return arr


def clean_posts(t):
    cols = {}
    for f in t.schema.names:
        cols[f] = b2s(t.column(f))
    t = pa.table(cols)
    t = t.append_column('fecha_cargue', pa.array([HOY] * t.num_rows, type=pa.date32()))
    # dedup por Id
    idx = {}
    ids = t.column('Id').to_pylist()
    for i, v in enumerate(ids):
        if v is not None:
            idx[v] = i
    return t.take(sorted(idx.values()))


def clean_users(t):
    cols = {}
    for f in t.schema.names:
        c = b2s(t.column(f))
        if f == 'DisplayName':
            c = pc.utf8_trim_whitespace(c)
        cols[f] = c
    t = pa.table(cols)
    t = t.append_column('fecha_cargue', pa.array([HOY] * t.num_rows, type=pa.date32()))
    idx = {}
    for i, v in enumerate(t.column('Id').to_pylist()):
        if v is not None:
            idx[v] = i
    return t.take(sorted(idx.values()))


def arrow_to_iceberg_schema(t):
    type_map = {pa.int64(): LongType(), pa.string(): StringType(),
                pa.large_string(): StringType()}
    fields = []
    for i, f in enumerate(t.schema, start=1):
        tp = f.type
        if pa.types.is_timestamp(tp):
            it = TimestamptzType()
        elif pa.types.is_date32(tp):
            it = DateType()
        else:
            it = type_map.get(tp, StringType())
            if it is StringType and tp not in (pa.string(), pa.large_string()):
                raise TypeError(f'col {f.name}: {tp} sin mapeo')
        fields.append(NestedField(i, f.name, it, required=False))
    return Schema(*fields)


def merge_new(catalog, name, batch):
    """MERGE: crea si falta; si existe anexa SOLO claves nuevas (idempotente)."""
    key = batch.schema.names[0]  # Id / user_id / tag primero
    try:
        table = catalog.load_table(name)
        have = set(table.scan(selected_fields=(key,)).to_arrow().column(key).to_pylist())
        mask = [v not in have for v in batch.column(key).to_pylist()]
        new = batch.filter(pa.array(mask))
        if new.num_rows:
            table.append(new)
        print(f'{name}: existentes={len(have)} nuevas={new.num_rows} total={table.scan().to_arrow().num_rows}')
        return table
    except NoSuchTableError:
        ns = name.split('.')[0]
        catalog.create_namespace_if_not_exists(ns)
        loc = f's3://{BUCKET}/warehouse/{name.replace(".", "/")}/'
        table = catalog.create_table(name, schema=arrow_to_iceberg_schema(batch), location=loc)
        table.append(batch)
        print(f'{name}: CREADA filas={batch.num_rows} @ {loc}')
        return table


def main():
    catalog = load_catalog('nessie', **CAT)
    p20 = read_bronze('post/2020/'); p21 = read_bronze('post/2021/')
    u20 = read_bronze('users/2020/'); u21 = read_bronze('users/2021/')
    print(f'bronze: post={p20.num_rows + p21.num_rows} users={u20.num_rows + u21.num_rows}')
    posts = clean_posts(pa.concat_tables([p20, p21]))
    users = clean_users(pa.concat_tables([u20, u21]))
    print(f'clean: post={posts.num_rows} users={users.num_rows}')
    merge_new(catalog, 'silver.post_hist', posts)
    merge_new(catalog, 'silver.users_hist', users)

    # Gold: conteo posts por usuario
    d = posts.to_pydict()
    cnt, nq, na = Counter(), Counter(), Counter()
    for o, pt in zip(d['OwnerUserId'], d['PostTypeId']):
        if o is None:
            continue
        cnt[o] += 1
        if pt == 1:
            nq[o] += 1
        elif pt == 2:
            na[o] += 1
    ud = users.to_pydict()
    names = {i: n for i, n in zip(ud['Id'], ud['DisplayName'])}
    gold = pa.table({
        'user_id': pa.array(list(cnt.keys()), type=pa.int64()),
        'display_name': pa.array([names.get(i) for i in cnt.keys()], type=pa.string()),
        'cant_posts': pa.array([cnt[i] for i in cnt.keys()], type=pa.int64()),
        'cant_preguntas': pa.array([nq[i] for i in cnt.keys()], type=pa.int64()),
        'cant_respuestas': pa.array([na[i] for i in cnt.keys()], type=pa.int64()),
        'fecha_cargue': pa.array([HOY] * len(cnt), type=pa.date32())})
    merge_new(catalog, 'gold.cant_post_x_user_hist', gold)

    # Gold extra: top tags
    tagc = Counter()
    for tg in d['Tags']:
        if tg:
            # fuente trae <tag><tag> o |tag|tag| segun espejo; se parte por ambos
            tagc.update(x for x in re.split(r'[<>\|\s]+', tg) if x and len(x) < 40)
    top = tagc.most_common(50)
    tags = pa.table({'tag': pa.array([t for t, _ in top], type=pa.string()),
                     'n': pa.array([n for _, n in top], type=pa.int64()),
                     'fecha_cargue': pa.array([HOY] * len(top), type=pa.date32())})
    merge_new(catalog, 'gold.top_tags', tags)
    print('SILVER+GOLD OK')


if __name__ == '__main__':
    main()
