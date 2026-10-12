"""Replica Gold desde Silver con LA MISMA definicion de gold_agg/build:
por usuario posteador (posts agrupados + left join users para el nombre).
Si esto da == gold.cant_post_x_user_hist, Trino y Dremio coinciden."""
import duckdb
from pyiceberg.catalog import load_catalog

CAT = dict(type='rest', uri='http://nessie:19120/iceberg/main/',
           **{'s3.endpoint': 'http://minio:9000', 's3.access-key-id': 'admin',
              's3.secret-access-key': 'password', 's3.path-style-access': 'true'})
c = load_catalog('nessie', **CAT)
con = duckdb.connect()
con.register('p', c.load_table('silver.post_hist').scan().to_arrow())
con.register('u', c.load_table('silver.users_hist').scan().to_arrow())
con.register('g', c.load_table('gold.cant_post_x_user_hist').scan().to_arrow())
rep = con.execute("""SELECT p.OwnerUserId uid, u.DisplayName nm,
 COUNT(*) n, SUM(CASE WHEN p.PostTypeId=1 THEN 1 ELSE 0 END) nq,
 SUM(CASE WHEN p.PostTypeId=2 THEN 1 ELSE 0 END) na
 FROM p LEFT JOIN u ON u.Id=p.OwnerUserId GROUP BY 1, 2""").fetchall()
g = {r[0]: r[2:] for r in con.execute(
    "SELECT user_id, display_name, cant_posts, cant_preguntas, cant_respuestas FROM g").fetchall()}
rm = {r[0]: r[2:] for r in rep}
print('replica_filas:', len(rm), 'gold_filas:', len(g))
print('sum_replica:', sum(v[0] for v in rm.values()),
      'sum_gold:', sum(v[0] for v in g.values()))
print('match:', rm == g)
print('--- top5 replica ---')
for r in con.execute("""SELECT p.OwnerUserId, u.DisplayName, COUNT(*) n FROM p
 LEFT JOIN u ON u.Id=p.OwnerUserId GROUP BY 1,2 ORDER BY n DESC LIMIT 5""").fetchall():
    print(r)
