import duckdb
BASE = "https://datasets-documentation.s3.eu-west-3.amazonaws.com/stackoverflow/parquet"
con = duckdb.connect()
con.execute("INSTALL httpfs; LOAD httpfs;")
POST_COLS = "Id, PostTypeId, CreationDate, Score, ViewCount, OwnerUserId, Title, Tags, AnswerCount, CommentCount"
# ponytail: sin Body (texto gigante), solo columnas para KPIs
con.execute(f"""COPY (SELECT {POST_COLS} FROM read_parquet('{BASE}/posts/2020.parquet') LIMIT 150000)
 TO 'data/post_2020.parquet' (FORMAT PARQUET)""")
print("post_2020", con.execute("SELECT count(*) FROM read_parquet('data/post_2020.parquet')").fetchone())
con.execute(f"""COPY (SELECT {POST_COLS} FROM read_parquet('{BASE}/posts/2021.parquet') LIMIT 150000)
 TO 'data/post_2021.parquet' (FORMAT PARQUET)""")
print("post_2021", con.execute("SELECT count(*) FROM read_parquet('data/post_2021.parquet')").fetchone())
USR = "Id, Reputation, CreationDate, DisplayName, LastAccessDate, Views, UpVotes, DownVotes, Location, AccountId"
for yr in (2020, 2021):
    con.execute(f"""COPY (SELECT {USR} FROM read_parquet('{BASE}/users.parquet')
     WHERE year(CreationDate)={yr} LIMIT 100000) TO 'data/users_{yr}.parquet' (FORMAT PARQUET)""")
    print(f"users_{yr}", con.execute(f"SELECT count(*) FROM read_parquet('data/users_{yr}.parquet')").fetchone())
print("DONE")
