"""Setup Dremio P2 (idempotente): crea admin si falta, source Nessie si falta,
corre checks gold. Uso: python scripts/dremio_setup.py (con p2-dremio arriba).
UI: http://localhost:9047 admin/Admin123!"""
import json
import os
import time
import urllib.request
import urllib.error

BASE = "http://localhost:9047"
USER, PASS = "admin", "Admin123!"
TOK = "scripts/.dremio_token"


def call(method, path, body=None, token=None):
    req = urllib.request.Request(
        BASE + path,
        data=json.dumps(body).encode() if body is not None else None,
        method=method,
        headers={"Content-Type": "application/json",
                 **({"Authorization": "_dremio" + token} if token else {})})
    try:
        with urllib.request.urlopen(req, timeout=60) as r:
            return r.status, json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode()[:300]


def login():
    if os.path.exists(TOK):
        return open(TOK).read().strip()
    now = int(time.time() * 1000)
    print("bootstrap:", call("PUT", "/apiv2/bootstrap/firstuser", {
        "userName": USER, "firstName": "a", "lastName": "a",
        "email": "a@local", "password": PASS, "createdAt": now})[0])
    s, b = call("POST", "/apiv2/login", {"userName": USER, "password": PASS})
    assert s == 200, b
    open(TOK, "w").write(b["token"])
    return b["token"]


def sql(token, q):
    s, job = call("POST", "/api/v3/sql", {"sql": q}, token)
    if s != 200:
        return job
    jid = job["id"]
    for _ in range(60):
        time.sleep(5)
        _, st = call("GET", "/api/v3/job/" + jid, token=token)
        if st.get("jobState") in ("COMPLETED", "FAILED", "CANCELED"):
            break
    if st.get("jobState") != "COMPLETED":
        return st
    return call("GET", "/api/v3/job/" + jid + "/results?limit=25", token=token)[1]


token = login()
s, _ = call("GET", "/api/v3/catalog/by-path/nessie", token=token)
print("source nessie:", "existe" if s == 200 else s)
if s == 404:
    # ponytail: sin storageType (Dremio v24 no lo conoce); endpoint interno minio:9000
    s, b = call("POST", "/api/v3/catalog", {
        "entityType": "source", "type": "NESSIE", "name": "nessie",
        "config": {
            "nessieEndpoint": "http://nessie:19120/api/v2",
            "nessieAuthType": "NONE",
            "credentialType": "ACCESS_KEY",
            "awsAccessKey": "admin", "awsAccessSecret": "admin123",
            "awsRootPath": "/lakehouse", "secure": False,
            "propertyList": [
                {"name": "fs.s3a.path.style.access", "value": "true"},
                {"name": "fs.s3a.endpoint", "value": "minio:9000"},
                {"name": "dremio.s3.compat", "value": "true"}]}}, token)
    print("create nessie:", s, json.dumps(b)[:200])
print(sql(token, "SELECT COUNT(*) n, SUM(cant_posts) total FROM nessie.gold.cant_post_x_user_hist"))
print(sql(token, "SELECT tag, n FROM nessie.gold.top_tags ORDER BY n DESC LIMIT 5"))
