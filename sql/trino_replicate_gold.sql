-- Trino: replica Gold desde Silver (misma definicion que gold.cant_post_x_user_hist)
-- Por usuario POSTEADOR: agrupa posts + left join users para el nombre.
-- Debe dar == Dremio gold: 180188 filas, SUM(cant_posts)=300000.
SELECT p.OwnerUserId AS user_id, u.DisplayName AS display_name,
       COUNT(*) AS cant_posts,
       COUNT_IF(p.PostTypeId = 1) AS cant_preguntas,
       COUNT_IF(p.PostTypeId = 2) AS cant_respuestas
FROM iceberg.silver.post_hist p
LEFT JOIN iceberg.silver.users_hist u ON u.Id = p.OwnerUserId
GROUP BY 1, 2
ORDER BY cant_posts DESC
LIMIT 20;

-- verificacion conteo total (comparar con Dremio: n=180188, total=300000):
SELECT COUNT(*) AS n, SUM(cant_posts) AS total_posts FROM (
  SELECT p.OwnerUserId, COUNT(*) AS cant_posts
  FROM iceberg.silver.post_hist p GROUP BY 1);

-- top tags desde silver (compara con gold.top_tags):
SELECT tag, COUNT(*) AS n FROM (
  SELECT TRIM(t.tag) AS tag FROM iceberg.silver.post_hist,
  UNNEST(SPLIT(REGEXP_REPLACE(Tags, '[<>|]', ' '), '\s+')) AS t(tag))
WHERE tag <> '' GROUP BY 1 ORDER BY n DESC LIMIT 20;
