-- Dremio: checks directos sobre Gold (10% rubro Consumo)
SELECT COUNT(*) AS n_users, SUM(cant_posts) AS total_posts FROM nessie.gold.cant_post_x_user_hist;
SELECT * FROM nessie.gold.cant_post_x_user_hist ORDER BY cant_posts DESC LIMIT 20;
SELECT * FROM nessie.gold.top_tags ORDER BY n DESC LIMIT 20;
