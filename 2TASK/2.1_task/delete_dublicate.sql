WITH dublicate_rec AS (
  SELECT MIN(ctid) AS ctid, client_rk, effective_from_date
  FROM dm.client
  GROUP BY client_rk, effective_from_date
)

DELETE FROM dm.client c
WHERE ctid NOT IN (SELECT ctid FROM dublicate_rec)