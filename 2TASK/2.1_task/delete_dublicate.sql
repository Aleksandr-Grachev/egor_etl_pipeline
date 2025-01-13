--Удаление только дубликатов, без оригинальных записей
WITH dublicate_rec AS (
    SELECT 
        client_rk, 
        effective_from_date, 
        ROW_NUMBER() OVER (PARTITION BY client_rk, effective_from_date ORDER BY client_rk) AS dr
    FROM dm.client
)
DELETE FROM dm.client
WHERE (client_rk, effective_from_date) IN (
    SELECT client_rk, effective_from_date
    FROM dublicate_rec
    WHERE dr > 1
);