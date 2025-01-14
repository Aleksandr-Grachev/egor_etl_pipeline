--1 Приравнивание начальной суммы дня к конечной сумме дня предыдущего дня
WITH prev_day_balance AS (
    SELECT 
        a.account_rk,
        a.effective_date,
        a.account_in_sum,
        a.account_out_sum,
        LAG(a.account_out_sum) OVER (PARTITION BY a.account_rk ORDER BY a.effective_date) AS prev_account_out_sum
    FROM rd.account_balance a
)
SELECT 
    account_rk,
    effective_date,
    account_in_sum,
    account_out_sum,
    prev_account_out_sum
FROM prev_day_balance
WHERE account_in_sum != prev_account_out_sum
  AND effective_date > (SELECT MIN(effective_date) FROM rd.account_balance);




--2 Приравнивание конечной суммы дня предыдущего дня к начальной сумме дня
WITH next_day_balance AS (
    SELECT 
        a.account_rk,
        a.effective_date,
        a.account_in_sum,
        a.account_out_sum,
        LEAD(a.account_in_sum) OVER (PARTITION BY a.account_rk ORDER BY a.effective_date) AS next_account_in_sum
    FROM rd.account_balance a
)
SELECT 
    account_rk,
    effective_date,
    account_in_sum,
    account_out_sum,
    next_account_in_sum
FROM next_day_balance
WHERE account_out_sum != next_account_in_sum
  AND effective_date < (SELECT MAX(effective_date) FROM rd.account_balance);




--3.1 Изменение информации входная сумма текущего дня на выходную сумму предыдущего дня
WITH prev_day_balance AS (
    SELECT 
        a.account_rk,
        a.effective_date,
        a.account_in_sum,
        a.account_out_sum,
        LAG(a.account_out_sum) OVER (PARTITION BY a.account_rk ORDER BY a.effective_date) AS prev_account_out_sum
    FROM rd.account_balance a
)
UPDATE rd.account_balance a
SET account_in_sum = p.prev_account_out_sum
FROM prev_day_balance p
WHERE a.account_rk = p.account_rk
  AND a.effective_date = p.effective_date
  AND a.account_in_sum != p.prev_account_out_sum;




--3.2 Изменение информации выходная сумма предыдущего дня на входную сумму текущего дня 
WITH next_day_balance AS (
    SELECT 
        a.account_rk,
        a.effective_date,
        a.account_in_sum,
        a.account_out_sum,
        LEAD(a.account_in_sum) OVER (PARTITION BY a.account_rk ORDER BY a.effective_date) AS next_account_in_sum
    FROM rd.account_balance a
)
UPDATE rd.account_balance a
SET account_out_sum = n.next_account_in_sum
FROM next_day_balance n
WHERE a.account_rk = n.account_rk
  AND a.effective_date = n.effective_date
  AND a.account_out_sum != n.next_account_in_sum;