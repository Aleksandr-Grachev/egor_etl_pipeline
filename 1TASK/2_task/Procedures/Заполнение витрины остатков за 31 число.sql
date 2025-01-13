--Заполнение витрины за 31-12-2017
INSERT INTO "DM".DM_ACCOUNT_BALANCE_F (on_date, account_rk, balance_out, balance_out_rub)
    SELECT 
    on_date,
    account_rk,
    balance_out,
    balance_out * COALESCE(reduced_cource, 1) AS credit_amount_rub
FROM "DS".ft_balance_f fbf
LEFT JOIN "DS".md_exchange_rate_d merd 
    ON fbf.currency_rk = merd.currency_rk
   AND data_actual_date <= '2017-12-31'
   AND data_actual_end_date >= '2017-12-31'
GROUP BY on_date, account_rk, reduced_cource