SELECT * FROM ds.ft_balance_f;

SELECT * FROM ds.ft_posting_f;

SELECT * FROM ds.md_account_d;

SELECT * FROM ds.md_currency_d;

SELECT * FROM ds.md_exchange_rate_d;

SELECT * FROM ds.md_ledger_account_s;

SELECT * FROM logs.etl_log
ORDER BY log_date DESC, start_time DESC;
