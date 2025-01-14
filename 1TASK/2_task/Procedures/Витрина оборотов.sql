-- DROP PROCEDURE IF EXISTS ds.fill_account_turnover_f(date);
CREATE OR REPLACE PROCEDURE ds.fill_account_turnover_f(
	IN i_ondate date)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval;
	v_rows_processed integer;
BEGIN
    v_start_time := clock_timestamp();
	--PERFORM pg_sleep(5);
    -- Логи
    INSERT INTO logs.ETL_LOG (PROCESS_NAME, LOG_DATE, START_TIME, END_TIME, STATUS,rows_processed, DURATION)
    VALUES ('fill_account_turnover-' || i_ondate::text , CURRENT_DATE, v_start_time, NULL, 'Start', NULL, NULL);

    DELETE FROM dm.DM_ACCOUNT_TURNOVER_F WHERE on_date = i_OnDate;

    -- Рассчёт и вставка в витрину
    INSERT INTO dm.DM_ACCOUNT_TURNOVER_F (on_date, account_rk, credit_amount, credit_amount_rub, debet_amount, debet_amount_rub)
    SELECT 
        i_ondate AS on_date,
        account_rk,
        SUM(credit_amount) AS credit_amount,
        SUM(credit_amount) * COALESCE(MAX(reduced_cource), 1) AS credit_amount_rub,
        SUM(debet_amount) AS debet_amount,
        SUM(debet_amount) * COALESCE(MAX(reduced_cource), 1) AS debet_amount_rub
    FROM (
        SELECT 
            p.credit_account_rk AS account_rk,
            p.credit_amount,
            0 AS debet_amount,
            e.reduced_cource
        FROM ds.FT_POSTING_F p
        LEFT JOIN ds.MD_ACCOUNT_D a ON a.account_rk = p.credit_account_rk
        LEFT JOIN ds.MD_EXCHANGE_RATE_D e ON e.currency_rk = a.currency_rk 
		 AND e.data_actual_date <= i_ondate
		 AND e.data_actual_end_date >= i_ondate
        WHERE p.oper_date = i_ondate
        
        UNION ALL
        
        SELECT 
            p.debet_account_rk AS account_rk,
            0 AS credit_amount,
            p.debet_amount,
            e.reduced_cource
        FROM ds.FT_POSTING_F p
        LEFT JOIN ds.MD_ACCOUNT_D a ON a.account_rk = p.debet_account_rk
        LEFT JOIN ds.MD_EXCHANGE_RATE_D e ON e.currency_rk = a.currency_rk 
		 AND e.data_actual_date <= i_ondate
		 AND e.data_actual_end_date >= i_ondate
        WHERE p.oper_date = i_ondate
    ) subquery
    GROUP BY account_rk;

	GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;

    UPDATE logs.ETL_LOG 
    SET END_TIME = v_end_time, duration = v_duration, STATUS = 'Success', ROWS_PROCESSED = v_rows_processed
    WHERE PROCESS_NAME = 'fill_account_turnover-' || i_ondate::text AND LOG_DATE = CURRENT_DATE AND STATUS = 'Start';

END;
$BODY$;


-- Заполнение витрины за 1 день
CALL ds.fill_account_turnover_f ('2018-01-12')

-- Заполнение витрины за весь январь 2018
DO $$
DECLARE
    v_date DATE := '2018-01-01';
BEGIN
    WHILE v_date <= '2018-01-31' LOOP
        CALL ds.fill_account_turnover_f(v_date);
        v_date := v_date + INTERVAL '1 day';
    END LOOP;
END;
$$;