CREATE OR REPLACE PROCEDURE "DS".fill_account_balance_f(IN i_OnDate DATE)
LANGUAGE plpgsql
AS $BODY$
DECLARE
	v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval;
    v_prev_date DATE := i_OnDate - INTERVAL '1 day';
BEGIN

	v_start_time := clock_timestamp();
	PERFORM pg_sleep(5);
    -- Логи
    INSERT INTO "LOGS".ETL_LOG (PROCESS_NAME, LOG_DATE, START_TIME, END_TIME, STATUS, DURATION)
    VALUES ('fill_account_balance_f', CURRENT_DATE, v_start_time, NULL, 'START', NULL);
	
    DELETE FROM "DM".dm_account_balance_f WHERE on_date = i_OnDate;

    -- Вставляем данные для активных счетов
    INSERT INTO "DM".dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate AS on_date,
        acc.account_rk,
        COALESCE(prev_bal.balance_out, 0) + COALESCE(turnover.debet_amount, 0) - COALESCE(turnover.credit_amount, 0) AS balance_out,
        COALESCE(prev_bal.balance_out_rub, 0) + COALESCE(turnover.debet_amount_rub, 0) - COALESCE(turnover.credit_amount_rub, 0) AS balance_out_rub
    FROM "DS".md_account_d acc
    LEFT JOIN "DM".dm_account_balance_f prev_bal
        ON acc.account_rk = prev_bal.account_rk AND prev_bal.on_date = v_prev_date
    LEFT JOIN "DM".dm_account_turnover_f turnover
        ON acc.account_rk = turnover.account_rk AND turnover.on_date = i_OnDate
    WHERE acc.char_type = 'А'
      AND i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;

    -- Вставляем данные для пассивных счетов
    INSERT INTO "DM".dm_account_balance_f (on_date, account_rk, balance_out, balance_out_rub)
    SELECT
        i_OnDate AS on_date,
        acc.account_rk,
        COALESCE(prev_bal.balance_out, 0) - COALESCE(turnover.debet_amount, 0) + COALESCE(turnover.credit_amount, 0) AS balance_out,
        COALESCE(prev_bal.balance_out_rub, 0) - COALESCE(turnover.debet_amount_rub, 0) + COALESCE(turnover.credit_amount_rub, 0) AS balance_out_rub
    FROM "DS".md_account_d acc
    LEFT JOIN "DM".dm_account_balance_f prev_bal
        ON acc.account_rk = prev_bal.account_rk AND prev_bal.on_date = v_prev_date
    LEFT JOIN "DM".dm_account_turnover_f turnover
        ON acc.account_rk = turnover.account_rk AND turnover.on_date = i_OnDate
    WHERE acc.char_type = 'П' 
      AND i_OnDate BETWEEN acc.data_actual_date AND acc.data_actual_end_date;

	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	
    UPDATE "LOGS".ETL_LOG 
    SET END_TIME = v_end_time, duration = v_duration, STATUS = 'Success'
    WHERE PROCESS_NAME = 'fill_account_balance_f' AND LOG_DATE = CURRENT_DATE AND STATUS = 'START';
END;
$BODY$;

-- Заполнение витрины за 1 день
CALL "DS".fill_account_balance_f('2018-01-31');


-- Заполнение витрины за весь январь 2018
DO $$
DECLARE
    v_date DATE := '2018-01-01';
BEGIN
    WHILE v_date <= '2018-01-31' LOOP
        CALL "DS".fill_account_balance_f(v_date);
        v_date := v_date + INTERVAL '1 day';
    END LOOP;
END;
$$;