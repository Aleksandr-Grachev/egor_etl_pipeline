CREATE OR REPLACE PROCEDURE "DM".fill_f101_round_f(
    i_ondate date)
LANGUAGE 'plpgsql'
AS $BODY$
DECLARE
	v_start_time_log timestamp;
    v_end_time_log timestamp;
	v_duration_log interval;
	v_rows_processed integer;

    start_date DATE := i_OnDate - INTERVAL '1 month';
    end_date DATE := i_OnDate - INTERVAL '1 day';
BEGIN

	v_start_time_log := clock_timestamp();
	PERFORM pg_sleep(5);
	
    -- Логи
    INSERT INTO "LOGS".ETL_LOG (PROCESS_NAME, LOG_DATE, START_TIME, END_TIME, STATUS, ROWS_PROCESSED, DURATION)
    VALUES ('fill_f101_round_f', CURRENT_DATE, v_start_time_log, NULL, 'Start', NULL, NULL);
	
    -- Удаляем старые данные
    DELETE FROM "DM".dm_f101_round_f
    WHERE from_date = start_date AND to_date = end_date;

    --Нахождение данных для вставки
    WITH
    account_data AS (
        SELECT 
            a.account_rk,
            a.currency_code::integer,
            a.char_type AS characteristic,
            la.chapter,
            la.ledger_account
        FROM "DS".md_account_d a
        JOIN "DS".md_ledger_account_s la
            ON LEFT(a.account_number, 5) = la.ledger_account::varchar
    ),
    balance_data AS (
        SELECT 
            ad.chapter,
            ad.ledger_account,
            ad.characteristic,
            SUM(CASE 
                    WHEN ad.currency_code IN (810, 643) AND ab.on_date = start_date - INTERVAL '1 day' 
                    THEN ab.balance_out_rub 
                    ELSE 0 
                END) AS balance_in_rub,
            SUM(CASE 
                    WHEN ad.currency_code NOT IN (810, 643) AND ab.on_date = start_date - INTERVAL '1 day' 
                    THEN ab.balance_out_rub 
                    ELSE 0 
                END) AS balance_in_val,
            SUM(CASE 
                    WHEN ab.on_date = start_date - INTERVAL '1 day' 
                    THEN ab.balance_out_rub 
                    ELSE 0 
                END) AS balance_in_total,
            SUM(CASE 
                    WHEN ad.currency_code IN (810, 643) AND ab.on_date = end_date 
                    THEN ab.balance_out_rub 
                    ELSE 0 
                END) AS balance_out_rub,
            SUM(CASE 
                    WHEN ad.currency_code NOT IN (810, 643) AND ab.on_date = end_date 
                    THEN ab.balance_out_rub 
                    ELSE 0 
                END) AS balance_out_val,
            SUM(CASE 
                    WHEN ab.on_date = end_date 
                    THEN ab.balance_out_rub 
                    ELSE 0 
                END) AS balance_out_total
        FROM "DM".dm_account_balance_f ab
        JOIN account_data ad
            ON ab.account_rk = ad.account_rk
        GROUP BY ad.chapter, ad.ledger_account, ad.characteristic
    ),
    turnover_data AS (
        SELECT 
            ad.chapter,
            ad.ledger_account,
            ad.characteristic,
            SUM(CASE 
                    WHEN ad.currency_code IN (810, 643) AND ac.on_date BETWEEN start_date AND end_date 
                    THEN ac.debet_amount_rub 
                    ELSE 0 
                END) AS turn_deb_rub,
            SUM(CASE 
                    WHEN ad.currency_code NOT IN (810, 643) AND ac.on_date BETWEEN start_date AND end_date 
                    THEN ac.debet_amount_rub 
                    ELSE 0 
                END) AS turn_deb_val,
            SUM(CASE 
                    WHEN ac.on_date BETWEEN start_date AND end_date 
                    THEN ac.debet_amount_rub 
                    ELSE 0 
                END) AS turn_deb_total,
            SUM(CASE 
                    WHEN ad.currency_code IN (810, 643) AND ac.on_date BETWEEN start_date AND end_date 
                    THEN ac.credit_amount_rub 
                    ELSE 0 
                END) AS turn_cre_rub,
            SUM(CASE 
                    WHEN ad.currency_code NOT IN (810, 643) AND ac.on_date BETWEEN start_date AND end_date 
                    THEN ac.credit_amount_rub 
                    ELSE 0 
                END) AS turn_cre_val,
            SUM(CASE 
                    WHEN ac.on_date BETWEEN start_date AND end_date 
                    THEN ac.credit_amount_rub 
                    ELSE 0 
                END) AS turn_cre_total
        FROM "DM".dm_account_turnover_f ac
        LEFT JOIN account_data ad
            ON ac.account_rk = ad.account_rk
        GROUP BY ad.chapter, ad.ledger_account, ad.characteristic
    )

    -- Вставка
    INSERT INTO "DM".dm_f101_round_f(
        from_date, to_date, chapter, ledger_account, characteristic, 
        balance_in_rub, balance_in_val, balance_in_total, 
        turn_deb_rub, turn_deb_val, turn_deb_total, 
        turn_cre_rub, turn_cre_val, turn_cre_total, 
        balance_out_rub, balance_out_val, balance_out_total
    )
    SELECT 
        start_date AS from_date,
        end_date AS to_date,
        bd.chapter,
        bd.ledger_account,
        bd.characteristic,
        bd.balance_in_rub,
        bd.balance_in_val,
        bd.balance_in_total,
        td.turn_deb_rub,
        td.turn_deb_val,
        td.turn_deb_total,
        td.turn_cre_rub,
        td.turn_cre_val,
        td.turn_cre_total,
        bd.balance_out_rub,
        bd.balance_out_val,
        bd.balance_out_total
    FROM balance_data bd
    LEFT JOIN turnover_data td
        ON bd.chapter = td.chapter 
        AND bd.ledger_account = td.ledger_account 
        AND bd.characteristic = td.characteristic;


	GET DIAGNOSTICS v_rows_processed = ROW_COUNT;
	v_end_time_log := clock_timestamp();
    v_duration_log := v_end_time_log - v_start_time_log;
	
	UPDATE "LOGS".ETL_LOG 
    SET END_TIME = v_end_time_log, duration = v_duration_log, STATUS = 'Success',  ROWS_PROCESSED = v_rows_processed
    WHERE PROCESS_NAME = 'fill_f101_round_f' AND LOG_DATE = CURRENT_DATE AND STATUS = 'Start';
	
END;
$BODY$;


--Вызов
CALL "DM".fill_f101_round_f('2018-02-01');