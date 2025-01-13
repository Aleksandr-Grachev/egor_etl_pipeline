DO $$ 
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval;  
BEGIN

    v_start_time := clock_timestamp();
	PERFORM pg_sleep(5);
    INSERT INTO "LOGS".etl_log (process_name, start_time, status)
    VALUES ('Insert ft_balance_f', v_start_time, 'Start');

    UPDATE "DS".ft_balance_f f
    SET currency_rk = fbf."CURRENCY_RK",
        balance_out = fbf."BALANCE_OUT"
    FROM stage.ft_balance_f fbf
    WHERE f."on_date" = to_date(fbf."ON_DATE", 'dd.mm.YYYY')
      AND f."account_rk" = fbf."ACCOUNT_RK"
	  AND fbf."ACCOUNT_RK" IS NOT NULL
      AND fbf."ON_DATE" IS NOT NULL;

    INSERT INTO "DS".ft_balance_f (on_date, account_rk, currency_rk, balance_out)
    SELECT to_date(fbf."ON_DATE", 'dd.mm.YYYY') AS on_date,
           fbf."ACCOUNT_RK",
           fbf."CURRENCY_RK",
           fbf."BALANCE_OUT"
    FROM (
        SELECT DISTINCT
            "ON_DATE", 
            "ACCOUNT_RK", 
            "CURRENCY_RK", 
            "BALANCE_OUT"
        FROM stage.ft_balance_f
        WHERE "ACCOUNT_RK" IS NOT NULL
          AND "ON_DATE" IS NOT NULL
    ) fbf
    LEFT JOIN "DS".ft_balance_f f
        ON f."on_date" = to_date(fbf."ON_DATE", 'dd.mm.YYYY')
        AND f."account_rk" = fbf."ACCOUNT_RK"
    WHERE f."account_rk" IS NULL;

    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
	
    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
		duration = v_duration,
        status = 'COMPLETED',
        rows_processed = (SELECT COUNT(*) 
                          FROM stage.ft_balance_f fbf
                          WHERE fbf."ACCOUNT_RK" IS NOT NULL
                            AND fbf."ON_DATE" IS NOT NULL)
    WHERE process_name = 'Insert ft_balance_f' AND status = 'Start';

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
		duration = v_duration,
        status = 'Failed',
        error_message = SQLERRM
    WHERE process_name = 'Insert ft_balance_f'
      AND status = 'Start';
    RAISE;
END $$;
