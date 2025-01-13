DO $$ 
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval; 
BEGIN

    v_start_time := clock_timestamp();
	PERFORM pg_sleep(5);
	
    INSERT INTO "LOGS".etl_log (process_name, start_time, status)
    VALUES ('Insert ft_posting_f', v_start_time, 'Start');

    INSERT INTO "DS".ft_posting_f(
        oper_date,
        credit_account_rk,
        debet_account_rk,
        credit_amount,
        debet_amount
    )
    SELECT to_date(fpf."OPER_DATE", 'dd.mm.YYYY') AS oper_date,
           fpf."CREDIT_ACCOUNT_RK",
           fpf."DEBET_ACCOUNT_RK",
           fpf."CREDIT_AMOUNT",
           fpf."DEBET_AMOUNT"
    FROM (
        SELECT DISTINCT
            "OPER_DATE", 
            "CREDIT_ACCOUNT_RK", 
            "DEBET_ACCOUNT_RK", 
            "CREDIT_AMOUNT", 
            "DEBET_AMOUNT"
        FROM stage.ft_posting_f
        WHERE "CREDIT_ACCOUNT_RK" IS NOT NULL
        AND "DEBET_ACCOUNT_RK" IS NOT NULL
    ) fpf
    LEFT JOIN "DS".ft_posting_f f
        ON f."oper_date" = to_date(fpf."OPER_DATE", 'dd.mm.YYYY')
        AND f."credit_account_rk" = fpf."CREDIT_ACCOUNT_RK"
        AND f."debet_account_rk" = fpf."DEBET_ACCOUNT_RK"
    WHERE f."credit_account_rk" IS NULL;
    
        v_end_time := clock_timestamp();
    	v_duration := v_end_time - v_start_time; 
    
        UPDATE "LOGS".etl_log
        SET end_time = v_end_time,
    		duration = v_duration,
            status = 'Success',
            rows_processed = (SELECT COUNT(*) 
                              FROM stage.ft_posting_f fpf
                             )
        WHERE process_name = 'Insert ft_posting_f' AND status = 'Start';

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time; 
    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
        status = 'Failed',
        error_message = SQLERRM,
		duration = v_duration
    WHERE process_name = 'Insert ft_posting_f' AND status = 'Start';
    RAISE;
END $$;
