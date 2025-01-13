DO $$ 
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval;  
BEGIN

   	v_start_time := clock_timestamp();
	PERFORM pg_sleep(5);
    INSERT INTO "LOGS".etl_log (process_name, start_time, status)
    VALUES ('Insert md_account_d', v_start_time, 'Start');

	UPDATE "DS".md_account_d t
	SET 
    data_actual_end_date = to_date(fbf."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd'),
    account_number = fbf."ACCOUNT_NUMBER",
    char_type = fbf."CHAR_TYPE",
    currency_rk = fbf."CURRENCY_RK",
    currency_code = fbf."CURRENCY_CODE"
	FROM stage.md_account_d fbf
	WHERE t."data_actual_date" = to_date(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd')
  	AND t."account_rk" = fbf."ACCOUNT_RK"
  	AND fbf."ACCOUNT_RK" IS NOT NULL
  	AND fbf."DATA_ACTUAL_DATE" IS NOT NULL
  	AND fbf."DATA_ACTUAL_END_DATE" IS NOT NULL
  	AND fbf."ACCOUNT_NUMBER" IS NOT NULL
  	AND fbf."CHAR_TYPE" IS NOT NULL
  	AND fbf."CURRENCY_RK" IS NOT NULL
  	AND fbf."CURRENCY_CODE" IS NOT NULL;

	INSERT INTO "DS".md_account_d (data_actual_date, data_actual_end_date, account_rk, account_number, char_type, currency_rk, currency_code)
	SELECT 
    	to_date(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd') AS data_actual_date,
    	to_date(fbf."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd') AS data_actual_end_date,
    	fbf."ACCOUNT_RK", 
    	fbf."ACCOUNT_NUMBER",
    	fbf."CHAR_TYPE",
    	fbf."CURRENCY_RK",
    	fbf."CURRENCY_CODE"
	FROM (
    	SELECT DISTINCT
        	"DATA_ACTUAL_DATE", 
        	"DATA_ACTUAL_END_DATE", 
        	"ACCOUNT_RK", 
        	"ACCOUNT_NUMBER", 
        	"CHAR_TYPE", 
        	"CURRENCY_RK", 
        	"CURRENCY_CODE"
    	FROM stage.md_account_d
    	WHERE "ACCOUNT_RK" IS NOT NULL
      	AND "DATA_ACTUAL_DATE" IS NOT NULL
	  	AND "DATA_ACTUAL_END_DATE" IS NOT NULL
	  	AND "ACCOUNT_NUMBER" IS NOT NULL
	  	AND "CHAR_TYPE" IS NOT NULL
	  	AND "CURRENCY_RK" IS NOT NULL
	  	AND "CURRENCY_CODE" IS NOT NULL
		) fbf
	LEFT JOIN "DS".md_account_d t
    ON t."data_actual_date" = to_date(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd')
    AND t."account_rk" = fbf."ACCOUNT_RK"
	WHERE t."account_rk" IS NULL;

	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;

	UPDATE "LOGS".etl_log
    	SET end_time = v_end_time,
			duration = v_duration,
        	status = 'COMPLETED',
       		rows_processed = (SELECT COUNT(*) 
                          FROM stage.md_account_d mad
                          WHERE mad."ACCOUNT_RK" IS NOT NULL
                            AND mad."DATA_ACTUAL_DATE" IS NOT NULL)
    	WHERE process_name = 'Insert md_account_d'
      	AND status = 'Start';

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
        status = 'Failed',
        error_message = SQLERRM,
		duration = v_duration
    WHERE process_name = 'Insert md_account_d'
      AND status = 'Start';
    RAISE;
END $$;