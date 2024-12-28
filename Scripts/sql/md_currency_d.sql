DO $$ 
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval; 
BEGIN

    v_start_time := clock_timestamp();
	 PERFORM pg_sleep(5);
    INSERT INTO "LOGS".etl_log (process_name, start_time, status)
    VALUES ('Insert md_currency_d', v_start_time, 'STARTED');

UPDATE "DS".md_currency_d t
SET 
    data_actual_end_date = TO_DATE(fbf."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd'),
    currency_code = LPAD(fbf."CURRENCY_CODE"::TEXT, 3, '0'),
    code_iso_char = fbf."CODE_ISO_CHAR"
FROM stage.md_currency_d fbf
WHERE t."currency_rk" = fbf."CURRENCY_RK"
  AND t."data_actual_date" = TO_DATE(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd')
  AND fbf."CURRENCY_RK" IS NOT NULL
  AND fbf."DATA_ACTUAL_DATE" IS NOT NULL;

INSERT INTO "DS".md_currency_d (currency_rk, data_actual_date, data_actual_end_date, currency_code, code_iso_char)
SELECT 
    fbf."CURRENCY_RK",
    TO_DATE(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd') AS data_actual_date,
    TO_DATE(fbf."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd') AS data_actual_end_date,
    LPAD(fbf."CURRENCY_CODE"::TEXT, 3, '0') AS currency_code,
    fbf."CODE_ISO_CHAR"
FROM (
    SELECT DISTINCT
        "CURRENCY_RK", 
        "DATA_ACTUAL_DATE", 
        "DATA_ACTUAL_END_DATE", 
        "CURRENCY_CODE", 
        "CODE_ISO_CHAR"
    FROM stage.md_currency_d
    WHERE "CURRENCY_RK" IS NOT NULL
      AND "DATA_ACTUAL_DATE" IS NOT NULL
) fbf
LEFT JOIN "DS".md_currency_d t
    ON t."currency_rk" = fbf."CURRENCY_RK"
    AND t."data_actual_date" = TO_DATE(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd')
WHERE t."currency_rk" IS NULL;

    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;  

    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
		duration = v_duration,
        status = 'COMPLETED',
        rows_processed = (SELECT COUNT(*) 
                          FROM stage.md_currency_d fbf
                          WHERE fbf."CURRENCY_RK" IS NOT NULL
                            AND fbf."DATA_ACTUAL_DATE" IS NOT NULL)
    WHERE process_name = 'Insert md_currency_d'
      AND status = 'STARTED';

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
        status = 'FAILED',
        error_message = SQLERRM,
		duration = v_duration
    WHERE process_name = 'Insert md_currency_d'
      AND status = 'STARTED';
    RAISE;
END $$;