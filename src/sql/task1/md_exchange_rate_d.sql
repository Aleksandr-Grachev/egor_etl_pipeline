DO $$ 
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval; 
BEGIN

    v_start_time := clock_timestamp();
	PERFORM pg_sleep(5);
    INSERT INTO "LOGS".etl_log (process_name, start_time, status)
    VALUES ('Insert md_exchange_rate_d', v_start_time, 'STARTED');

UPDATE "DS".md_exchange_rate_d t
SET 
    data_actual_end_date = to_date(fbf."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd'),
    reduced_cource = fbf."REDUCED_COURCE",
    code_iso_num = fbf."CODE_ISO_NUM"
FROM stage.md_exchange_rate_d fbf
WHERE t."currency_rk" = fbf."CURRENCY_RK"
  AND t."data_actual_date" = to_date(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd')
  AND fbf."CURRENCY_RK" IS NOT NULL
  AND fbf."DATA_ACTUAL_DATE" IS NOT NULL;

INSERT INTO "DS".md_exchange_rate_d (data_actual_date, data_actual_end_date, currency_rk, reduced_cource, code_iso_num)
SELECT 
    to_date(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd') as data_actual_date,
    to_date(fbf."DATA_ACTUAL_END_DATE", 'YYYY.mm.dd') as data_actual_end_date,
    fbf."CURRENCY_RK",
    fbf."REDUCED_COURCE",
    fbf."CODE_ISO_NUM"
FROM (
    SELECT DISTINCT
        "DATA_ACTUAL_DATE",
        "DATA_ACTUAL_END_DATE",
        "CURRENCY_RK",
        "REDUCED_COURCE",
        "CODE_ISO_NUM"
    FROM stage.md_exchange_rate_d
    WHERE "CURRENCY_RK" IS NOT NULL
      AND "DATA_ACTUAL_DATE" IS NOT NULL
) fbf
LEFT JOIN "DS".md_exchange_rate_d t
    ON t."currency_rk" = fbf."CURRENCY_RK"
    AND t."data_actual_date" = to_date(fbf."DATA_ACTUAL_DATE", 'YYYY.mm.dd')
WHERE t."currency_rk" IS NULL;

	v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;

    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
		duration = v_duration,
        status = 'COMPLETED',
        rows_processed = (SELECT COUNT(*) 
                          FROM stage.md_exchange_rate_d fbf
                          WHERE fbf."DATA_ACTUAL_DATE" IS NOT NULL
                          	AND fbf."CURRENCY_RK" IS NOT NULL)
    WHERE process_name = 'Insert md_exchange_rate_d'
      AND status = 'STARTED';

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
		duration = v_duration,
        status = 'FAILED',
        error_message = SQLERRM
    WHERE process_name = 'Insert md_exchange_rate_d'
      AND status = 'STARTED';
    RAISE;
END $$;