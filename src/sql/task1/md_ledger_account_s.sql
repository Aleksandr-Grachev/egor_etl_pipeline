DO $$ 
DECLARE
    v_start_time timestamp;
    v_end_time timestamp;
	v_duration interval;
BEGIN

    v_start_time := clock_timestamp();
	PERFORM pg_sleep(5);
    INSERT INTO "LOGS".etl_log (process_name, start_time, status)
    VALUES ('Insert md_ledger_account_s', v_start_time, 'STARTED');

UPDATE "DS".md_ledger_account_s t
SET 
    chapter = fbf."CHAPTER",
    chapter_name = fbf."CHAPTER_NAME",
    section_number = fbf."SECTION_NUMBER",
    section_name = fbf."SECTION_NAME",
    subsection_name = fbf."SUBSECTION_NAME",
    ledger1_account = fbf."LEDGER1_ACCOUNT",
    ledger1_account_name = fbf."LEDGER1_ACCOUNT_NAME",
    ledger_account_name = fbf."LEDGER_ACCOUNT_NAME",
    characteristic = fbf."CHARACTERISTIC",
    end_date = to_date(fbf."END_DATE", 'YYYY.mm.dd')
FROM stage.md_ledger_account_s fbf
WHERE t."ledger_account" = fbf."LEDGER_ACCOUNT"
  AND t."start_date" = to_date(fbf."START_DATE", 'YYYY.mm.dd')
   AND fbf."LEDGER_ACCOUNT" IS NOT NULL
   AND fbf."START_DATE" IS NOT NULL;

INSERT INTO "DS".md_ledger_account_s (chapter, chapter_name, section_number, section_name, subsection_name, ledger1_account, ledger1_account_name, ledger_account, ledger_account_name, characteristic, start_date, end_date)
SELECT 
    fbf."CHAPTER",
    fbf."CHAPTER_NAME",
    fbf."SECTION_NUMBER",
    fbf."SECTION_NAME",
    fbf."SUBSECTION_NAME",
    fbf."LEDGER1_ACCOUNT",
    fbf."LEDGER1_ACCOUNT_NAME",
    fbf."LEDGER_ACCOUNT",
    fbf."LEDGER_ACCOUNT_NAME",
    fbf."CHARACTERISTIC",
    to_date(fbf."START_DATE", 'YYYY.mm.dd') as start_date,
    to_date(fbf."END_DATE", 'YYYY.mm.dd') as end_date
FROM (
    SELECT DISTINCT
        "CHAPTER",
        "CHAPTER_NAME",
        "SECTION_NUMBER",
        "SECTION_NAME",
        "SUBSECTION_NAME",
        "LEDGER1_ACCOUNT",
        "LEDGER1_ACCOUNT_NAME",
        "LEDGER_ACCOUNT",
        "LEDGER_ACCOUNT_NAME",
        "CHARACTERISTIC",
        "START_DATE",
        "END_DATE"
    FROM stage.md_ledger_account_s
    WHERE "LEDGER_ACCOUNT" IS NOT NULL
      AND "START_DATE" IS NOT NULL
) fbf
LEFT JOIN "DS".md_ledger_account_s t
    ON t."ledger_account" = fbf."LEDGER_ACCOUNT"
    AND t."start_date" = to_date(fbf."START_DATE", 'YYYY.mm.dd')
WHERE t."ledger_account" IS NULL;

    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;

    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
		duration = v_duration,
        status = 'COMPLETED',
        rows_processed = (SELECT COUNT(*) 
                          FROM stage.md_ledger_account_s fbf
                          WHERE fbf."LEDGER_ACCOUNT" IS NOT NULL
                            AND fbf."START_DATE" IS NOT NULL)
    WHERE process_name = 'Insert md_ledger_account_s'
      AND status = 'STARTED';

EXCEPTION WHEN OTHERS THEN
    v_end_time := clock_timestamp();
	v_duration := v_end_time - v_start_time;
    UPDATE "LOGS".etl_log
    SET end_time = v_end_time,
		duration = v_duration,
        status = 'FAILED',
        error_message = SQLERRM
    WHERE process_name = 'Insert md_ledger_account_s'
      AND status = 'STARTED';
    RAISE;
END $$;