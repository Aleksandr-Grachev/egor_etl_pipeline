\c taskdb

-- Task1/1
CREATE TABLE IF NOT EXISTS LOGS.ETL_LOG (
	PROCESS_NAME VARCHAR(60) NOT NULL,
	LOG_DATE DATE NOT NULL DEFAULT CURRENT_DATE, 
	START_TIME TIMESTAMP NOT NULL,
	END_TIME TIMESTAMP,
	STATUS VARCHAR(20) NOT NULL,
	ROWS_PROCESSED INTEGER DEFAULT 0,
	ERROR_MESSAGE TEXT,
	duration INTERVAL
);

CREATE TABLE IF NOT EXISTS ds.ft_balance_f(
	on_date       DATE NOT NULL,
    account_rk    NUMERIC NOT NULL,
    currency_rk   NUMERIC,
    balance_out   FLOAT,
	CONSTRAINT BALANCE_PKEY PRIMARY KEY (ON_DATE, ACCOUNT_RK)
);

CREATE TABLE IF NOT EXISTS ds.ft_posting_f(
	oper_date         DATE NOT NULL,
    credit_account_rk NUMERIC NOT NULL,
    debet_account_rk  NUMERIC NOT NULL,
    credit_amount     FLOAT,
    debet_amount      FLOAT
);


CREATE TABLE IF NOT EXISTS ds.md_account_d (
	DATA_ACTUAL_DATE DATE NOT NULL,
	DATA_ACTUAL_END_DATE DATE NOT NULL,
	ACCOUNT_RK NUMERIC NOT NULL,
	ACCOUNT_NUMBER VARCHAR(20) NOT NULL,
	CHAR_TYPE VARCHAR(1) NOT NULL,
	CURRENCY_RK NUMERIC NOT NULL,
	CURRENCY_CODE VARCHAR(3) NOT NULL,
	CONSTRAINT ACCOUNT_PKEY PRIMARY KEY (DATA_ACTUAL_DATE, ACCOUNT_RK)
);

CREATE TABLE IF NOT EXISTS ds.md_currency_d (
	CURRENCY_RK NUMERIC NOT NULL,
	DATA_ACTUAL_DATE DATE NOT NULL,
	DATA_ACTUAL_END_DATE DATE,
	CURRENCY_CODE VARCHAR(3),
	CODE_ISO_CHAR VARCHAR(3),
	CONSTRAINT CURRENCY_PKEY PRIMARY KEY (CURRENCY_RK, DATA_ACTUAL_DATE)
);

CREATE TABLE IF NOT EXISTS ds.md_exchange_rate_d (
	DATA_ACTUAL_DATE DATE NOT NULL,
	DATA_ACTUAL_END_DATE DATE,
	CURRENCY_RK NUMERIC NOT NULL,
	REDUCED_COURCE FLOAT,
	CODE_ISO_NUM VARCHAR(3),
	CONSTRAINT EXCHANGE_RATE_PKEY PRIMARY KEY (DATA_ACTUAL_DATE, CURRENCY_RK)
);

CREATE TABLE IF NOT EXISTS ds.md_ledger_account_s (
	CHAPTER CHARACTER(1),
	CHAPTER_NAME VARCHAR(16),
	SECTION_NUMBER INTEGER,
	SECTION_NAME VARCHAR(22),
	SUBSECTION_NAME VARCHAR(21),
	LEDGER1_ACCOUNT INTEGER,
	LEDGER1_ACCOUNT_NAME VARCHAR(47),
	LEDGER_ACCOUNT INTEGER NOT NULL,
	LEDGER_ACCOUNT_NAME VARCHAR(153),
	CHARACTERISTIC CHARACTER(1),
	IS_RESIDENT INTEGER,
	IS_RESERVE INTEGER,
	IS_RESERVED INTEGER,
	IS_LOAN INTEGER,
	IS_RESERVED_ASSETS INTEGER,
	IS_OVERDUE INTEGER,
	IS_INTEREST INTEGER,
	PAIR_ACCOUNT VARCHAR(5),
	START_DATE DATE NOT NULL,
	END_DATE DATE,
	IS_RUB_ONLY INTEGER,
	MIN_TERM VARCHAR(1),
	MIN_TERM_MEASURE VARCHAR(1),
	MAX_TERM VARCHAR(1),
	MAX_TERM_MEASURE VARCHAR(1),
	LEDGER_ACC_FULL_NAME_TRANSLIT VARCHAR(1),
	IS_REVALUATION VARCHAR(1),
	IS_CORRECT VARCHAR(1),
	CONSTRAINT LEDGER_ACCOUNT_PKEY PRIMARY KEY (LEDGER_ACCOUNT, START_DATE)
);

-- task1/2
CREATE TABLE IF NOT EXISTS dm.dm_account_turnover_f
(
    on_date date,
    account_rk numeric,
    credit_amount numeric(23,8),
    credit_amount_rub numeric(23,8),
    debet_amount numeric(23,8),
    debet_amount_rub numeric(23,8)
);

CREATE TABLE IF NOT EXISTS dm.dm_account_balance_f
(
    on_date date NOT NULL,
    account_rk numeric NOT NULL,
    balance_out numeric(23,8),
    balance_out_rub numeric(23,8)
);

--task1/3
CREATE TABLE IF NOT EXISTS dm.dm_f101_round_f
(
    from_date date,
    to_date date,
    chapter character(1),
    ledger_account character(5),
    characteristic character(1),
    balance_in_rub numeric(23,8),
    r_balance_in_rub numeric(23,8),
    balance_in_val numeric(23,8),
    r_balance_in_val numeric(23,8),
    balance_in_total numeric(23,8),
    r_balance_in_total numeric(23,8),
    turn_deb_rub numeric(23,8),
    r_turn_deb_rub numeric(23,8),
    turn_deb_val numeric(23,8),
    r_turn_deb_val numeric(23,8),
    turn_deb_total numeric(23,8),
    r_turn_deb_total numeric(23,8),
    turn_cre_rub numeric(23,8),
    r_turn_cre_rub numeric(23,8),
    turn_cre_val numeric(23,8),
    r_turn_cre_val numeric(23,8),
    turn_cre_total numeric(23,8),
    r_turn_cre_total numeric(23,8),
    balance_out_rub numeric(23,8),
    r_balance_out_rub numeric(23,8),
    balance_out_val numeric(23,8),
    r_balance_out_val numeric(23,8),
    balance_out_total numeric(23,8),
    r_balance_out_total numeric(23,8)
);

-- task 2/2

CREATE TABLE dm.account_balance_turnover (
    account_rk bigint NOT NULL,
    currency_name text,
    department_rk bigint NOT NULL,
    effective_date date NOT NULL,
    account_in_sum numeric,
    account_out_sum numeric
);

CREATE TABLE IF NOT EXISTS dm.client (
    client_rk bigint NOT NULL,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL,
    account_rk bigint,
    address_rk bigint,
    department_rk bigint,
    card_type_code text,
    client_id text,
    counterparty_type_cd text,
    black_list_flag boolean,
    client_open_dttm timestamp without time zone,
    bankruptcy_rk bigint
);

CREATE TABLE IF NOT EXISTS dm.dict_currency (
    currency_cd text NOT NULL,
    currency_name text NOT NULL,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);

CREATE TABLE IF NOT EXISTS dm.loan_holiday_info (
    deal_rk bigint NOT NULL,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL,
    agreement_rk bigint,
    account_rk bigint,
    client_rk bigint,
    department_rk bigint,
    product_rk bigint,
    product_name text,
    deal_type_cd text,
    deal_start_date date,
    deal_name text,
    deal_number text,
    deal_sum numeric,
    loan_holiday_type_cd text,
    loan_holiday_start_date date,
    loan_holiday_finish_date date,
    loan_holiday_fact_finish_date date,
    loan_holiday_finish_flg boolean,
    loan_holiday_last_possible_date date
);

CREATE TABLE IF NOT EXISTS rd.account (
    account_rk bigint NOT NULL,
    currency_cd text,
    account_number text,
    account_open_date date,
    account_close_date date,
    department_rk bigint NOT NULL,
    employee_rk bigint NOT NULL,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);

CREATE TABLE IF NOT EXISTS rd.account_balance (
    account_rk bigint NOT NULL,
    effective_date date,
    account_in_sum numeric,
    account_out_sum numeric
);

CREATE TABLE IF NOT EXISTS rd.deal_info (
    deal_rk bigint NOT NULL,
    deal_num text,
    deal_name text,
    deal_sum numeric,
    client_rk bigint NOT NULL,
    account_rk bigint NOT NULL,
    agreement_rk bigint NOT NULL,
    deal_start_date date,
    department_rk bigint,
    product_rk bigint,
    deal_type_cd text,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);

CREATE TABLE IF NOT EXISTS rd.loan_holiday (
    deal_rk bigint NOT NULL,
    loan_holiday_type_cd text,
    loan_holiday_start_date date,
    loan_holiday_finish_date date,
    loan_holiday_fact_finish_date date,
    loan_holiday_finish_flg boolean,
    loan_holiday_last_possible_date date,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);

CREATE TABLE IF NOT EXISTS rd.product (
    product_rk bigint NOT NULL,
    product_name text,
    effective_from_date date NOT NULL,
    effective_to_date date NOT NULL
);


