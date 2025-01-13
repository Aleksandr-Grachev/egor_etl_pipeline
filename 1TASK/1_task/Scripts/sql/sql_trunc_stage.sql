DO $$
BEGIN
    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'stage'
          AND table_name = 'ft_balance_f'
    ) THEN
        TRUNCATE TABLE stage.ft_balance_f;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'stage'
          AND table_name = 'ft_posting_f'
    ) THEN
        TRUNCATE TABLE stage.ft_posting_f;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'stage'
          AND table_name = 'md_account_d'
    ) THEN
        TRUNCATE TABLE stage.md_account_d;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'stage'
          AND table_name = 'md_currency_d'
    ) THEN
        TRUNCATE TABLE stage.md_currency_d;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'stage'
          AND table_name = 'md_exchange_rate_d'
    ) THEN
        TRUNCATE TABLE stage.md_exchange_rate_d;
    END IF;

    IF EXISTS (
        SELECT 1
        FROM information_schema.tables
        WHERE table_schema = 'stage'
          AND table_name = 'md_ledger_account_s'
    ) THEN
        TRUNCATE TABLE stage.md_ledger_account_s;
    END IF;
END $$;