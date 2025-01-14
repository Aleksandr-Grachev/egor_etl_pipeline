SELECT * FROM dm.loan_holiday_info;

SELECT * FROM rd.product;

SELECT * FROM rd.loan_holiday;

SELECT * FROM rd.deal_info;

-- Даты присутствующие в основной витрине (1.1 запрос)
SELECT effective_from_date, COUNT(*)
FROM dm.loan_holiday_info
GROUP BY effective_from_date;

-- Даты присутствующие в product (1.2 запрос)
SELECT effective_from_date, COUNT(*)
FROM rd.product
GROUP BY effective_from_date;

-- Даты присутствующие в deal_info (1.3 запрос)
SELECT effective_from_date, COUNT(*)
FROM rd.deal_info
GROUP BY effective_from_date;

