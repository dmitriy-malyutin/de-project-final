------------------------------------------------------------------
-- transactions

-- DROP TABLE IF EXISTS CRAZ93YANDEXRU__STAGING.transactions;
CREATE TABLE IF NOT EXISTS CRAZ93YANDEXRU__STAGING.transactions (
    _id UUID PRIMARY KEY,
    operation_id UUID,
    account_number_from INTEGER,
    account_number_to INTEGER,
    currency_code INTEGER,
    country VARCHAR(64),
    status VARCHAR(32),
    transaction_type VARCHAR(32),
    amount INTEGER,
    transaction_dt TIMESTAMP(0)
)
ORDER BY transaction_dt, _id
SEGMENTED BY HASH(transaction_dt, _id) ALL NODES
PARTITION BY "transaction_dt"::date
GROUP BY calendar_hierarchy_day("transaction_dt"::date, 3, 2)
;

-- DROP PROJECTION IF EXISTS CRAZ93YANDEXRU__STAGING.transactions_dates;
CREATE PROJECTION IF NOT EXISTS CRAZ93YANDEXRU__STAGING.transactions_dates AS
SELECT
    _id, operation_id, account_number_from, account_number_to,
    currency_code, country, status, transaction_type, amount, transaction_dt
from CRAZ93YANDEXRU__STAGING.transactions
ORDER BY transaction_dt
SEGMENTED BY hash(transaction_dt) ALL NODES;

------------------------------------------------------------------
-- currencies

-- DROP TABLE IF EXISTS CRAZ93YANDEXRU__STAGING.currencies;
CREATE TABLE IF NOT EXISTS CRAZ93YANDEXRU__STAGING.currencies (
    _id UUID PRIMARY KEY,
    date_update TIMESTAMP(0),
    currency_code INTEGER,
    currency_code_with INTEGER,
    currency_with_div NUMERIC(8,2)
)
ORDER BY date_update, _id
SEGMENTED BY HASH(date_update, _id) ALL NODES
PARTITION BY "date_update"::date
GROUP BY calendar_hierarchy_day("date_update"::date, 3, 2)
;

-- DROP PROJECTION IF EXISTS CRAZ93YANDEXRU__STAGING.currencies_dates;
CREATE PROJECTION IF NOT EXISTS CRAZ93YANDEXRU__STAGING.currencies_dates AS
SELECT
    _id, date_update, currency_code, currency_code_with, currency_with_div
from CRAZ93YANDEXRU__STAGING.currencies
ORDER BY date_update
SEGMENTED BY hash(date_update) ALL NODES;