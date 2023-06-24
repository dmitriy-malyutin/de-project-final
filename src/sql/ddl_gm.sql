-- DROP TABLE IF EXISTS CRAZ93YANDEXRU__DWH.global_metrics;
CREATE TABLE CRAZ93YANDEXRU__DWH.global_metrics (
    hk_global_metrics INTEGER PRIMARY KEY, 
    date_update DATE NOT NULL,
    currency_from INTEGER NOT NULL,
    amount_total NUMERIC(18,2) NOT NULL,
    cnt_transactions INTEGER NOT NULL,
    avg_transactions_per_account NUMERIC(12,4) NOT NULL,
    cnt_accounts_make_transactions INTEGER NOT NULL,
    load_dt DATETIME NOT NULL,
    load_src VARCHAR(32) NOT NULL
)
ORDER BY date_update, currency_from
SEGMENTED BY hk_global_metrics ALL NODES
PARTITION BY "date_update"::date
GROUP BY calendar_hierarchy_day("date_update"::date, 3, 2)
;