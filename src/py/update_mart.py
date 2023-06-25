import os
import logging
import sys
import vertica_python
from datetime import datetime, timedelta


def update_mart():

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.captureWarnings(True)

    date = datetime.fromisoformat(execution_date)
    partition_date = datetime.strftime(date - timedelta(days=1), '%Y-%m-%d')
    execution_date = datetime.strftime(date, '%Y-%m-%d')

    logging.info("======================================")
    logging.info("partition_date: {}".format(partition_date))
    logging.info("execution_date: {}".format(execution_date))

    vertica_params = {
        'host': os.getenv('VERTICA_HOST'),
        'port': os.getenv('VERTICA_PORT'),
        'user': os.getenv('VERTICA_USER'),
        'password': os.getenv('VERTICA_PASSWORD'),
        'database': os.getenv('VERTICA_DB')
    }

    query = f"""
        INSERT INTO CRAZ93YANDEXRU__DWH.global_metrics
        WITH cte_currencies_420 AS (
            (
                SELECT DISTINCT
                    (FIRST_VALUE(currency_code) OVER w) AS currency_code,
                    (FIRST_VALUE(currency_code_with) OVER w) AS currency_code_with,
                    (FIRST_VALUE(currency_with_div) OVER w) AS currency_with_div,
                    (FIRST_VALUE(date_update) OVER w)::date AS date_update
                FROM
                    CRAZ93YANDEXRU__STAGING.currencies
                WHERE
                    currency_code_with = 420
                    AND date_update::date <= '{partition_date}'::date
                WINDOW w AS (
                    PARTITION BY currency_code
                    ORDER BY date_update DESC
                )
            )
            UNION ALL
            (
                SELECT
                    420 AS currency_code,
                    420 AS currency_code_with,
                    1.0 AS currency_with_div,
                    '{partition_date}'::date AS date_update
            )
        )
        SELECT
            hash(t.transaction_dt::date, t.currency_code) AS hk_global_metrics,
            t.transaction_dt::date AS date_update,
            t.currency_code AS currency_from,
            SUM((t.amount * c.currency_with_div)) AS amount_total,
            COUNT(*) AS cnt_transactions,
            (COUNT(*) / COUNT(DISTINCT t.account_number_from)) AS avg_transactions_per_account,
            COUNT(DISTINCT t.account_number_from) AS cnt_accounts_make_transactions,
            now() AS load_dt,
            'project_final' AS load_src
        FROM
            CRAZ93YANDEXRU__STAGING.transactions AS t
            INNER JOIN cte_currencies_420 AS "c" ON (
                "c"."currency_code" = "t"."currency_code"
                -- AND c.date_update::date = transaction_dt::date
            )
        WHERE
            t.transaction_dt::date = '{partition_date}'::date
            -- AND status = 'done'
            AND t.account_number_from > 0
            AND t.account_number_to > 0
            AND hash(t.transaction_dt::date, t.currency_code) NOT IN (
                SELECT hk_global_metrics FROM CRAZ93YANDEXRU__DWH.global_metrics
            )
        GROUP BY
            t.transaction_dt::date,
            t.currency_code
    """


    def update(vertica_params, query):
        with vertica_python.connect(**vertica_params) as connection:
            cursor = connection.cursor()
            cursor.execute(query)
            connection.commit()
            cursor.close()
    
    update(vertica_params, query)