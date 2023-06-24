def get_transactions():
    import psycopg2 as pg
    import os
    import logging
    import sys
    import vertica_python
    from datetime import datetime, timedelta

    logging.basicConfig(stream=sys.stdout, level=logging.INFO)
    logging.captureWarnings(True)

    date = datetime.fromisoformat(execution_date)
    partition_date = datetime.strftime(date - timedelta(days=1), '%Y-%m-%d')
    execution_date = datetime.strftime(date, '%Y-%m-%d')

    logging.info("======================================")
    logging.info("partition_date: {}".format(partition_date))
    logging.info("execution_date: {}".format(execution_date))

    table_name = 'transactions'

    select_query = f"""
    SELECT 
        operation_id,
        account_number_from,
        account_number_to,
        currency_code,
        country,
        status,
        transaction_type,
        amount,
        transaction_dt
    FROM public.{table_name}
    WHERE transaction_dt >= '{partition_date}' 
        AND transaction_dt < '{partition_date}'
    """
    insert_query = f"""
        INSERT INTO CRAZ93YANDEXRU__STAGING.{table_name} (
            operation_id, 
            account_number_from, 
            account_number_to, 
            currency_code, 
            country, 
            status, 
            transaction_type, 
            amount, 
            transaction_dt
            )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    """

    poastgres_params = {
        "host": os.getenv('POSTGRS_HOST'),
        "port": os.getenv('POSTGRS_PORT'),
        "database": os.getenv('POSTGRS_DB_NAME'),
        "user": os.getenv('POSTGRS_USER'),
        "password": os.getenv('POSTGRS_PASSWORD'),
        "sslmode": "require",
        "sslrootcert": "CA.pem"
    }

    vertica_params = {
        'host': os.getenv('VERTICA_HOST'),
        'port': os.getenv('VERTICA_PORT'),
        'user': os.getenv('VERTICA_USER'),
        'password': os.getenv('VERTICA_PASSWORD'),
        'database': os.getenv('VERTICA_DB')
    }

    def load_from_postgres(poastgres_params, select_query):
        with pg.connect(**poastgres_params) as conn:
            cursor = conn.cursor()
            cursor.execute(select_query)
            result = cursor.fetchall()
            cursor.close()
        return result
    
    def upload_to_vertica(vertica_params, insert_query, data):
        with vertica_python.connect(**vertica_params) as connection:
            cursor = connection.cursor()
            cursor.executemany(insert_query, data)
            connection.commit()
            cursor.close()
    

    transaction = load_from_postgres(poastgres_params, select_query)
    logging.info("Transaction loaded from postgres")

    upload_to_vertica(vertica_params, insert_query, transaction)
    logging.info("Transaction uploaded to vertica")