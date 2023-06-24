# Итоговый проект

## Описание
Репозиторий предназначен для сдачи итогового проекта.

### Шаг 1. Изучение входных данных
В качестве источника данных выбран PostgreSQL

### transactions
```json
{    
	"operation_id": UUID,
    "account_number_from": INTEGER,
    "account_number_to": INTEGER,
    "currency_code": INTEGER,
    "country": VARCHAR(64),
    "status": VARCHAR(32),
    "transaction_type": VARCHAR(32),
    "amount": INTEGER,
    "transaction_dt": TIMESTAMP(0)
}
```

### currencies

```json
{    
    "date_update": TIMESTAMP(0),
    "currency_code": INTEGER,
    "currency_code_with": INTEGER,
    "currency_with_div": NUMERIC(8,2)
}
```
### Шаг 2. Создание таблиц для хранилища
2. Создание таблиц в Stage Vertica описано в sql/ddl_stage.sql
3. Создание витрины global_metrics описано в sql/ddl_gm.sql
4. Реализовано 2 таска по наполнению stage-слоя в БД Vertica. Обе таблицы наполняются AS IS