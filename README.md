# Итоговый проект

## Описание
Репозиторий предназначен для сдачи итогового проекта.

### Источник данных
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
### Реализация
	1. ДАГ запускается раз в сутки, забирает данные из Postgres и переносит их в Vertica AS IS (параллельный запуск тасок)
	2. Следующим таском обновляется витрина

### Vertica
Базы CRAZ93YANDEXRU__STAGE и CRAZ93YANDEXRU__DWH

### Дашборд в Metabase
![Alt text](src\img\image.png)