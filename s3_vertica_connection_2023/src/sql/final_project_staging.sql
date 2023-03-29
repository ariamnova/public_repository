--staging table creation - for the row data

--create tables - currencies
DROP TABLE IF EXISTS MARINAARYAMNOVAMAILRU__STAGING.currencies;
CREATE TABLE MARINAARYAMNOVAMAILRU__STAGING.currencies
(
    currency_code varchar(3),
    currency_code_with varchar(3),
    date_update timestamp NOT NULL,
    currency_code_div float
)
order by date_update
SEGMENTED BY hash(date_update) ALL NODES;


--create tables - transactions
DROP TABLE IF EXISTS MARINAARYAMNOVAMAILRU__STAGING.transactions;
CREATE TABLE MARINAARYAMNOVAMAILRU__STAGING.transactions
(
    operation_id varchar(100) NOT NULL,
    account_number_from varchar(20),
    account_number_to varchar(20),
    currency_code varchar(3),
    country varchar(40),
    status varchar(12),
    transaction_type varchar(20),
    amount float,
    transaction_dt timestamp
)
order by transaction_dt, operation_id
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES;

-- Create a projection for currencies table
CREATE PROJECTION MARINAARYAMNOVAMAILRU__STAGING.currencies_proj1 as
SELECT
	currency_code,
	currency_code_with,
	date_update,
	currency_code_div
FROM MARINAARYAMNOVAMAILRU__STAGING.currencies
order by date_update
SEGMENTED BY hash(date_update) ALL NODES; 


-- Create a projection for transactions table 
CREATE PROJECTION MARINAARYAMNOVAMAILRU__STAGING.transactions_proj1 as
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
FROM
    MARINAARYAMNOVAMAILRU__STAGING.transactions
ORDER BY
    transaction_dt,
    operation_id
SEGMENTED BY hash(transaction_dt, operation_id) ALL NODES;
