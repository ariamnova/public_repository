-- create final view
DROP TABLE IF EXISTS MARINAARYAMNOVAMAILRU__DWH.global_metrics;
CREATE TABLE MARINAARYAMNOVAMAILRU__DWH.global_metrics
(
	date_update date NOT NULL,
	currency_from varchar(3),
	amount_total float,
	cnt_transactions integer,
	avg_transactions_per_account float,
	cnt_accounts_make_transactions integer

)