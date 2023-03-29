MERGE INTO MARINAARYAMNOVAMAILRU__DWH.global_metrics AS tgt 
USING (
	WITH trans AS (
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
		FROM MARINAARYAMNOVAMAILRU__STAGING.transactions 
		WHERE account_number_from > 0
			AND account_number_to > 0
			AND status = 'done' 
	),
	-- здесь только USA, потому что мне нужно привести курсы валют и конвертировать их в доллары =>
	-- чтобы понять, какой код имеет доллар, я выбираю USA
	-- дальше я отбираю из файла валют только те курсы, где есть USA, чтобы менять из/в доллары
	usa_code AS (
		SELECT DISTINCT
			currency_code,
			country
		FROM trans
		WHERE country = 'usa'
	),
	
	exchange_rates AS (
		SELECT 
			CAST(date_update AS date) AS date_update,
			currency_code,
			currency_code_with,
			currency_code_div
		FROM MARINAARYAMNOVAMAILRU__STAGING.currencies
		WHERE currency_code IN (SELECT currency_code FROM usa_code)
			OR currency_code_with IN (SELECT currency_code FROM usa_code)
	),
	
	calculations AS (
		SELECT
			date_update,
			currency_code AS currency_from,
			SUM(amount_currency) AS amount_total,
			SUM(ABS(amount_currency)) AS amount_with_negative,
			COUNT(operation_id) AS cnt_transactions,
			COUNT(DISTINCT account_number_from) AS cnt_accounts_make_transactions
		FROM (
			SELECT
				DATE(t.transaction_dt) AS date_update,
				t.operation_id,
				t.currency_code,
				t.transaction_type,
				t.amount,
				CASE 
					WHEN t.currency_code IN (SELECT currency_code FROM usa_code) 
						THEN amount
					ELSE t.amount * er.currency_code_div
				END AS amount_currency,
				t.account_number_from,
				t.account_number_to
			FROM trans t
			LEFT JOIN exchange_rates er 
				ON t.currency_code = er.currency_code
					AND DATE(t.transaction_dt) = er.date_update
		) mm
		GROUP BY 1, 2
	)
	
	SELECT
		date_update,
		currency_from,
		amount_total,
		cnt_transactions,
		ROUND(cnt_transactions/cnt_accounts_make_transactions, 2) AS avg_transactions_per_account,
		cnt_accounts_make_transactions
	FROM calculations
) AS src
ON tgt.date_update = src.date_update
	AND tgt.currency_from = src.currency_from	

WHEN MATCHED THEN UPDATE SET
	amount_total = src.amount_total,
	cnt_transactions = src.cnt_transactions,
	avg_transactions_per_account = src.avg_transactions_per_account,
	cnt_accounts_make_transactions = src.cnt_accounts_make_transactions

WHEN NOT MATCHED THEN INSERT
	(date_update, currency_from, amount_total, cnt_transactions, avg_transactions_per_account, cnt_accounts_make_transactions)
	VALUES (
		src.date_update,
		src.currency_from,
		src.amount_total,
		src.cnt_transactions,
		src.avg_transactions_per_account,
		src.cnt_accounts_make_transactions
	);
