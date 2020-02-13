DROP TABLE IF EXISTS PP.STG_RETAIL;
CREATE TABLE PP.STG_RETAIL (
	order_id            	INT64
	, card_bin          	STRING
	, card_number       	STRING
	, bill_date         	TIMESTAMP
	, transaction_amount	FLOAT64
	, ps_financing      	FLOAT64
	, partner_financing 	FLOAT64
	, location          	STRING
);

DROP TABLE IF EXISTS PP.ODS_RETAIL;
CREATE TABLE PP.ODS_RETAIL (
	order_id            	INT64
	, card_bin          	STRING
	, card_number       	STRING
	, bill_date         	TIMESTAMP
	, transaction_amount	FLOAT64
	, ps_financing      	FLOAT64
	, partner_financing 	FLOAT64
	, location          	STRING
	, period_year       	INT64
	, period_month      	INT64
	, filename          	STRING
	, load_ts           	TIMESTAMP
);

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_LOADS_RETAIL` AS
SELECT
	period_name
	, period_year
	, period_month
	, filename
	, load_ts
	, stg
	, ods
	, dds
	, bad
FROM `my-project-1530001957977.PP.DM_LOADS` p
WHERE upper(p.partner_name) = 'RETAIL'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_BANK_RETAIL` AS
SELECT
	sum_opers
	, avg_total
	, bank
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_BANK` p
WHERE upper(p.partner_name) = 'RETAIL'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CARD_RETAIL` AS
SELECT
	sum_opers
	, avg_total
	, card_type
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_CARD` p
WHERE upper(p.partner_name) = 'RETAIL'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CITY_RETAIL` AS
SELECT
	sum_opers
	, avg_total
	, operation_city
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_CITY` p
WHERE upper(p.partner_name) = 'RETAIL'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_COUNTRY_RETAIL` AS
SELECT
	sum_opers
	, avg_total
	, operation_country
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_COUNTRY` p
WHERE upper(p.partner_name) = 'RETAIL'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PRIVILEGE_RETAIL` AS
SELECT
	sum_opers
	, privilege_type
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_PRIVILEGE` p
WHERE upper(p.partner_name) = 'RETAIL'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PAYMENT_RETAIL` AS
SELECT
	avg_client
	, avg_ps
	, avg_partner
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_PAYMENT` p
WHERE upper(p.partner_name) = 'RETAIL'
;

CREATE OR REPLACE PROCEDURE PP.LOAD_RETAIL (fname STRING, pyear INT64, pmonth INT64)
BEGIN

	DECLARE loadts TIMESTAMP;
	DECLARE stg INT64;
	DECLARE ods INT64;
	DECLARE dds INT64;

	SET loadts = CURRENT_TIMESTAMP;
	SET stg = (SELECT count(*) FROM PP.STG_RETAIL);

	DELETE FROM PP.ODS_RETAIL WHERE true;

	INSERT INTO PP.ODS_RETAIL
	SELECT DISTINCT
		COALESCE(order_id, 0)
		, COALESCE(card_bin, '')
		, COALESCE(card_number, '')
		, COALESCE(bill_date, TIMESTAMP(DATE(pyear, pmonth, 1)))
		, COALESCE(transaction_amount, 0.0)
		, COALESCE(ps_financing, 0.0)
		, COALESCE(partner_financing, 0.0)
		, COALESCE(location, '')
		, pyear AS period_year
		, pmonth AS period_month
		, fname AS filename
		, loadts AS load_ts
	FROM PP.STG_RETAIL
	WHERE DATE(bill_date) BETWEEN DATE(pyear, pmonth, 1) AND DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH)
	;
	
	SET ods = (SELECT count(*) FROM PP.ODS_RETAIL);

	DELETE FROM PP.TMP_DATA WHERE true;

	INSERT INTO PP.TMP_DATA
	SELECT
		card_bin AS bin
		, card_number AS card_number
		, bill_date AS operation_ts
		, pyear AS period_year
		, pmonth AS period_month
		, FORMAT("%4d-%02d", pyear, pmonth) AS period_name
		, 'Россия' AS operation_country
		, location AS operation_city
		, transaction_amount AS payment_total
		, transaction_amount AS payment_tariff
		, transaction_amount * (1 - (ps_financing + partner_financing) / 100.0) AS payment_main_client
		, transaction_amount * (ps_financing / 100.0) AS payment_ps
		, transaction_amount * (partner_financing / 100.0) AS payment_partner
		, 0.0 AS payment_other_client
		, CASE WHEN (transaction_amount < 10.0) THEN 'free' ELSE 'discount' END AS privilege_type
		, load_ts AS processed_dttm
	FROM PP.ODS_RETAIL
	;

	CALL PP.LOAD_DATA('retail', dds);

	INSERT INTO PP.DM_LOADS
	VALUES ('retail', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;
