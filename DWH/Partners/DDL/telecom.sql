DROP TABLE IF EXISTS PP.STG_TELECOM;
CREATE TABLE PP.STG_TELECOM (
	operation_ts       	TIMESTAMP
	, operation_country	STRING
	, operation_city   	STRING
	, card_bin         	STRING
	, card_number      	STRING
	, service          	STRING
	, payment_tariff   	FLOAT64
	, payment_ps       	FLOAT64
);

DROP TABLE IF EXISTS PP.ODS_TELECOM;
CREATE TABLE PP.ODS_TELECOM (
	operation_ts       	TIMESTAMP
	, operation_country	STRING
	, operation_city   	STRING
	, card_bin         	STRING
	, card_number      	STRING
	, service          	STRING
	, payment_tariff   	FLOAT64
	, payment_ps       	FLOAT64
	, period_year      	INT64
	, period_month     	INT64
	, filename         	STRING
	, load_ts          	TIMESTAMP
);

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_LOADS_TELECOM` AS
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
WHERE upper(p.partner_name) = 'TELECOM'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_BANK_TELECOM` AS
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
WHERE upper(p.partner_name) = 'TELECOM'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CARD_TELECOM` AS
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
WHERE upper(p.partner_name) = 'TELECOM'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CITY_TELECOM` AS
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
WHERE upper(p.partner_name) = 'TELECOM'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_COUNTRY_TELECOM` AS
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
WHERE upper(p.partner_name) = 'TELECOM'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PRIVILEGE_TELECOM` AS
SELECT
	sum_opers
	, privilege_type
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_PRIVILEGE` p
WHERE upper(p.partner_name) = 'TELECOM'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PAYMENT_TELECOM` AS
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
WHERE upper(p.partner_name) = 'TELECOM'
;

CREATE OR REPLACE PROCEDURE PP.LOAD_TELECOM (fname STRING, pyear INT64, pmonth INT64)
BEGIN

	DECLARE loadts TIMESTAMP;
	DECLARE stg INT64;
	DECLARE ods INT64;
	DECLARE dds INT64;

	SET loadts = CURRENT_TIMESTAMP;
	SET stg = (SELECT count(*) FROM PP.STG_TELECOM);

	DELETE FROM PP.ODS_TELECOM WHERE true;

	INSERT INTO PP.ODS_TELECOM
	SELECT DISTINCT
		COALESCE(operation_ts, TIMESTAMP(DATE(pyear, pmonth, 1)))
		, COALESCE(operation_country, '')
		, COALESCE(operation_city, '')
		, COALESCE(card_bin, '')
		, COALESCE(card_number, '')
		, COALESCE(service, '')
		, COALESCE(payment_tariff, 0.0)
		, COALESCE(payment_ps, 0.0)
		, pyear AS period_year
		, pmonth AS period_month
		, fname AS filename
		, loadts AS load_ts
	FROM PP.STG_TELECOM
	WHERE DATE(operation_ts) BETWEEN DATE(pyear, pmonth, 1) AND DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH)
	;
	
	SET ods = (SELECT count(*) FROM PP.ODS_TELECOM);

	DELETE FROM PP.TMP_DATA WHERE true;

	INSERT INTO PP.TMP_DATA
	SELECT
		card_bin AS bin
		, card_number AS card_number
		, operation_ts AS operation_ts
		, pyear AS period_year
		, pmonth AS period_month
		, FORMAT("%4d-%02d", pyear, pmonth) AS period_name
		, operation_country AS operation_country
		, operation_city AS operation_city
		, payment_tariff AS payment_total
		, payment_tariff AS payment_tariff
		, 0.0 AS payment_main_client
		, payment_tariff * (payment_ps / 100.0) AS payment_ps
		, payment_tariff * (1 - (payment_ps / 100.0)) AS payment_partner
		, 0.0 AS payment_other_client
		, 'free' AS privilege_type
		, load_ts AS processed_dttm
	FROM PP.ODS_TELECOM
	;

	CALL PP.LOAD_DATA('telecom', dds);

	INSERT INTO PP.DM_LOADS
	VALUES ('telecom', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;
