DROP TABLE IF EXISTS PP.STG_CINEMA;
CREATE TABLE PP.STG_CINEMA (
	cinema_name    	STRING
	, trans_time   	TIMESTAMP
	, discount_type	STRING
	, base_price   	FLOAT64
	, discount     	FLOAT64
	, film         	STRING
	, rrn          	STRING
	, card_number  	STRING
);

DROP TABLE IF EXISTS PP.ODS_CINEMA;
CREATE TABLE PP.ODS_CINEMA (
	cinema_name    	STRING
	, trans_time   	TIMESTAMP
	, discount_type	STRING
	, base_price   	FLOAT64
	, discount     	FLOAT64
	, film         	STRING
	, rrn          	STRING
	, card_number  	STRING
	, period_year  	INT64
	, period_month 	INT64
	, filename     	STRING
	, load_ts      	TIMESTAMP
);

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_LOADS_CINEMA` AS
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
WHERE upper(p.partner_name) = 'CINEMA'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_BANK_CINEMA` AS
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
WHERE upper(p.partner_name) = 'CINEMA'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CARD_CINEMA` AS
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
WHERE upper(p.partner_name) = 'CINEMA'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CITY_CINEMA` AS
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
WHERE upper(p.partner_name) = 'CINEMA'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_COUNTRY_CINEMA` AS
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
WHERE upper(p.partner_name) = 'CINEMA'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PRIVILEGE_CINEMA` AS
SELECT
	sum_opers
	, privilege_type
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_PRIVILEGE` p
WHERE upper(p.partner_name) = 'CINEMA'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PAYMENT_CINEMA` AS
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
WHERE upper(p.partner_name) = 'CINEMA'
;

CREATE OR REPLACE PROCEDURE PP.LOAD_CINEMA (fname STRING, pyear INT64, pmonth INT64)
BEGIN

	DECLARE loadts TIMESTAMP;
	DECLARE stg INT64;
	DECLARE ods INT64;
	DECLARE dds INT64;

	SET loadts = CURRENT_TIMESTAMP;
	SET stg = (SELECT count(*) FROM PP.STG_CINEMA);

	DELETE FROM PP.ODS_CINEMA WHERE true;

	INSERT INTO PP.ODS_CINEMA
	SELECT DISTINCT
		COALESCE(cinema_name, '')
		, COALESCE(trans_time, TIMESTAMP(DATE(pyear, pmonth, 1)))
		, COALESCE(discount_type, '')
		, COALESCE(base_price, 0.0)
		, COALESCE(discount, 0.0)
		, COALESCE(film, '')
		, COALESCE(rrn, '')
		, COALESCE(card_number, '')
		, pyear AS period_year
		, pmonth AS period_month
		, fname AS filename
		, loadts AS load_ts
	FROM PP.STG_CINEMA
	WHERE DATE(trans_time) BETWEEN DATE(pyear, pmonth, 1) AND DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH)
	;
	
	SET ods = (SELECT count(*) FROM PP.ODS_CINEMA);

	DELETE FROM PP.TMP_DATA WHERE true;

	INSERT INTO PP.TMP_DATA
	SELECT
		rrn AS bin
		, card_number AS card_number
		, trans_time AS operation_ts
		, pyear AS period_year
		, pmonth AS period_month
		, FORMAT("%4d-%02d", pyear, pmonth) AS period_name
		, 'Россия' AS operation_country
		, 'Москва' AS operation_city
		, base_price AS payment_total
		, base_price AS payment_tariff
		, base_price * (1 - (discount / 100.0)) AS payment_main_client
		, base_price * (discount / 100.0) AS payment_ps
		, 0.0 AS payment_partner
		, 0.0 AS payment_other_client
		, CASE WHEN (base_price * (1 - (discount / 100.0)) < 1.0) THEN 'free' ELSE 'discount' END AS privilege_type
		, load_ts AS processed_dttm
	FROM PP.ODS_CINEMA
	;

	CALL PP.LOAD_DATA('cinema', dds);

	INSERT INTO PP.DM_LOADS
	VALUES ('cinema', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;
