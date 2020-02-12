DROP TABLE IF EXISTS PP.STG_TAXI;
CREATE TABLE PP.STG_TAXI (
	datetime        	TIMESTAMP
	, ride_town     	STRING
	, bin_number    	STRING
	, last4         	STRING
	, class         	STRING
	, tariff        	FLOAT64
	, ps_financing  	FLOAT64
	, taxi_financing	FLOAT64
);

DROP TABLE IF EXISTS PP.ODS_TAXI;
CREATE TABLE PP.ODS_TAXI (
	datetime        	TIMESTAMP
	, ride_town     	STRING
	, bin_number    	STRING
	, last4         	STRING
	, class         	STRING
	, tariff        	FLOAT64
	, ps_financing  	FLOAT64
	, taxi_financing	FLOAT64
	, period_year   	INT64
	, period_month  	INT64
	, filename      	STRING
	, load_ts       	TIMESTAMP
);

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_LOADS` AS
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
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_BANK` AS
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
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_CARD` AS
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
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_CITY` AS
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
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_COUNTRY` AS
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
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_PRIVILEGE` AS
SELECT
	sum_opers
	, privilege_type
	, operation_day
	, period_name
	, period_year
	, period_month
	, week_num
FROM `my-project-1530001957977.PP.V_DM_PRIVILEGE` p
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_PAYMENT` AS
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
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE PROCEDURE PP.LOAD_TAXI (fname STRING, pyear INT64, pmonth INT64)
BEGIN

	DECLARE loadts TIMESTAMP;
	DECLARE stg INT64;
	DECLARE ods INT64;
	DECLARE dds INT64;

	SET loadts = CURRENT_TIMESTAMP;
	SET stg = (SELECT count(*) FROM PP.STG_TAXI);

	DELETE FROM PP.ODS_TAXI WHERE true;

	INSERT INTO PP.ODS_TAXI
	SELECT DISTINCT
		COALESCE(datetime, TIMESTAMP(DATE(pyear, pmonth, 1)))
		, COALESCE(ride_town, '')
		, COALESCE(bin_number, '')
		, COALESCE(last4, '')
		, COALESCE(class, '')
		, COALESCE(tariff, 0.0)
		, COALESCE(ps_financing, 0.0)
		, COALESCE(taxi_financing, 0.0)
		, pyear AS period_year
		, pmonth AS period_month
		, fname AS filename
		, loadts AS load_ts
	FROM PP.STG_TAXI
	WHERE DATE(datetime) BETWEEN DATE(pyear, pmonth, 1) AND DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH)
	;
	
	SET ods = (SELECT count(*) FROM PP.ODS_TAXI);

	DELETE FROM PP.TMP_DATA WHERE true;

	INSERT INTO PP.TMP_DATA
	SELECT
		bin_number AS bin
		, last4 AS card_number
		, datetime AS operation_ts
		, pyear AS period_year
		, pmonth AS period_month
		, FORMAT("%4d-%02d", pyear, pmonth) AS period_name
		, 'Россия' AS operation_country
		, ride_town AS operation_city
		, tariff AS payment_total
		, tariff AS payment_tariff
		, tariff * (1 - (ps_financing + taxi_financing) / 100.0) AS payment_main_client
		, tariff * (ps_financing / 100.0) AS payment_ps
		, tariff * (taxi_financing / 100.0) AS payment_partner
		, 0.0 AS payment_other_client
		, CASE WHEN (tariff * (1 - (ps_financing + taxi_financing) / 100.0) < 10.0) THEN 'free' ELSE 'discount' END AS privilege_type
		, load_ts AS processed_dttm
	FROM PP.ODS_TAXI
	;

	CALL PP.LOAD_DATA('taxi', dds);

	INSERT INTO PP.DM_LOADS
	VALUES ('taxi', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;
