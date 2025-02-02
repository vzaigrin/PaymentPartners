DROP TABLE IF EXISTS PP.STG_TELECOM;
CREATE TABLE PP.STG_TELECOM (
    operation_ts                TIMESTAMP
    , operation_country         STRING
    , operation_city            STRING
    , card_bin                  STRING
    , card_number               STRING
    , service                   STRING
    , payment_tariff            FLOAT64
    , payment_ps                FLOAT64
);

DROP TABLE IF EXISTS PP.ODS_TELECOM;
CREATE TABLE PP.ODS_TELECOM (
    operation_ts                TIMESTAMP
    , operation_country         STRING
    , operation_city            STRING
    , card_bin                  STRING
    , card_number               STRING
    , service                   STRING
    , payment_tariff            FLOAT64
    , payment_ps                FLOAT64
    , period_year               INT64
    , period_month              INT64
    , filename                  STRING
    , load_ts                   TIMESTAMP
);

CREATE OR REPLACE VIEW `my-project-1530001957977.TELECOM.V_DM_LOADS` AS
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

CREATE OR REPLACE VIEW `my-project-1530001957977.TELECOM.V_DM_BANK` AS
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

CREATE OR REPLACE VIEW `my-project-1530001957977.TELECOM.V_DM_CARD` AS
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

CREATE OR REPLACE VIEW `my-project-1530001957977.TELECOM.V_DM_CITY` AS
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

CREATE OR REPLACE VIEW `my-project-1530001957977.TELECOM.V_DM_COUNTRY` AS
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

CREATE OR REPLACE VIEW `my-project-1530001957977.TELECOM.V_DM_PRIVILEGE` AS
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

CREATE OR REPLACE VIEW `my-project-1530001957977.TELECOM.V_DM_PAYMENT` AS
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
