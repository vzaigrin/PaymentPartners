-- Stage
DROP TABLE IF EXISTS PP.STG_TAXI;
CREATE TABLE PP.STG_TAXI (
    datetime                    TIMESTAMP NOT NULL
    , ride_town                 STRING NOT NULL
    , bin_number                STRING NOT NULL
    , last4                     STRING NOT NULL
    , class                     STRING NOT NULL
    , tariff                    FLOAT64 NOT NULL
    , ps_financing              FLOAT64 NOT NULL
    , taxi_financing            FLOAT64 NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
)
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.STG_TELECOM;
CREATE TABLE PP.STG_TELECOM (
    operation_ts                TIMESTAMP NOT NULL
    , operation_country         STRING NOT NULL
    , operation_city            STRING
    , card_bin                  STRING NOT NULL
    , card_number               STRING NOT NULL
    , service                   STRING NOT NULL
    , payment_tariff            FLOAT64 NOT NULL
    , payment_ps                FLOAT64 NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
  
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.STG_CINEMA;
CREATE TABLE PP.STG_CINEMA (
    cinema_name                 STRING NOT NULL
    , trans_time                TIMESTAMP NOT NULL
    , discount_type             STRING NOT NULL
    , base_price                FLOAT64 NOT NULL
    , discount                  FLOAT64 NOT NULL
    , film                      STRING NOT NULL
    , rrn                       STRING NOT NULL
    , card_number               STRING NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.STG_RETAIL;
CREATE TABLE PP.STG_RETAIL (
    order_id                    STRING NOT NULL
    , card_bin                  STRING NOT NULL
    , card_number               STRING NOT NULL
    , bill_date                 TIMESTAMP NOT NULL
    , transaction_amount        FLOAT64 NOT NULL
    , ps_financing              FLOAT64 NOT NULL
    , partner_financing         FLOAT64 NOT NULL
    , location                  STRING NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;


-- ODS
DROP TABLE IF EXISTS PP.ODS_TAXI;
CREATE TABLE PP.ODS_TAXI (
    datetime                    TIMESTAMP NOT NULL
    , ride_town                 STRING NOT NULL
    , bin_number                STRING NOT NULL
    , last4                     STRING NOT NULL
    , class                     STRING NOT NULL
    , tariff                    FLOAT64 NOT NULL
    , ps_financing              FLOAT64 NOT NULL
    , taxi_financing            FLOAT64 NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.ODS_TELECOM;
CREATE TABLE PP.ODS_TELECOM (
    operation_ts                TIMESTAMP NOT NULL
    , operation_country         STRING NOT NULL
    , operation_city            STRING
    , card_bin                  STRING NOT NULL
    , card_number               STRING NOT NULL
    , service                   STRING NOT NULL
    , payment_tariff            FLOAT64 NOT NULL
    , payment_ps                FLOAT64 NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.ODS_CINEMA;
CREATE TABLE PP.ODS_CINEMA (
    cinema_name                 STRING NOT NULL
    , trans_time                TIMESTAMP NOT NULL
    , discount_type             STRING NOT NULL
    , base_price                FLOAT64 NOT NULL
    , discount                  FLOAT64 NOT NULL
    , film                      STRING NOT NULL
    , rrn                       STRING NOT NULL
    , card_number               STRING NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.ODS_RETAIL;
CREATE TABLE PP.ODS_RETAIL (
    order_id                    STRING NOT NULL
    , card_bin                  STRING NOT NULL
    , card_number               STRING NOT NULL
    , bill_date                 TIMESTAMP NOT NULL
    , transaction_amount        FLOAT64 NOT NULL
    , ps_financing              FLOAT64 NOT NULL
    , partner_financing         FLOAT64 NOT NULL
    , location                  STRING NOT NULL
    , period_name               STRING NOT NULL
    , filename					STRING NOT NULL
    , load_ts					TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;


-- DDS
DROP TABLE IF EXISTS PP.HUB_PARTNERS;
CREATE TABLE PP.HUB_PARTNERS (
    partner_id                  NUMERIC NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.SUB_PARTNERS;
CREATE TABLE PP.SUB_PARTNERS (
    partner_id                  NUMERIC NOT NULL
    , partner_name              STRING NOT NULL
    , tag                       STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INT64 NOT NULL
    , version                   INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.HUB_DATA;
CREATE TABLE PP.HUB_DATA (
    data_id                     NUMERIC NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.SUB_DATA;
CREATE TABLE PP.SUB_DATA (
    data_id                     NUMERIC NOT NULL
    , card_number               STRING NOT NULL
    , card_is_premium           BOOL NOT NULL
    , operation_ts              TIMESTAMP NOT NULL
    , period_year               INT64 NOT NULL
    , period_month              INT64 NOT NULL
    , period_name               STRING NOT NULL
    , operation_country         STRING NOT NULL
    , operation_city            STRING NOT NULL
    , payment_total             FLOAT64 NOT NULL
    , payment_tariff            FLOAT64 NOT NULL
    , payment_main_client       FLOAT64 NOT NULL
    , payment_ps                FLOAT64 NOT NULL
    , payment_partner           FLOAT64 NOT NULL
    , payment_other_client      FLOAT64 NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INT64 NOT NULL
    , version                   INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.LNK_DATA_PARTNERS;
CREATE TABLE PP.LNK_DATA_PARTNERS (
    data_id                     NUMERIC NOT NULL
    , partner_id                NUMERIC NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INT64 NOT NULL
    , version                   INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.LNK_DATA_BINS;
CREATE TABLE PP.LNK_DATA_BINS (
    data_id                     NUMERIC NOT NULL
    , bin_id                    NUMERIC NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INT64 NOT NULL
    , version                   INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.LNK_DATA_PRIVILEGES;
CREATE TABLE PP.LNK_DATA_PRIVILEGES (
    data_id                     NUMERIC NOT NULL
    , privilege_id              NUMERIC NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INT64 NOT NULL
    , version                   INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.HUB_BINS;
CREATE TABLE PP.HUB_BINS (
    bin_id                      NUMERIC NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.SUB_BINS;
CREATE TABLE PP.SUB_BINS (
    bin_id                      NUMERIC NOT NULL
    , bin                       STRING NOT NULL
    , range_from                INT64 NOT NULL
    , range_to                  INT64 NOT NULL
    , bank                      STRING NOT NULL
    , card_type                 STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INT64 NOT NULL
    , version                   INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.HUB_PRIVILEGES;
CREATE TABLE PP.HUB_PRIVILEGES (
    privilege_id                NUMERIC NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

DROP TABLE IF EXISTS PP.SUB_PRIVILEGES;
CREATE TABLE PP.SUB_PRIVILEGES (
    privilege_id                NUMERIC NOT NULL
    , privilege_type            STRING NOT NULL
    , privilege_short           STRING NOT NULL
    , privilege_full            STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INT64 NOT NULL
    , version                   INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;


-- Data Marts
DROP TABLE IF EXISTS PP.DM_REPORT;
CREATE TABLE PP.DM_REPORT (
    partner_name                STRING NOT NULL
    , card_type                 STRING NOT NULL
    , bank                      STRING NOT NULL
    , operation_dt              DATE NOT NULL
    , operation_day             INT64 NOT NULL
    , operation_country         STRING NOT NULL
    , operation_city            STRING NOT NULL
    , privilege_type            STRING NOT NULL
    , purchase_type             STRING NOT NULL
    , partner_class             STRING NOT NULL
    , period_name               STRING NOT NULL
    , period_year               INT64 NOT NULL
    , period_month              INT64 NOT NULL
    , week_num                  INT64 NOT NULL
    , payment_total             FLOAT64 NOT NULL
    , payment_tariff            FLOAT64 NOT NULL
    , payment_main_client       FLOAT64 NOT NULL
    , payment_ps                FLOAT64 NOT NULL
    , payment_partner           FLOAT64 NOT NULL
    , payment_other_client      FLOAT64 NOT NULL
    , trans_num                 INT64 NOT NULL
) 
PARTITION BY _PARTITIONDATE
;

CREATE OR REPLACE VIEW PP.V_DM_PARTNERS_CARD AS
SELECT
    sum(trans_num) AS sum_opers
    , avg(payment_total) AS avg_total
    , partner_name
    , card_type
    , operation_day
    , period_name
FROM PP.DM_PARTNERS p
GROUP BY partner_name, card_type, operation_day, period_name
ORDER BY partner_name, card_type, operation_day, period_name
;

CREATE OR REPLACE VIEW PP.V_DM_PARTNERS_BANK AS
SELECT
    sum(trans_num) AS sum_opers
    , avg(payment_total) AS avg_total
    , partner_name
    , bank
    , operation_day
    , period_name
FROM PP.DM_PARTNERS p
GROUP BY partner_name, bank, operation_day, period_name
ORDER BY partner_name, bank, operation_day, period_name
;

CREATE OR REPLACE VIEW PP.V_DM_PARTNERS_CITY AS
SELECT
    sum(trans_num) AS sum_opers
    , avg(payment_total) AS avg_total
    , partner_name
    , operation_city
    , operation_day
    , period_name
FROM PP.DM_PARTNERS p
GROUP BY partner_name, operation_city, operation_day, period_name
ORDER BY partner_name, operation_city, operation_day, period_name
;

CREATE OR REPLACE VIEW PP.V_DM_PARTNERS_PURCHASE AS
SELECT
    sum(trans_num) AS sum_opers
    , sum(payment_total) AS sum_total
    , sum(payment_ps) AS sum_ps
    , avg(payment_total) AS avg_total
    , partner_name
    , purchase_type
    , operation_day
    , period_name
FROM PP.DM_PARTNERS p
GROUP BY partner_name, purchase_type, operation_day, period_name
ORDER BY partner_name, purchase_type, operation_day, period_name
;

CREATE OR REPLACE VIEW PP.V_DM_PARTNERS_PRIVILEGE AS
SELECT
    sum(trans_num) AS sum_opers
    , partner_name
    , privilege_type
    , operation_day
    , period_name
FROM PP.DM_PARTNERS p
GROUP BY partner_name, privilege_type, operation_day, period_name
ORDER BY partner_name, privilege_type, operation_day, period_name
;

CREATE OR REPLACE VIEW PP.V_DM_PARTNERS_PAYMENT AS
SELECT
    avg(payment_main_client) AS client
    , avg(payment_ps) AS ps
    , avg(payment_partner) AS client
    , partner_name
    , operation_day
    , period_name
FROM PP.DM_PARTNERS p
GROUP BY partner_name, operation_day, period_name
ORDER BY partner_name, operation_day, period_name
;

CREATE OR REPLACE VIEW PP.V_DM_TAXI AS
SELECT
FROM PP.DM_PARTNERS p
WHERE upper(p.partner_name) = 'TAXI'
;
