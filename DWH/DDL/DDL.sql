#standardSQL

-- Stage
DROP TABLE IF EXISTS PP.STG_BINS;
CREATE TABLE PP.STG_BINS (
    bin                         STRING
    , range_from                STRING
    , range_to                  STRING
    , bank                      STRING
    , card_type                 STRING
);

DROP TABLE IF EXISTS PP.STG_PRIVILEGES;
CREATE TABLE PP.STG_PRIVILEGES (
    privilege_type              STRING
    , privilege_short           STRING
    , privilege_full            STRING
);

DROP TABLE IF EXISTS PP.STG_CITY;
CREATE TABLE PP.STG_CITY (
    id                          STRING
    , city                      STRING
    , country                   STRING
);


-- ODS
DROP TABLE IF EXISTS PP.ODS_BINS;
CREATE TABLE PP.ODS_BINS (
    bin_id                      STRING
    , bin                       STRING
    , bank                      STRING
    , card_type                 STRING
    , processed_dttm            TIMESTAMP
    , valid_from_dttm           TIMESTAMP
    , valid_to_dttm             TIMESTAMP
    , _hash                     BYTES
);

DROP TABLE IF EXISTS PP.ODS_PRIVILEGES;
CREATE TABLE PP.ODS_PRIVILEGES (
    privilege_id                STRING
    , privilege_type            STRING
    , privilege_short           STRING
    , privilege_full            STRING
    , processed_dttm            TIMESTAMP
    , valid_from_dttm           TIMESTAMP
    , valid_to_dttm             TIMESTAMP
    , _hash                     BYTES
);

DROP TABLE IF EXISTS PP.ODS_CITY;
CREATE TABLE PP.ODS_CITY (
    city_id                     STRING
    , id                        STRING
    , city                      STRING
    , country                   STRING
    , processed_dttm            TIMESTAMP
    , valid_from_dttm           TIMESTAMP
    , valid_to_dttm             TIMESTAMP
    , _hash                     BYTES
);

DROP TABLE IF EXISTS PP.TMP_DATA;
CREATE TABLE PP.TMP_DATA (
    bin                         STRING
    , card_number               STRING
    , operation_ts              TIMESTAMP
    , period_year               INT64
    , period_month              INT64
    , period_name               STRING
    , operation_country         STRING
    , operation_city            STRING
    , payment_total             FLOAT64
    , payment_tariff            FLOAT64
    , payment_main_client       FLOAT64
    , payment_ps                FLOAT64
    , payment_partner           FLOAT64
    , payment_other_client      FLOAT64
    , privilege_type            STRING
    , processed_dttm            TIMESTAMP
);

DROP TABLE IF EXISTS PP.ODS_DATA;
CREATE TABLE PP.ODS_DATA (
    data_id                     STRING
    , card_number               STRING
    , operation_ts              TIMESTAMP
    , period_year               INT64
    , period_month              INT64
    , period_name               STRING
    , operation_country         STRING
    , operation_city            STRING
    , payment_total             FLOAT64
    , payment_tariff            FLOAT64
    , payment_main_client       FLOAT64
    , payment_ps                FLOAT64
    , payment_partner           FLOAT64
    , payment_other_client      FLOAT64
    , processed_dttm            TIMESTAMP
    , partner_id                STRING
    , bin_id                    STRING
    , privilege_id              STRING
    , city_id                   STRING
    , _hash                     BYTES
);


-- DDS
DROP TABLE IF EXISTS PP.HUB_DATA;
CREATE TABLE PP.HUB_DATA (
    data_id                     STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS PP.SAT_DATA;
CREATE TABLE PP.SAT_DATA (
    data_id                     STRING NOT NULL
    , card_number               STRING NOT NULL
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
    , _hash                     BYTES NOT NULL
);

DROP TABLE IF EXISTS PP.LNK_DATA_PARTNERS;
CREATE TABLE PP.LNK_DATA_PARTNERS (
    data_id                     STRING NOT NULL
    , partner_id                STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS PP.LNK_DATA_BINS;
CREATE TABLE PP.LNK_DATA_BINS (
    data_id                     STRING NOT NULL
    , bin_id                    STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS PP.LNK_DATA_PRIVILEGES;
CREATE TABLE PP.LNK_DATA_PRIVILEGES (
    data_id                     STRING NOT NULL
    , privilege_id              STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS PP.LNK_DATA_CITY;
CREATE TABLE PP.LNK_DATA_CITY (
    data_id                     STRING NOT NULL
    , city_id                   STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
);

DROP TABLE IF EXISTS PP.HUB_PARTNERS;
CREATE TABLE PP.HUB_PARTNERS (
    partner_id                  STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
);

DROP TABLE IF EXISTS PP.SAT_PARTNERS;
CREATE TABLE PP.SAT_PARTNERS (
    partner_id                  STRING NOT NULL
    , partner_name              STRING NOT NULL
    , tag                       STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
    , _hash                     BYTES NOT NULL
);

DROP TABLE IF EXISTS PP.HUB_BINS;
CREATE TABLE PP.HUB_BINS (
    bin_id                      STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
);

DROP TABLE IF EXISTS PP.SAT_BINS;
CREATE TABLE PP.SAT_BINS (
    bin_id                      STRING NOT NULL
    , bin                       STRING NOT NULL
    , bank                      STRING NOT NULL
    , card_type                 STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
    , _hash                     BYTES NOT NULL
);

DROP TABLE IF EXISTS PP.HUB_PRIVILEGES;
CREATE TABLE PP.HUB_PRIVILEGES (
    privilege_id                STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
);

DROP TABLE IF EXISTS PP.SAT_PRIVILEGES;
CREATE TABLE PP.SAT_PRIVILEGES (
    privilege_id                STRING NOT NULL
    , privilege_type            STRING NOT NULL
    , privilege_short           STRING NOT NULL
    , privilege_full            STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
    , _hash                     BYTES NOT NULL
);

DROP TABLE IF EXISTS PP.HUB_CITY;
CREATE TABLE PP.HUB_CITY (
    city_id                     STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
);

DROP TABLE IF EXISTS PP.SAT_CITY;
CREATE TABLE PP.SAT_CITY (
    city_id                     STRING NOT NULL
    , id                        STRING NOT NULL
    , city                      STRING NOT NULL
    , country                   STRING NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , valid_from_dttm           TIMESTAMP NOT NULL
    , valid_to_dttm             TIMESTAMP
    , _hash                     BYTES NOT NULL
);


-- Data Marts
DROP TABLE IF EXISTS PP.DM_LOADS;
CREATE TABLE PP.DM_LOADS (
    partner_name                STRING NOT NULL
    , period_name               STRING NOT NULL
    , period_year               INT64 NOT NULL
    , period_month              INT64 NOT NULL
    , filename                  STRING NOT NULL
    , load_ts                   TIMESTAMP NOT NULL
    , stg                       INT64
    , ods                       INT64
    , dds                       INT64
    , bad                       INT64
);

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
);

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_BANK` AS
SELECT
    sum(trans_num) AS sum_opers
    , avg(payment_total) AS avg_total
    , partner_name
    , bank
    , operation_day
    , period_name
    , period_year
    , period_month
    , week_num
FROM `my-project-1530001957977.PP.DM_REPORT` p
GROUP BY partner_name, bank, operation_day, period_name, period_year, period_month, week_num
ORDER BY partner_name, bank, operation_day, period_name, period_year, period_month, week_num
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CARD` AS
SELECT
    sum(trans_num) AS sum_opers
    , avg(payment_total) AS avg_total
    , partner_name
    , card_type
    , operation_day
    , period_name
    , period_year
    , period_month
    , week_num
FROM `my-project-1530001957977.PP.DM_REPORT` p
GROUP BY partner_name, card_type, operation_day, period_name, period_year, period_month, week_num
ORDER BY partner_name, card_type, operation_day, period_name, period_year, period_month, week_num
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_CITY` AS
SELECT
    sum(trans_num) AS sum_opers
    , avg(payment_total) AS avg_total
    , partner_name
    , operation_city
    , operation_day
    , period_name
    , period_year
    , period_month
    , week_num
FROM `my-project-1530001957977.PP.DM_REPORT` p
GROUP BY partner_name, operation_city, operation_day, period_name, period_year, period_month, week_num
ORDER BY partner_name, operation_city, operation_day, period_name, period_year, period_month, week_num
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_COUNTRY` AS
SELECT
    sum(trans_num) AS sum_opers
    , avg(payment_total) AS avg_total
    , partner_name
    , operation_country
    , operation_day
    , period_name
    , period_year
    , period_month
    , week_num
FROM `my-project-1530001957977.PP.DM_REPORT` p
GROUP BY partner_name, operation_country, operation_day, period_name, period_year, period_month, week_num
ORDER BY partner_name, operation_country, operation_day, period_name, period_year, period_month, week_num
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PRIVILEGE` AS
SELECT
    sum(trans_num) AS sum_opers
    , partner_name
    , privilege_type
    , operation_day
    , period_name
    , period_year
    , period_month
    , week_num
FROM `my-project-1530001957977.PP.DM_REPORT` p
GROUP BY partner_name, privilege_type, operation_day, period_name, period_year, period_month, week_num
ORDER BY partner_name, privilege_type, operation_day, period_name, period_year, period_month, week_num
;

CREATE OR REPLACE VIEW `my-project-1530001957977.PP.V_DM_PAYMENT` AS
SELECT
    avg(payment_main_client) AS avg_client
    , avg(payment_ps) AS avg_ps
    , avg(payment_partner) AS avg_partner
    , partner_name
    , operation_day
    , period_name
    , period_year
    , period_month
    , week_num
FROM `my-project-1530001957977.PP.DM_REPORT` p
GROUP BY partner_name, operation_day, period_name, period_year, period_month, week_num
ORDER BY partner_name, operation_day, period_name, period_year, period_month, week_num
;
