CREATE SCHEMA IF NOT EXISTS PP;

-- Stage
DROP TABLE IF EXISTS PP.STG_TAXI;
CREATE TABLE PP.STG_TAXI (
    datetime                    TIMESTAMP NOT NULL
    , class                     VARCHAR(64) NOT NULL
    , tariff                    DECIMAL(38,2) NOT NULL
    , ps_financing              DECIMAL(38,2) NOT NULL
    , taxi_financing            DECIMAL(38,2) NOT NULL
    , client_price              DECIMAL(38,2) NOT NULL
    , last4                     INTEGER NOT NULL
    , bin_number                INTEGER NOT NULL
    , card_type                 VARCHAR(64) NOT NULL
    , card_is_premium           BOOLEAN
    , payment_total             DECIMAL(38,2) NOT NULL
    , payment_other_client      DECIMAL(38,2) NOT NULL
    , free_discount             VARCHAR(64)
    , campaign_name             VARCHAR(64)
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (datetime, bin_number, last4)
) 
ORDER BY datetime, bin_number, last4, load_ts
SEGMENTED BY HASH(datetime, bin_number, last4) ALL NODES
;

DROP TABLE IF EXISTS PP.STG_TELECOM;
CREATE TABLE PP.STG_TELECOM (
    payment_type                VARCHAR(64) NOT NULL
    , operation_country         VARCHAR(64) NOT NULL
    , card_bin                  INTEGER NOT NULL
    , card_number               INTEGER NOT NULL
    , card_type                 VARCHAR(64) NOT NULL
    , card_is_premium           BOOLEAN
    , operation_ts              TIMESTAMP NOT NULL
    , payment_tariff            DECIMAL(38,2) NOT NULL
    , payment_ps                DECIMAL(38,2) NOT NULL
    , of_id                     VARCHAR(64)
    , discount_id               VARCHAR(64)
    , operation_city            VARCHAR(64)
    , campaign_name             VARCHAR(64)
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (operation_ts, card_bin, card_number)
) 
ORDER BY operation_ts, card_bin, card_number, load_ts
SEGMENTED BY HASH(operation_ts, card_bin, card_number) ALL NODES
;

DROP TABLE IF EXISTS PP.STG_CINEMA;
CREATE TABLE PP.STG_CINEMA (
    cinema_code                 VARCHAR(64) NOT NULL
    , cinema_name               VARCHAR(64) NOT NULL
    , trans_number              VARCHAR(64) NOT NULL
    , trans_time                TIMESTAMP NOT NULL
    , discount_type             VARCHAR(64) NOT NULL
    , trans_type                VARCHAR(64) NOT NULL
    , base_price                DECIMAL(38,2) NOT NULL
    , ticket_price              DECIMAL(38,2) NOT NULL
    , discount                  DECIMAL(38,2) NOT NULL
    , film                      VARCHAR(64) NOT NULL
    , session_time              TIMESTAMP NOT NULL
    , card_bin                  INTEGER NOT NULL
    , card_number               INTEGER NOT NULL
    , payment_partner           DECIMAL(38,2)
    , card_is_premium           BOOLEAN
    , payment_total             DECIMAL(38,2)
    , payment_other_client      DECIMAL(38,2)
    , free_discount             VARCHAR(64)
    , of_id                     VARCHAR(64)
    , campaign_name             VARCHAR(64)
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (trans_time, card_bin, card_number)
) 
ORDER BY trans_time, card_bin, card_number, load_ts
SEGMENTED BY HASH(trans_time, card_bin, card_number) ALL NODES
;

DROP TABLE IF EXISTS PP.STG_RETAIL;
CREATE TABLE PP.STG_RETAIL (
    order_id                    VARCHAR(64) NOT NULL
    , card_bin                  INTEGER NOT NULL
    , card_number               INTEGER NOT NULL
    , card_type                 VARCHAR(64) NOT NULL
    , bill_date                 TIMESTAMP NOT NULL
    , transaction_amount        DECIMAL(38,2) NOT NULL
    , ps_financing              DECIMAL(38,2) NOT NULL
    , partner_financing         DECIMAL(38,2) NOT NULL
    , client_price              DECIMAL(38,2) NOT NULL
    , priviledge_type           VARCHAR(64) NOT NULL
    , location                  VARCHAR(64) NOT NULL
    , card_is_premium           BOOLEAN
    , payment_total             DECIMAL(38,2)
    , payment_other_client      DECIMAL(38,2)
    , free_discount             VARCHAR(64)
    , campaign_name             VARCHAR(64)
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (bill_date, card_bin, card_number)
) 
ORDER BY bill_date, card_bin, card_number, load_ts
SEGMENTED BY HASH(bill_date, card_bin, card_number) ALL NODES
;


-- ODS
DROP TABLE IF EXISTS PP.ODS_TAXI;
CREATE TABLE PP.ODS_TAXI (
    datetime                    TIMESTAMP NOT NULL
    , class                     VARCHAR(64) NOT NULL
    , tariff                    DECIMAL(38,2) NOT NULL
    , ps_financing              DECIMAL(38,2) NOT NULL
    , taxi_financing            DECIMAL(38,2) NOT NULL
    , client_price              DECIMAL(38,2) NOT NULL
    , last4                     VARCHAR(4) NOT NULL
    , bin_number                INTEGER NOT NULL
    , card_type                 VARCHAR(64) NOT NULL
    , card_is_premium           BOOLEAN NOT NULL
    , payment_total             DECIMAL(38,2) NOT NULL
    , payment_other_client      DECIMAL(38,2) NOT NULL
    , free_discount             VARCHAR(64) NOT NULL
    , campaign_name             VARCHAR(64) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (datetime, bin_number, last4)
) 
ORDER BY datetime, bin_number, last4, load_ts
SEGMENTED BY HASH(datetime, bin_number, last4) ALL NODES
;

DROP TABLE IF EXISTS PP.ODS_TELECOM;
CREATE TABLE PP.ODS_TELECOM (
    payment_type                VARCHAR(64) NOT NULL
    , operation_country         VARCHAR(64) NOT NULL
    , card_bin                  INTEGER NOT NULL
    , card_number               INTEGER NOT NULL
    , card_type                 VARCHAR(64) NOT NULL
    , card_is_premium           BOOLEAN NOT NULL
    , operation_ts              TIMESTAMP NOT NULL
    , payment_tariff            DECIMAL(38,2) NOT NULL
    , payment_ps                DECIMAL(38,2) NOT NULL
    , of_id                     VARCHAR(64) NOT NULL
    , discount_id               VARCHAR(64) NOT NULL
    , operation_city            VARCHAR(64) NOT NULL
    , campaign_name             VARCHAR(64) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (operation_ts, card_bin, card_number)
) 
ORDER BY operation_ts, card_bin, card_number, load_ts
SEGMENTED BY HASH(operation_ts, card_bin, card_number) ALL NODES
;

DROP TABLE IF EXISTS PP.ODS_CINEMA;
CREATE TABLE PP.ODS_CINEMA (
    cinema_code                 VARCHAR(64) NOT NULL
    , cinema_name               VARCHAR(64) NOT NULL
    , trans_number              VARCHAR(64) NOT NULL
    , trans_time                TIMESTAMP NOT NULL
    , discount_type             VARCHAR(64) NOT NULL
    , trans_type                VARCHAR(64) NOT NULL
    , base_price                DECIMAL(38,2) NOT NULL
    , ticket_price              DECIMAL(38,2) NOT NULL
    , discount                  DECIMAL(38,2) NOT NULL
    , film                      VARCHAR(64) NOT NULL
    , session_time              TIMESTAMP NOT NULL
    , card_bin                  INTEGER NOT NULL
    , card_number               INTEGER NOT NULL
    , payment_partner           DECIMAL(38,2) NOT NULL
    , card_is_premium           BOOLEAN NOT NULL
    , payment_total             DECIMAL(38,2) NOT NULL
    , payment_other_client      DECIMAL(38,2) NOT NULL
    , free_discount             VARCHAR(64) NOT NULL
    , of_id                     VARCHAR(64) NOT NULL
    , campaign_name             VARCHAR(64) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (trans_time, card_bin, card_number)
) 
ORDER BY trans_time, card_bin, card_number, load_ts
SEGMENTED BY HASH(trans_time, card_bin, card_number) ALL NODES
;

DROP TABLE IF EXISTS PP.ODS_RETAIL;
CREATE TABLE PP.ODS_RETAIL (
    order_id                    VARCHAR(64) NOT NULL
    , card_bin                  INTEGER NOT NULL
    , card_number               INTEGER NOT NULL
    , card_type                 VARCHAR(64) NOT NULL
    , bill_date                 TIMESTAMP NOT NULL
    , transaction_amount        DECIMAL(38,2) NOT NULL
    , ps_financing              DECIMAL(38,2) NOT NULL
    , partner_financing         DECIMAL(38,2) NOT NULL
    , client_price              DECIMAL(38,2) NOT NULL
    , priviledge_type           VARCHAR(64) NOT NULL
    , location                  VARCHAR(64) NOT NULL
    , card_is_premium           BOOLEAN NOT NULL
    , payment_total             DECIMAL(38,2) NOT NULL
    , payment_other_client      DECIMAL(38,2) NOT NULL
    , free_discount             VARCHAR(64) NOT NULL
    , campaign_name             VARCHAR(64) NOT NULL
	, filename					VARCHAR(64) NOT NULL
	, load_ts					TIMESTAMP NOT NULL
	, PRIMARY KEY (bill_date, card_bin, card_number)
) 
ORDER BY bill_date, card_bin, card_number, load_ts
SEGMENTED BY HASH(bill_date, card_bin, card_number) ALL NODES
;


-- DDS
DROP TABLE IF EXISTS PP.HUB_PARTNERS;
CREATE TABLE PP.HUB_PARTNERS (
    partner_id                  UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
	, PRIMARY KEY (partner_id, processed_dttm)
) 
ORDER BY partner_id, processed_dttm
SEGMENTED BY HASH(partner_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.SUB_PARTNERS;
CREATE TABLE PP.SUB_PARTNERS (
    partner_id                  UUID NOT NULL
    , partner_name              VARCHAR(64) NOT NULL
    , tag                       VARCHAR(64) NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (partner_id, processed_dttm)
) 
ORDER BY partner_id, partner_name, processed_dttm
SEGMENTED BY HASH(partner_id, partner_name, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.HUB_DATA;
CREATE TABLE PP.HUB_DATA (
    data_id                      UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
	, PRIMARY KEY (data_id, processed_dttm)
) 
ORDER BY data_id, processed_dttm
SEGMENTED BY HASH(data_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.SUB_DATA;
CREATE TABLE PP.SUB_DATA (
    data_id                     UUID NOT NULL
    , card_number               INTEGER NOT NULL
    , card_is_premium           BOOLEAN NOT NULL
    , operation_ts              TIMESTAMP NOT NULL
    , period_year               INTEGER NOT NULL
    , period_month              INTEGER NOT NULL
    , period_name               VARCHAR(64) NOT NULL
    , operation_country         VARCHAR(64) NOT NULL
    , operation_city            VARCHAR(64) NOT NULL
    , payment_total             DECIMAL(38,2) NOT NULL
    , payment_tariff            DECIMAL(38,2) NOT NULL
    , payment_main_client       DECIMAL(38,2) NOT NULL
    , payment_visa              DECIMAL(38,2) NOT NULL
    , payment_partner           DECIMAL(38,2) NOT NULL
    , payment_other_client      DECIMAL(38,2) NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (data_id, card_number, period_name)
) 
ORDER BY data_id, card_number, period_name, processed_dttm
SEGMENTED BY HASH(data_id, card_number, period_name, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.LNK_DATA_PARTNERS;
CREATE TABLE PP.LNK_DATA_PARTNERS (
    data_id                     UUID NOT NULL
    , partner_id                UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (data_id, partner_id, processed_dttm)
) 
ORDER BY data_id, partner_id, processed_dttm
SEGMENTED BY HASH(data_id, partner_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.LNK_DATA_BINS;
CREATE TABLE PP.LNK_DATA_BINS (
    data_id                     UUID NOT NULL
    , bin_id                    UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (data_id, bin_id, processed_dttm)
) 
ORDER BY data_id, bin_id, processed_dttm
SEGMENTED BY HASH(data_id, bin_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.LNK_DATA_OFFERS;
CREATE TABLE PP.LNK_DATA_OFFERS (
    data_id                     UUID NOT NULL
    , offer_id                  UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (data_id, offer_id, processed_dttm)
) 
ORDER BY data_id, offer_id, processed_dttm
SEGMENTED BY HASH(data_id, offer_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.LNK_DATA_PRIVILEGES;
CREATE TABLE PP.LNK_DATA_PRIVILEGES (
    data_id                     UUID NOT NULL
    , privilege_id              UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (data_id, privilege_id, processed_dttm)
) 
ORDER BY data_id, privilege_id, processed_dttm
SEGMENTED BY HASH(data_id, privilege_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.LNK_DATA_CAMPAIGNS;
CREATE TABLE PP.LNK_DATA_CAMPAIGNS (
    data_id                     UUID NOT NULL
    , campaign_id               UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (data_id, campaign_id, processed_dttm)
) 
ORDER BY data_id, campaign_id, processed_dttm
SEGMENTED BY HASH(data_id, campaign_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.HUB_BINS;
CREATE TABLE PP.HUB_BINS (
    bin_id                      UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
	, PRIMARY KEY (bin_id, processed_dttm)
) 
ORDER BY bin_id, processed_dttm
SEGMENTED BY HASH(bin_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.SUB_BINS;
CREATE TABLE PP.SUB_BINS (
    bin_id                      UUID NOT NULL
    , bin                       INTEGER NOT NULL
    , range_from                INTEGER NOT NULL
    , range_to                  INTEGER NOT NULL
    , bank                      VARCHAR(64) NOT NULL
    , card_type                 VARCHAR(64) NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (bin_id, bin, processed_dttm)
) 
ORDER BY bin_id, bin, processed_dttm
SEGMENTED BY HASH(bin_id, bin, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.HUB_OFFERS;
CREATE TABLE PP.HUB_OFFERS (
    offer_id                      UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
	, PRIMARY KEY (offer_id, processed_dttm)
) 
ORDER BY offer_id, processed_dttm
SEGMENTED BY HASH(offer_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.SUB_OFFERS;
CREATE TABLE PP.SUB_OFFERS (
    offer_id                    UUID NOT NULL
    , offer_name                VARCHAR(64) NOT NULL
    , partner_name              VARCHAR(64) NOT NULL
    , partner_class             VARCHAR(64) NOT NULL
    , operation_city            VARCHAR(64) NOT NULL
    , operation_country         VARCHAR(64) NOT NULL
    , purchase_type             VARCHAR(64) NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (offer_id, offer_name, processed_dttm)
) 
ORDER BY offer_id, offer_name, processed_dttm
SEGMENTED BY HASH(offer_id, offer_name, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.HUB_PRIVILEGES;
CREATE TABLE PP.HUB_PRIVILEGES (
    privilege_id                      UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
	, PRIMARY KEY (privilege_id, processed_dttm)
) 
ORDER BY privilege_id, processed_dttm
SEGMENTED BY HASH(privilege_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.SUB_PRIVILEGES;
CREATE TABLE PP.SUB_PRIVILEGES (
    privilege_id                UUID NOT NULL
    , privilege_name            VARCHAR(64) NOT NULL
    , privilege_short           VARCHAR(64) NOT NULL
    , privilege_full            VARCHAR(64) NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (privilege_id, privilege_name, processed_dttm)
) 
ORDER BY privilege_id, privilege_name, processed_dttm
SEGMENTED BY HASH(privilege_id, privilege_name, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.HUB_CAMPAIGNS;
CREATE TABLE PP.HUB_CAMPAIGNS (
    campaign_id                      UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
	, PRIMARY KEY (campaign_id, processed_dttm)
) 
ORDER BY campaign_id, processed_dttm
SEGMENTED BY HASH(campaign_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.SUB_CAMPAIGNS;
CREATE TABLE PP.SUB_CAMPAIGNS (
    campaign_id                 UUID NOT NULL
    , campaign_name             VARCHAR(64) NOT NULL
    , from_dt                   TIMESTAMP NOT NULL
    , to_dt                     TIMESTAMP NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (campaign_id, privilege_name, processed_dttm)
) 
ORDER BY campaign_id, privilege_name, processed_dttm
SEGMENTED BY HASH(campaign_id, privilege_name, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.LNK_CAMPAIGNS_PARTNERS;
CREATE TABLE PP.LNK_CAMPAIGNS_PARTNERS (
    campaign_id                 UUID NOT NULL
    , partner_id                UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (campaign_id, partner_id, processed_dttm)
) 
ORDER BY campaign_id, partner_id, processed_dttm
SEGMENTED BY HASH(campaign_id, partner_id, processed_dttm) ALL NODES
;

DROP TABLE IF EXISTS PP.LNK_CAMPAIGNS_BINS;
CREATE TABLE PP.LNK_CAMPAIGNS_BINS (
    campaign_id                 UUID NOT NULL
    , bin_id                    UUID NOT NULL
    , processed_dttm            TIMESTAMP NOT NULL
    , hash                      INTEGER NOT NULL
    , version                   INTEGER NOT NULL
	, PRIMARY KEY (campaign_id, bin_id, processed_dttm)
) 
ORDER BY campaign_id, bin_id, processed_dttm
SEGMENTED BY HASH(campaign_id, bin_id, processed_dttm) ALL NODES
;


-- Data Marts
