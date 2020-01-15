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

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_CARD` AS
SELECT
    sum_opers
    , avg_total
    , card_type
    , operation_day
    , period_name
FROM `my-project-1530001957977.PP.V_DM_CARD` p
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_BANK` AS
SELECT
    sum_opers
    , avg_total
    , bank
    , operation_day
    , period_name
FROM `my-project-1530001957977.PP.V_DM_BANK` p
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_CITY` AS
SELECT
    sum_opers
    , avg_total
    , operation_city
    , operation_day
    , period_name
FROM `my-project-1530001957977.PP.V_DM_CITY` p
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_PRIVILEGE` AS
SELECT
    sum_opers
    , privilege_type
    , operation_day
    , period_name
FROM `my-project-1530001957977.PP.V_DM_PRIVILEGE` p
WHERE upper(p.partner_name) = 'TAXI'
;

CREATE OR REPLACE VIEW `my-project-1530001957977.TAXI.V_DM_PAYMENT` AS
SELECT
    avg_client
    , ps
    , avg_partner
    , operation_day
    , period_name
FROM `my-project-1530001957977.PP.V_DM_PAYMENT` p
WHERE upper(p.partner_name) = 'TAXI'
;
