-- Тестирование процесса загрузки данных от партнёра Кино

-- Этап 1 - первоначальная загрузка данных

-- Загружаем данные в область STG
DELETE FROM PP.STG_CINEMA WHERE true;
INSERT INTO PP.STG_CINEMA VALUES 
('Кино 1', '2019-12-15 20:31:39', 'Platinum', 400.0, 3.0, 'Фильм 1', 472446, 8498),
('Кино 4', '2019-12-10 14:47:15', 'Signature', 500.0, 3.0, 'Фильм 1', 414097, 4592),
('Кино 5', '2019-12-14 13:56:06', 'Platinum', 300.0, 3.0, 'Фильм 1', 446915, 6633)
;

-- Перегружаем данные в область ODS
DELETE FROM PP.ODS_CINEMA WHERE true;
INSERT INTO PP.ODS_CINEMA
SELECT DISTINCT
    cinema_name
    , trans_time
    , discount_type
    , base_price
    , discount
    , film
    , CAST(rrn AS STRING) AS rrn
    , CAST(card_number AS STRING) AS card_number
    , 2019 AS period_year
    , 12 AS period_month
    , 'cinema-12.csv' AS filename
    , CURRENT_TIMESTAMP() AS load_ts
FROM PP.STG_CINEMA
WHERE trans_time BETWEEN TIMESTAMP(DATE(2019, 12, 1)) AND TIMESTAMP(DATE_ADD(DATE(2019, 12, 1), INTERVAL 1 MONTH))
;

-- Приводим данные к единому формату области DDS
DELETE FROM PP.TMP_DATA WHERE true;
INSERT INTO PP.TMP_DATA
SELECT
  GENERATE_UUID() AS data_id
  , card_number
  , operation_ts
  , period_year
  , period_month
  , period_name
  , operation_country
  , operation_city
  , payment_total
  , payment_tariff
  , payment_main_client
  , payment_ps
  , payment_partner
  , payment_other_client
  , t.processed_dttm
  , p.partner_id
  , b.bin_id
  , l.privilege_id
  , MD5(CONCAT(card_number, CAST(operation_ts AS STRING), CAST(period_year AS STRING), CAST(period_month AS STRING), period_name,
      operation_country, operation_city, CAST(payment_total AS STRING), CAST(payment_tariff AS STRING), CAST(payment_main_client AS STRING),
      CAST(payment_ps AS STRING), CAST(payment_partner AS STRING), CAST(payment_other_client AS STRING))) AS _hash
FROM (
  SELECT
    rrn AS bin
    , card_number AS card_number
    , trans_time AS operation_ts
    , 2019 AS period_year
    , 12 AS period_month
    , FORMAT("%4d-%02d", 2019, 12) AS period_name
    , 'Россия' AS operation_country
    , 'Москва' AS operation_city
    , base_price AS payment_total
    , base_price AS payment_tariff
    , base_price * (1 - (discount / 100.0)) AS payment_main_client
    , base_price * (discount / 100.0) AS payment_ps
    , 0 AS payment_partner
    , 0 AS payment_other_client
    , load_ts AS processed_dttm
  FROM PP.ODS_CINEMA
) t
LEFT JOIN PP.SAT_PARTNERS p
ON p.partner_name = 'cinema'
INNER JOIN PP.SAT_BINS b
ON b.bin = t.bin
INNER JOIN PP.SAT_PRIVILEGES l
ON l.privilege_type = 'discount'
WHERE p.valid_to_dttm IS NULL
    AND b.valid_to_dttm IS NULL
    AND l.valid_to_dttm IS NULL
;

-- Загружаем данные в область DDS
DELETE FROM PP.HUB_DATA WHERE true;
INSERT INTO PP.HUB_DATA
SELECT data_id, processed_dttm
FROM PP.TMP_DATA
;

DELETE FROM PP.SAT_DATA WHERE true;
INSERT INTO PP.SAT_DATA
SELECT
    data_id
    , card_number
    , operation_ts
    , period_year
    , period_month
    , period_name
    , operation_country
    , operation_city
    , payment_total
    , payment_tariff
    , payment_main_client
    , payment_ps
    , payment_partner
    , payment_other_client
    , processed_dttm
    , MD5(CONCAT(card_number, CAST(operation_ts AS STRING), CAST(period_year AS STRING), CAST(period_month AS STRING), period_name,
        operation_country, operation_city, CAST(payment_total AS STRING), CAST(payment_tariff AS STRING), CAST(payment_main_client AS STRING),
        CAST(payment_ps AS STRING), CAST(payment_partner AS STRING), CAST(payment_other_client AS STRING))) AS _hash
FROM PP.TMP_DATA
;

DELETE FROM PP.LNK_DATA_PARTNERS WHERE true;
INSERT INTO PP.LNK_DATA_PARTNERS
SELECT data_id, partner_id, processed_dttm
FROM PP.TMP_DATA
;

DELETE FROM PP.LNK_DATA_BINS WHERE true;
INSERT INTO PP.LNK_DATA_BINS
SELECT data_id, bin_id, processed_dttm
FROM PP.TMP_DATA
;

DELETE FROM PP.LNK_DATA_PRIVILEGES WHERE true;
INSERT INTO PP.LNK_DATA_PRIVILEGES
SELECT data_id, privilege_id, processed_dttm
FROM PP.TMP_DATA
;

-- Этап 2 - добавляем новые строки к старым

-- Загружаем данные в область STG
INSERT INTO PP.STG_CINEMA VALUES 
('Кино 1', '2019-12-25 20:31:39', 'Platinum', 400.0, 2.0, 'Фильм 10', 472446, 8948),
('Кино 4', '2019-12-15 14:47:15', 'Signature', 500.0, 2.0, 'Фильм 15', 414097, 4952),
('Кино 5', '2019-12-24 13:56:06', 'Platinum', 300.0, 2.0, 'Фильм 20', 446915, 6363)
;

-- Перегружаем данные в область ODS
DELETE FROM PP.ODS_CINEMA WHERE true;
INSERT INTO PP.ODS_CINEMA
SELECT DISTINCT
    cinema_name
    , trans_time
    , discount_type
    , base_price
    , discount
    , film
    , CAST(rrn AS STRING) AS rrn
    , CAST(card_number AS STRING) AS card_number
    , 2019 AS period_year
    , 12 AS period_month
    , 'cinema-12.csv' AS filename
    , CURRENT_TIMESTAMP() AS load_ts
FROM PP.STG_CINEMA
WHERE trans_time BETWEEN TIMESTAMP(DATE(2019, 12, 1)) AND TIMESTAMP(DATE_ADD(DATE(2019, 12, 1), INTERVAL 1 MONTH))
;

-- Приводим данные к единому формату области DDS
DELETE FROM PP.TMP_DATA WHERE true;
INSERT INTO PP.TMP_DATA
SELECT
  GENERATE_UUID() AS data_id
  , card_number
  , operation_ts
  , period_year
  , period_month
  , period_name
  , operation_country
  , operation_city
  , payment_total
  , payment_tariff
  , payment_main_client
  , payment_ps
  , payment_partner
  , payment_other_client
  , t.processed_dttm
  , p.partner_id
  , b.bin_id
  , l.privilege_id
  , MD5(CONCAT(card_number, CAST(operation_ts AS STRING), CAST(period_year AS STRING), CAST(period_month AS STRING), period_name,
      operation_country, operation_city, CAST(payment_total AS STRING), CAST(payment_tariff AS STRING), CAST(payment_main_client AS STRING),
      CAST(payment_ps AS STRING), CAST(payment_partner AS STRING), CAST(payment_other_client AS STRING))) AS _hash
FROM (
  SELECT
    rrn AS bin
    , card_number AS card_number
    , trans_time AS operation_ts
    , 2019 AS period_year
    , 12 AS period_month
    , FORMAT("%4d-%02d", 2019, 12) AS period_name
    , 'Россия' AS operation_country
    , 'Москва' AS operation_city
    , base_price AS payment_total
    , base_price AS payment_tariff
    , base_price * (1 - (discount / 100.0)) AS payment_main_client
    , base_price * (discount / 100.0) AS payment_ps
    , 0 AS payment_partner
    , 0 AS payment_other_client
    , load_ts AS processed_dttm
  FROM PP.ODS_CINEMA
) t
LEFT JOIN PP.SAT_PARTNERS p
ON p.partner_name = 'cinema'
INNER JOIN PP.SAT_BINS b
ON b.bin = t.bin
INNER JOIN PP.SAT_PRIVILEGES l
ON l.privilege_type = 'discount'
WHERE p.valid_to_dttm IS NULL
    AND b.valid_to_dttm IS NULL
    AND l.valid_to_dttm IS NULL
;

-- Загружаем данные в область DDS
DELETE FROM PP.TMP_DATA_2 WHERE true;
INSERT INTO PP.TMP_DATA_2
SELECT t.*
FROM PP.TMP_DATA t
LEFT JOIN PP.SAT_DATA s
ON t._hash = s._hash
WHERE s._hash IS NULL
;

INSERT INTO PP.HUB_DATA
SELECT data_id, processed_dttm
FROM PP.TMP_DATA_2
;

INSERT INTO PP.SAT_DATA
SELECT
    data_id
    , card_number
    , operation_ts
    , period_year
    , period_month
    , period_name
    , operation_country
    , operation_city
    , payment_total
    , payment_tariff
    , payment_main_client
    , payment_ps
    , payment_partner
    , payment_other_client
    , processed_dttm
    , _hash
FROM PP.TMP_DATA_2
;

INSERT INTO PP.LNK_DATA_PARTNERS
SELECT data_id, partner_id, processed_dttm
FROM PP.TMP_DATA_2
;

INSERT INTO PP.LNK_DATA_BINS
SELECT data_id, bin_id, processed_dttm
FROM PP.TMP_DATA_2
;

INSERT INTO PP.LNK_DATA_PRIVILEGES
SELECT data_id, privilege_id, processed_dttm
FROM PP.TMP_DATA_2
;
