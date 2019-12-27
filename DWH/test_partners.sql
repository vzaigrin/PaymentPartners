-- Тестирование процесса перегрузки данных о партнёрах

-- Этап 1 - загрузка данных в пустые таблицы

-- Очищаем таблицу с данными о партнёрах в области Stage
DELETE FROM PP.STG_PARTNERS WHERE true;

-- Загружаем данные о партнёрах в область Stage
-- Данные содержат ошибки (дубликаты)
INSERT INTO PP.STG_PARTNERS (partner_name, tag)
VALUES ("cinema", "Cinema"),
       ("retail", "Retail"),
       ("taxi", "Taxi"),
       ("retail", "Retail"),
       ("telecom", "Telecom")
;

-- Проверяем как загрузились данные в область Stage
SELECT * FROM PP.STG_PARTNERS;
-- Должно быть 5 записей - все из предыдущего шага:
--Строка    partner_name    tag	
--1         taxi            Taxi
--2         telecom         Telecom
--3         retail          Retail
--4         retail          Retail
--5         cinema          Cinema

-- Очищаем таблицу с данными о партнёрах в области ODS
DELETE FROM PP.ODS_PARTNERS WHERE true;

-- Перегружаем данные о партнёрах из области Stage в область ODS
INSERT INTO PP.ODS_PARTNERS
SELECT DISTINCT * FROM PP.STG_PARTNERS;

-- Проверяем как загрузились данные в область ODS
SELECT * FROM PP.ODS_PARTNERS;
-- Должно быть 4 записи - без дубликатов:
--Строка    partner_name    tag	
--1         taxi            Taxi
--2         retail          Retail
--3         telecom         Telecom
--4         cinema          Cinema

-- Перегружаем данные о партнёрах из области ODS во временную таблицу
CREATE OR REPLACE TABLE PP.temp AS
SELECT
    GENERATE_UUID() AS partner_id
    , partner_name
    , tag
    , CURRENT_TIMESTAMP AS processed_dttm
    , CURRENT_TIMESTAMP AS valid_from_dttm
    , CAST(NULL AS TIMESTAMP) AS valid_to_dttm
    , MD5(CONCAT(partner_name, tag)) AS _hash
FROM PP.ODS_PARTNERS
;

-- Проверяем, что загрузилось во временную таблицу
SELECT * FROM PP.temp;
--Строка    partner_id	                            partner_name    tag	    processed_dttm	                valid_from_dttm                 valid_to_dttm	_hash	
--1         6c4870a4-6de9-4f33-af3f-04ae54ef53f9    telecom         Telecom 2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            +6vfAkoEdgA3VVJoIyORgQ==
--2         76efb5d0-e226-4148-ad90-0ff946b69d22    taxi            Taxi    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            E28N85GKAHO+JURQzjruhQ==
--3         66a42f3c-a8f3-4453-967c-772ed364b3e3    retail          Retail  2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            SLM/FPyGEiGXJc512VJ07w==
--4         6ced0978-43d3-40f3-beba-4dbd8ac38e83    cinema          Cinema  2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            ADFhMZyuajy2OPbKelnKSw==

-- Очищаем HUB_PARTNERS и заполняем его новыми данными
DELETE FROM PP.HUB_PARTNERS WHERE true;

INSERT INTO PP.HUB_PARTNERS
SELECT partner_id, processed_dttm, valid_from_dttm, valid_to_dttm
FROM PP.temp
;

-- Очищаем SAT_PARTNERS и заполняем его новыми данными
DELETE FROM PP.SAT_PARTNERS WHERE true;

INSERT INTO PP.SAT_PARTNERS
SELECT *
FROM PP.temp
;

-- Проверяем как загрузились данные в область DDS
SELECT * FROM PP.HUB_PARTNERS;
--Строка    partner_id                              processed_dttm                  valid_from_dttm                 valid_to_dttm	
--1         6c4870a4-6de9-4f33-af3f-04ae54ef53f9    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null
--2         76efb5d0-e226-4148-ad90-0ff946b69d22    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null
--3         6ced0978-43d3-40f3-beba-4dbd8ac38e83    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null
--4         66a42f3c-a8f3-4453-967c-772ed364b3e3    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null

SELECT * FROM PP.SAT_PARTNERS;
--Строка    partner_id                              partner_name    tag     processed_dttm                  valid_from_dttm                 valid_to_dttm   _hash	
--1         66a42f3c-a8f3-4453-967c-772ed364b3e3    retail          Retail  2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            SLM/FPyGEiGXJc512VJ07w==
--2         6ced0978-43d3-40f3-beba-4dbd8ac38e83    cinema          Cinema  2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            ADFhMZyuajy2OPbKelnKSw==
--3         76efb5d0-e226-4148-ad90-0ff946b69d22    taxi            Taxi    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            E28N85GKAHO+JURQzjruhQ==
--4         6c4870a4-6de9-4f33-af3f-04ae54ef53f9    telecom         Telecom 2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null            +6vfAkoEdgA3VVJoIyORgQ==


-- Этап 2 - изменяем данные - добавляем одного партнёра и удаляем одного партнёра

-- Очищаем таблицу с данными о партнёрах в области Stage
DELETE FROM PP.STG_PARTNERS WHERE true;

-- Загружаем данные о партнёрах в область Stage
-- Данные содержат ошибки (дубликаты)
INSERT INTO PP.STG_PARTNERS (partner_name, tag)
VALUES ("cinema", "Cinema"),
       ("retail", "Retail"),
       ("taxi2", "Taxi"),
       ("telecom", "Telecom")
;

-- Проверяем как загрузились данные в область Stage
SELECT * FROM PP.STG_PARTNERS;
-- Должно быть 4 записи - все из предыдущего шага:
--Строка    partner_name    tag	
--1         telecom         Telecom
--2         cinema          Cinema
--3         taxi2           Taxi
--4         retail          Retail

-- Очищаем таблицу с данными о партнёрах в области ODS
DELETE FROM PP.ODS_PARTNERS WHERE true;

-- Перегружаем данные о партнёрах из области Stage в область ODS
INSERT INTO PP.ODS_PARTNERS
SELECT DISTINCT * FROM PP.STG_PARTNERS;

-- Проверяем как загрузились данные в область ODS
SELECT * FROM PP.ODS_PARTNERS;
-- Должно быть 4 записи:
--Строка    partner_name    tag	
--1         retail          Retail
--2         taxi2           Taxi
--3         telecom         Telecom
--4         cinema          Cinema

-- Объединяем данные о партнёрах из области ODS с данными в области DDS во временную таблицу
-- Добавляем записи о партнёрах, которых не было
-- Помечаем устаревшими записи, которых уже нет
CREATE OR REPLACE TABLE PP.temp AS
SELECT
  COALESCE(s.partner_id, o.partner_id) AS partner_id
  , COALESCE(s.partner_name, o.partner_name) AS partner_name
  , COALESCE(s.tag, o.tag) AS tag
  , COALESCE(s.processed_dttm, o.processed_dttm) AS processed_dttm
  , COALESCE(s.valid_from_dttm, o.processed_dttm) AS valid_from_dttm
  , CASE
        WHEN o.partner_id IS NULL
        THEN CURRENT_TIMESTAMP
        ELSE CAST(NULL AS TIMESTAMP)
    END AS valid_to_dttm
  , COALESCE(s._hash, o._hash) AS _hash
FROM (
  SELECT
    GENERATE_UUID() AS partner_id
    , partner_name
    , tag
    , CURRENT_TIMESTAMP AS processed_dttm
    , MD5(CONCAT(partner_name, tag)) AS _hash
   FROM PP.ODS_PARTNERS
) o
FULL JOIN PP.SAT_PARTNERS s
ON o._hash = s._hash
;

-- Очищаем HUB_PARTNERS и заполняем его новыми данными
DELETE FROM PP.HUB_PARTNERS WHERE true;

INSERT INTO PP.HUB_PARTNERS
SELECT partner_id, processed_dttm, valid_from_dttm, valid_to_dttm
FROM PP.temp
;

-- Очищаем SAT_PARTNERS и заполняем его новыми данными
DELETE FROM PP.SAT_PARTNERS WHERE true;

INSERT INTO PP.SAT_PARTNERS
SELECT *
FROM PP.temp
;

-- Проверяем как загрузились данные в область DDS
SELECT * FROM PP.HUB_PARTNERS;
--Строка    partner_id                              processed_dttm                  valid_from_dttm                 valid_to_dttm	
--1         a50b9d98-e350-4317-b0b9-bc13a84074d6    2019-12-26 09:56:38.510598 UTC  2019-12-26 09:56:38.510598 UTC  null
--2         6c4870a4-6de9-4f33-af3f-04ae54ef53f9    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null
--3         6ced0978-43d3-40f3-beba-4dbd8ac38e83    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null
--4         66a42f3c-a8f3-4453-967c-772ed364b3e3    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null
--5         76efb5d0-e226-4148-ad90-0ff946b69d22    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  2019-12-26 09:56:38.510598 UTC

SELECT * FROM PP.SAT_PARTNERS;
--Строка    partner_id                              partner_name    tag     processed_dttm                  valid_from_dttm                 valid_to_dttm                   _hash	
--1         76efb5d0-e226-4148-ad90-0ff946b69d22    taxi            Taxi    2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  2019-12-26 09:56:38.510598 UTC  E28N85GKAHO+JURQzjruhQ==
--2         a50b9d98-e350-4317-b0b9-bc13a84074d6    taxi2           Taxi    2019-12-26 09:56:38.510598 UTC  2019-12-26 09:56:38.510598 UTC  null                            t3HvHJhx2GMxataEHHKNFw==
--3         6ced0978-43d3-40f3-beba-4dbd8ac38e83    cinema          Cinema  2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null                            ADFhMZyuajy2OPbKelnKSw==
--4         66a42f3c-a8f3-4453-967c-772ed364b3e3    retail          Retail  2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null                            SLM/FPyGEiGXJc512VJ07w==
--5         6c4870a4-6de9-4f33-af3f-04ae54ef53f9    telecom         Telecom 2019-12-25 20:01:18.768857 UTC  2019-12-25 20:01:18.768857 UTC  null                            +6vfAkoEdgA3VVJoIyORgQ==
