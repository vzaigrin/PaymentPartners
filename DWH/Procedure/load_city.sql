-- Процедура заполнения таблиц в областях ODS и DDS данными о привилегиях после заполнения таблицы в области Scitye
CREATE OR REPLACE PROCEDURE PP.LOAD_CITY ()
BEGIN

    -- Очищаем таблицу с данными о привилегиях в области ODS
    DELETE FROM PP.ODS_CITY WHERE true;

    -- Перегружаем данные о привилегиях из области STAGE в область ODS
    INSERT INTO PP.ODS_CITY
    SELECT DISTINCT * FROM PP.STG_CITY;

    -- Очищаем TMP_CITY
    DELETE FROM PP.TMP_CITY WHERE true;

    -- Объединяем данные о привилегиях из области ODS с данными в области DDS во временную таблицу
    -- Добавляем записи о привилегиях, которых не было
    -- Помечаем устаревшими записи, которых уже нет
    INSERT INTO PP.TMP_CITY
    SELECT
    COALESCE(s.city_id, o.city_id) AS city_id
    , COALESCE(s.id, o.id) AS id
    , COALESCE(s.city, o.city) AS city
    , COALESCE(s.country, o.country) AS country
    , COALESCE(s.processed_dttm, o.processed_dttm) AS processed_dttm
    , COALESCE(s.valid_from_dttm, o.processed_dttm) AS valid_from_dttm
    , CASE
            WHEN o.city_id IS NULL
            THEN CURRENT_TIMESTAMP
            ELSE CAST(NULL AS TIMESTAMP)
        END AS valid_to_dttm
    , COALESCE(s._hash, o._hash) AS _hash
    FROM (
    SELECT
        GENERATE_UUID() AS city_id
        , id
        , city
        , country
        , CURRENT_TIMESTAMP AS processed_dttm
        , MD5(CONCAT(id)) AS _hash
    FROM PP.ODS_CITY
    ) o
    FULL JOIN PP.SAT_CITY s
    ON o._hash = s._hash;

    -- Очищаем HUB_CITY и заполняем его новыми данными
    DELETE FROM PP.HUB_CITY WHERE true;

    INSERT INTO PP.HUB_CITY
    SELECT city_id, processed_dttm, valid_from_dttm, valid_to_dttm
    FROM PP.TMP_CITY;

    -- Очищаем SAT_CITY и заполняем его новыми данными
    DELETE FROM PP.SAT_CITY WHERE true;

    INSERT INTO PP.SAT_CITY
    SELECT *
    FROM PP.TMP_CITY;

END;