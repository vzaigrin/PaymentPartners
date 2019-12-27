-- Процедура заполнения таблиц в областях ODS и DDS данными о привилегиях после заполнения таблицы в области Sprivilege_shorte
CREATE OR REPLACE PROCEDURE PP.LOAD_PRIVILEGES ()
BEGIN

    -- Очищаем таблицу с данными о привилегиях в области ODS
    DELETE FROM PP.ODS_PRIVILEGES WHERE true;

    -- Перегружаем данные о привилегиях из области Sprivilege_shorte в область ODS
    INSERT INTO PP.ODS_PRIVILEGES
    SELECT DISTINCT * FROM PP.STG_PRIVILEGES;

    -- Объединяем данные о привилегиях из области ODS с данными в области DDS во временную таблицу
    -- Добавляем записи о привилегиях, которых не было
    -- Помечаем устаревшими записи, которых уже нет
    CREATE OR REPLACE TABLE PP.temp AS
    SELECT
    COALESCE(s.privilege_id, o.privilege_id) AS privilege_id
    , COALESCE(s.privilege_type, o.privilege_type) AS privilege_type
    , COALESCE(s.privilege_short, o.privilege_short) AS privilege_short
    , COALESCE(s.privilege_full, o.privilege_full) AS privilege_full
    , COALESCE(s.processed_dttm, o.processed_dttm) AS processed_dttm
    , COALESCE(s.valid_from_dttm, o.processed_dttm) AS valid_from_dttm
    , CASE
            WHEN o.privilege_id IS NULL
            THEN CURRENT_TIMESTAMP
            ELSE CAST(NULL AS TIMESTAMP)
        END AS valid_to_dttm
    , COALESCE(s._hash, o._hash) AS _hash
    FROM (
    SELECT
        GENERATE_UUID() AS privilege_id
        , privilege_type
        , privilege_short
        , privilege_full
        , CURRENT_TIMESTAMP AS processed_dttm
        , MD5(CONCAT(privilege_type)) AS _hash
    FROM PP.ODS_PRIVILEGES
    ) o
    FULL JOIN PP.SAT_PRIVILEGES s
    ON o._hash = s._hash;

    -- Очищаем HUB_PRIVILEGES и заполняем его новыми данными
    DELETE FROM PP.HUB_PRIVILEGES WHERE true;

    INSERT INTO PP.HUB_PRIVILEGES
    SELECT privilege_id, processed_dttm, valid_from_dttm, valid_to_dttm
    FROM PP.temp;

    -- Очищаем SAT_PRIVILEGES и заполняем его новыми данными
    DELETE FROM PP.SAT_PRIVILEGES WHERE true;

    INSERT INTO PP.SAT_PRIVILEGES
    SELECT *
    FROM PP.temp;

END;