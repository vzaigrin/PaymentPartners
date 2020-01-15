-- Процедура заполнения таблиц в областях ODS и DDS данными о партнёрах после заполнения таблицы в области Stage
CREATE OR REPLACE PROCEDURE PP.LOAD_PARTNERS ()
BEGIN

    -- Очищаем таблицу с данными о партнёрах в области ODS
    DELETE FROM PP.ODS_PARTNERS WHERE true;

    -- Перегружаем данные о партнёрах из области Stage в область ODS
    INSERT INTO PP.ODS_PARTNERS
    SELECT DISTINCT * FROM PP.STG_PARTNERS;

    -- Очищаем TMP_PARTNERS
    DELETE FROM PP.TMP_PARTNERS WHERE true;

    -- Объединяем данные о партнёрах из области ODS с данными в области DDS во временную таблицу
    -- Добавляем записи о партнёрах, которых не было
    -- Помечаем устаревшими записи, которых уже нет
    INSERT INTO PP.TMP_PARTNERS
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
    ON o._hash = s._hash;

    -- Очищаем HUB_PARTNERS и заполняем его новыми данными
    DELETE FROM PP.HUB_PARTNERS WHERE true;

    INSERT INTO PP.HUB_PARTNERS
    SELECT partner_id, processed_dttm, valid_from_dttm, valid_to_dttm
    FROM PP.TMP_PARTNERS;

    -- Очищаем SAT_PARTNERS и заполняем его новыми данными
    DELETE FROM PP.SAT_PARTNERS WHERE true;

    INSERT INTO PP.SAT_PARTNERS
    SELECT *
    FROM PP.TMP_PARTNERS;

END;