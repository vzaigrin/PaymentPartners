-- Процедура заполнения таблиц в областях ODS и DDS данными о bins после заполнения таблицы в области Srange_frome
CREATE OR REPLACE PROCEDURE PP.LOAD_BINS ()
BEGIN

    -- Очищаем таблицу с данными о bins в области ODS
    DELETE FROM PP.ODS_BINS WHERE true;

    -- Объединяем данные о bins из области Stage с данными в области DDS в область ODS
    -- Добавляем записи о bins, которых не было
    -- Помечаем устаревшими записи, которых уже нет
    INSERT INTO PP.ODS_BINS
    SELECT
    COALESCE(s.bin_id, o.bin_id) AS bin_id
    , COALESCE(s.bin, o.bin) AS bin
    , COALESCE(s.bank, o.bank) AS bank
    , COALESCE(s.card_type, o.card_type) AS card_type
    , COALESCE(s.processed_dttm, o.processed_dttm) AS processed_dttm
    , COALESCE(s.valid_from_dttm, o.processed_dttm) AS valid_from_dttm
    , CASE
        WHEN o.bin_id IS NULL
            THEN CURRENT_TIMESTAMP
            ELSE CAST(NULL AS TIMESTAMP)
      END AS valid_to_dttm
    , COALESCE(s._hash, o._hash) AS _hash
    FROM (
        SELECT
            GENERATE_UUID() AS bin_id
            , bin
            , bank
            , card_type
            , CURRENT_TIMESTAMP AS processed_dttm
            , MD5(CONCAT(bin, range_from, range_to)) AS _hash
        FROM (
            SELECT
                bin
                , range_from
                , range_to
                , bank
                , card_type
                , ROW_NUMBER() OVER (PARTITION BY bin ORDER BY bin, range_from, range_to DESC) AS row_num
            FROM PP.STG_BINS
        ) b
        WHERE b.row_num = 1
    ) o
    FULL JOIN PP.SAT_BINS s
    ON o._hash = s._hash;

    -- Очищаем HUB_BINS и заполняем его новыми данными
    DELETE FROM PP.HUB_BINS WHERE true;

    INSERT INTO PP.HUB_BINS
    SELECT bin_id, processed_dttm, valid_from_dttm, valid_to_dttm
    FROM PP.ODS_BINS;

    -- Очищаем SAT_BINS и заполняем его новыми данными
    DELETE FROM PP.SAT_BINS WHERE true;

    INSERT INTO PP.SAT_BINS
    SELECT *
    FROM PP.ODS_BINS;

END;