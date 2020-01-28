-- Процедура заполнения таблиц в областях ODS и DDS после заполнения таблицы в области Stage
CREATE OR REPLACE PROCEDURE PP.LOAD_CINEMA (fname STRING, pyear INT64, pmonth INT64)
BEGIN

    DECLARE loadts TIMESTAMP;
    DECLARE stg INT64;
    DECLARE ods INT64;
    DECLARE dds INT64;

    SET loadts = CURRENT_TIMESTAMP;
    SET stg = (SELECT count(*) FROM PP.STG_CINEMA);

    -- Очищаем таблицу с данными в области ODS
    DELETE FROM PP.ODS_CINEMA WHERE true;

    -- Перегружаем данные из области STAGE в область ODS
    -- Добавляем имя файла и время обработки, удаляем некорректные данные
    INSERT INTO PP.ODS_CINEMA
    SELECT DISTINCT
        COALESCE(cinema_name, '')
        , COALESCE(trans_time, TIMESTAMP(DATE(pyear, pmonth, 1)))
        , COALESCE(discount_type, '')
        , COALESCE(base_price, 0.0)
        , COALESCE(discount, 0.0)
        , COALESCE(film, '')
        , CAST(rrn AS STRING) AS rrn
        , CAST(card_number AS STRING) AS card_number
        , pyear AS period_year
        , pmonth AS period_month
        , fname AS filename
        , loadts AS load_ts
    FROM PP.STG_CINEMA
    WHERE trans_time BETWEEN TIMESTAMP(DATE(pyear, pmonth, 1)) AND TIMESTAMP(DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH))
    ;

    SET ods = (SELECT count(*) FROM PP.ODS_CINEMA);

    -- Очищаем TMP_DATA
    DELETE FROM PP.TMP_DATA WHERE true;

    -- Приводим данные к единому формату области DDS
    -- Сохраняем всё во временную таблицу
    INSERT INTO PP.TMP_DATA
    SELECT
        rrn AS bin
        , card_number AS card_number
        , trans_time AS operation_ts
        , pyear AS period_year
        , pmonth AS period_month
        , FORMAT("%4d-%02d", pyear, pmonth) AS period_name
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
    ;

    -- Сохраняем данные в HUB_DATA, SAT_DATA и в линки
    CALL PP.LOAD_DATA('cinema', dds);

    -- Заносим данные о загрузке в отчёт загрузок
    INSERT INTO PP.DM_LOADS
    VALUES ('cinema', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;