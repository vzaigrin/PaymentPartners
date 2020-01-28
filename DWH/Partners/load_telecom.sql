-- Процедура заполнения таблиц в областях ODS и DDS после заполнения таблицы в области Stage
CREATE OR REPLACE PROCEDURE PP.LOAD_TELECOM (fname STRING, pyear INT64, pmonth INT64)
BEGIN

    DECLARE loadts TIMESTAMP;
    DECLARE stg INT64;
    DECLARE ods INT64;
    DECLARE dds INT64;

    SET loadts = CURRENT_TIMESTAMP;
    SET stg = (SELECT count(*) FROM PP.STG_TELECOM);

    -- Очищаем таблицу с данными в области ODS
    DELETE FROM PP.ODS_TELECOM WHERE true;

    -- Перегружаем данные из области STAGE в область ODS
    -- Добавляем имя файла и время обработки, удаляем некорректные данные
    INSERT INTO PP.ODS_TELECOM
    SELECT DISTINCT
        COALESCE(operation_ts, TIMESTAMP(DATE(pyear, pmonth, 1)))
        , COALESCE(operation_country, '')
        , COALESCE(operation_city, '')
        , COALESCE(card_bin, '')
        , COALESCE(card_number, '')
        , COALESCE(service, '')
        , COALESCE(payment_tariff, 0.0)
        , COALESCE(payment_ps, 0.0)
        , pyear AS period_year
        , pmonth AS period_month
        , fname AS filename
        , loadts AS load_ts
    FROM PP.STG_TELECOM
    WHERE operation_ts BETWEEN TIMESTAMP(DATE(pyear, pmonth, 1)) AND TIMESTAMP(DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH))
    ;

    SET ods = (SELECT count(*) FROM PP.ODS_TELECOM);

    -- Очищаем TMP_DATA
    DELETE FROM PP.TMP_DATA WHERE true;

    -- Приводим данные к единому формату области DDS
    -- Сохраняем всё во временную таблицу
    INSERT INTO PP.TMP_DATA
    SELECT
        card_bin AS bin
        , card_number AS card_number
        , operation_ts AS operation_ts
        , pyear AS period_year
        , pmonth AS period_month
        , FORMAT("%4d-%02d", pyear, pmonth) AS period_name
        , COALESCE(operation_country, '') AS operation_country
        , COALESCE(operation_city, '') AS operation_city
        , payment_tariff AS payment_total
        , payment_tariff AS payment_tariff
        , 0 AS payment_main_client
        , payment_tariff * (payment_ps / 100.0) AS payment_ps
        , payment_tariff * (1 - (payment_ps / 100.0)) AS payment_partner
        , 0 AS payment_other_client
        , load_ts AS processed_dttm
    FROM PP.ODS_TELECOM
    ;

    -- Сохраняем данные в HUB_DATA, SAT_DATA и в линки
    CALL PP.LOAD_DATA('telecom', dds);

    -- Заносим данные о загрузке в отчёт загрузок
    INSERT INTO PP.DM_LOADS
    VALUES ('telecom', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;