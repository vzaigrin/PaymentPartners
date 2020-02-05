-- Процедура заполнения таблиц в областях ODS и DDS после заполнения таблицы в области Stage
CREATE OR REPLACE PROCEDURE PP.LOAD_TAXI (fname STRING, pyear INT64, pmonth INT64)
BEGIN

    DECLARE loadts TIMESTAMP;
    DECLARE stg INT64;
    DECLARE ods INT64;
    DECLARE dds INT64;

    SET loadts = CURRENT_TIMESTAMP;
    SET stg = (SELECT count(*) FROM PP.STG_TAXI);

    -- Очищаем таблицу с данными в области ODS
    DELETE FROM PP.ODS_TAXI WHERE true;

    -- Перегружаем данные из области STAGE в область ODS
    -- Добавляем имя файла и время обработки, удаляем некорректные данные
    INSERT INTO PP.ODS_TAXI
    SELECT DISTINCT
        COALESCE(datetime, TIMESTAMP(DATE(pyear, pmonth, 1)))
        , COALESCE(ride_town, '')
        , COALESCE(bin_number, '')
        , COALESCE(last4, '')
        , COALESCE(class, '')
        , COALESCE(tariff, 0.0)
        , COALESCE(ps_financing, 0.0)
        , COALESCE(taxi_financing, 0.0)
        , pyear AS period_year
        , pmonth AS period_month
        , fname AS filename
        , loadts AS load_ts
    FROM PP.STG_TAXI
    WHERE datetime BETWEEN TIMESTAMP(DATE(pyear, pmonth, 1)) AND TIMESTAMP(DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH))
    ;

    SET ods = (SELECT count(*) FROM PP.ODS_TAXI);

    -- Очищаем TMP_DATA
    DELETE FROM PP.TMP_DATA WHERE true;

    -- Приводим данные к единому формату области DDS
    -- Сохраняем всё во временную таблицу
    INSERT INTO PP.TMP_DATA
    SELECT
        bin_number AS bin
        , last4 AS card_number
        , datetime AS operation_ts
        , pyear AS period_year
        , pmonth AS period_month
        , FORMAT("%4d-%02d", pyear, pmonth) AS period_name
        , 'Россия' AS operation_country
        , ride_town AS operation_city
        , tariff AS payment_total
        , tariff AS payment_tariff
        , tariff * (1 - (ps_financing + taxi_financing) / 100.0) AS payment_main_client
        , tariff * (ps_financing / 100.0) AS payment_ps
        , tariff * (taxi_financing / 100.0) AS payment_partner
        , 0.0 AS payment_other_client
        , load_ts AS processed_dttm
    FROM PP.ODS_TAXI
    ;

    -- Сохраняем данные в HUB_DATA, SAT_DATA и в линки
    CALL PP.LOAD_DATA('taxi', dds);

    -- Заносим данные о загрузке в отчёт загрузок
    INSERT INTO PP.DM_LOADS
    VALUES ('taxi', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;