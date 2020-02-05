-- Процедура заполнения таблиц в областях ODS и DDS после заполнения таблицы в области Stage
CREATE OR REPLACE PROCEDURE PP.LOAD_RETAIL (fname STRING, pyear INT64, pmonth INT64)
BEGIN

    DECLARE loadts TIMESTAMP;
    DECLARE stg INT64;
    DECLARE ods INT64;
    DECLARE dds INT64;

    SET loadts = CURRENT_TIMESTAMP;
    SET stg = (SELECT count(*) FROM PP.STG_RETAIL);

    -- Очищаем таблицу с данными в области ODS
    DELETE FROM PP.ODS_RETAIL WHERE true;

    -- Перегружаем данные из области STAGE в область ODS
    -- Добавляем имя файла и время обработки, удаляем некорректные данные
    INSERT INTO PP.ODS_RETAIL
    SELECT DISTINCT
        COALESCE(order_id, 0)
        , COALESCE(card_bin, '')
        , COALESCE(card_number, '')
        , COALESCE(bill_date, TIMESTAMP(DATE(pyear, pmonth, 1)))
        , COALESCE(transaction_amount, 0.0)
        , COALESCE(ps_financing, 0.0)
        , COALESCE(partner_financing, 0.0)
        , COALESCE(location, '')
        , pyear AS period_year
        , pmonth AS period_month
        , fname AS filename
        , loadts AS load_ts
    FROM PP.STG_RETAIL
    WHERE bill_date BETWEEN TIMESTAMP(DATE(pyear, pmonth, 1)) AND TIMESTAMP(DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH))
    ;

    SET ods = (SELECT count(*) FROM PP.ODS_RETAIL);

    -- Очищаем TMP_DATA
    DELETE FROM PP.TMP_DATA WHERE true;

    -- Приводим данные к единому формату области DDS
    -- Сохраняем всё во временную таблицу
    INSERT INTO PP.TMP_DATA
    SELECT
        card_bin AS bin
        , card_number AS card_number
        , bill_date AS operation_ts
        , pyear AS period_year
        , pmonth AS period_month
        , FORMAT("%4d-%02d", pyear, pmonth) AS period_name
        , 'Россия' AS operation_country
        , location AS operation_city
        , transaction_amount AS payment_total
        , transaction_amount AS payment_tariff
        , transaction_amount * (1 - (ps_financing + partner_financing) / 100.0) AS payment_main_client
        , transaction_amount * (ps_financing / 100.0) AS payment_ps
        , transaction_amount * (partner_financing / 100.0) AS payment_partner
        , 0.0 AS payment_other_client
        , load_ts AS processed_dttm
    FROM PP.ODS_RETAIL
    ;

    -- Сохраняем данные в HUB_DATA, SAT_DATA и в линки
    CALL PP.LOAD_DATA('retail', dds);

    -- Заносим данные о загрузке в отчёт загрузок
    INSERT INTO PP.DM_LOADS
    VALUES ('retail', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;