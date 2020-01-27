-- Процедура заполнения таблиц в областях ODS и DDS после заполнения таблицы в области Stage
CREATE OR REPLACE PROCEDURE PP.LOAD_RETAIL (fname STRING, pyear INT64, pmonth INT64)
BEGIN

    DECLARE loadts TIMESTAMP;
    DECLARE stg INT64;
    DECLARE ods INT64;
    DECLARE dds INT64;

    SET loadts = CURRENT_TIMESTAMP;

    -- Очищаем таблицу с данными в области ODS
    DELETE FROM PP.ODS_RETAIL WHERE true;

    -- Перегружаем данные из области Srange_frome в область ODS
    -- Добавляем имя файла и время обработки, удаляем некорректные данные
    INSERT INTO PP.ODS_RETAIL
    SELECT DISTINCT
        CAST(order_id AS STRING) AS order_id
        , CAST(card_bin AS STRING) AS card_bin
        , CAST(card_number AS STRING) AS card_number
        , bill_date
        , transaction_amount
        , ps_financing
        , partner_financing
        , location
        , pyear AS period_year
        , pmonth AS period_month
        , fname AS filename
        , loadts AS load_ts
    FROM PP.STG_RETAIL
    WHERE bill_date BETWEEN TIMESTAMP(DATE(pyear, pmonth, 1)) AND TIMESTAMP(DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH))
    ;

    -- Очищаем TMP_DATA
    DELETE FROM PP.TMP_DATA WHERE true;

    -- Приводим данные к единому формату области DDS
    -- Добавляем id партнёров, bins и привелегий
    -- Сохраняем всё во временную таблицу
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
            , 0 AS payment_other_client
            , load_ts AS processed_dttm
        FROM PP.ODS_RETAIL
    ) t
    LEFT JOIN PP.SAT_PARTNERS p
    ON p.partner_name = 'retail'
    INNER JOIN PP.SAT_BINS b
    ON b.bin = t.bin
    INNER JOIN PP.SAT_PRIVILEGES l
    ON l.privilege_type = 'discount'
    WHERE p.valid_to_dttm IS NULL
        AND b.valid_to_dttm IS NULL
        AND l.valid_to_dttm IS NULL
    ;

    -- Сохраняем данные в HUB_DATA, SAT_DATA и в линки
    CALL PP.LOAD_DATA();

    -- Заносим данные о загрузке в отчёт загрузок
    SET stg = (SELECT count(*) FROM PP.STG_RETAIL);
    SET ods = (SELECT count(*) FROM PP.ODS_RETAIL);
    SET dds = (SELECT count(*) FROM PP.TMP_DATA_2);

    INSERT INTO PP.DM_LOADS
    VALUES ('retail', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;