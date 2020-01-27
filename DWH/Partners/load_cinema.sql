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

    -- Перегружаем данные из области Srange_frome в область ODS
    -- Добавляем имя файла и время обработки, удаляем некорректные данные
    INSERT INTO PP.ODS_CINEMA
    SELECT DISTINCT
        cinema_name
        , trans_time
        , discount_type
        , COALESCE(base_price, 0.0)
        , COALESCE(discount, 0.0)
        , film
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
    ) t
    LEFT JOIN PP.SAT_PARTNERS p
    ON p.partner_name = 'cinema'
    INNER JOIN PP.SAT_BINS b
    ON b.bin = t.bin
    INNER JOIN PP.SAT_PRIVILEGES l
    ON l.privilege_type = 'discount'
    WHERE p.valid_to_dttm IS NULL
        AND b.valid_to_dttm IS NULL
        AND l.valid_to_dttm IS NULL
    ;

    -- Сохраняем данные в HUB_DATA, SAT_DATA и в линки
    CALL PP.LOAD_DATA(dds);

    -- Заносим данные о загрузке в отчёт загрузок
    INSERT INTO PP.DM_LOADS
    VALUES ('cinema', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, stg, ods, dds, stg - dds);

END;