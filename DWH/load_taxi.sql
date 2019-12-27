-- Процедура заполнения таблиц в областях ODS и DDS после заполнения таблицы в области Stage
CREATE OR REPLACE PROCEDURE PP.LOAD_TAXI (fname STRING, pyear INT64, pmonth INT64)
BEGIN

    DECLARE loadts TIMESTAMP;
    DECLARE total INT64;
    DECLARE good INT64;

    SET loadts = CURRENT_TIMESTAMP;

    -- Очищаем таблицу с данными в области ODS
    DELETE FROM PP.ODS_TAXI WHERE true;

    -- Перегружаем данные из области Srange_frome в область ODS
    -- Добавляем имя файла и время обработки, удаляем некорректные данные
    INSERT INTO PP.ODS_TAXI
    SELECT DISTINCT
        datetime
        , ride_town
        , CAST(bin_number AS STRING) AS bin_number
        , CAST(last4 AS STRING) AS last4
        , class
        , tariff
        , ps_financing
        , taxi_financing
        , pyear AS period_year
        , pmonth AS period_month
        , fname AS filename
        , loadts AS load_ts
    FROM PP.STG_TAXI
    WHERE datetime BETWEEN TIMESTAMP(DATE(pyear, pmonth, 1)) AND TIMESTAMP(DATE_ADD(DATE(pyear, pmonth, 1), INTERVAL 1 MONTH))
    ;

    -- Заносим данные о загрузке в отчёт загрузок
    SET total = (SELECT count(*) FROM PP.STG_TAXI);
    SET good = (SELECT count(*) FROM PP.ODS_TAXI);

    INSERT INTO PP.DM_LOADS (partner_name, period_name, period_year, period_month, filename, load_ts, total, good, bad)
    VALUES ('taxi', FORMAT("%4d-%02d", pyear, pmonth), pyear, pmonth, fname, loadts, total, good, total - good);

    -- Приводим данные к единому формату области DDS
    -- Добавляем id партнёров, bins и привелегий
    -- Сохраняем всё во временную таблицу
    CREATE OR REPLACE TABLE PP.temp AS
    SELECT
        GENERATE_UUID() AS data_id
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
        , 0 AS payment_other_client
        , load_ts AS processed_dttm
        , p.partner_id
        , b.bin_id
        , l.privilege_id
    FROM PP.ODS_TAXI t
    LEFT JOIN PP.SAT_PARTNERS p
    ON p.partner_name = 'taxi'
    INNER JOIN PP.SAT_BINS b
    ON b.bin = t.bin_number
    INNER JOIN PP.SAT_PRIVILEGES l
    ON l.privilege_type = 'discount'
    WHERE p.valid_to_dttm IS NULL
        AND b.valid_to_dttm IS NULL
        AND l.valid_to_dttm IS NULL
    ;

    -- Сохраняем данные в HUB_DATA, SAT_DATA и в линки
    INSERT INTO PP.HUB_DATA
    SELECT data_id, processed_dttm
    FROM PP.temp;

    INSERT INTO PP.SAT_DATA
    SELECT
        data_id
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
        , processed_dttm
    FROM PP.temp;

    INSERT INTO PP.LNK_DATA_PARTNERS
    SELECT data_id, partner_id, processed_dttm
    FROM PP.temp;

    INSERT INTO PP.LNK_DATA_BINS
    SELECT data_id, bin_id, processed_dttm
    FROM PP.temp;

    INSERT INTO PP.LNK_DATA_PRIVILEGES
    SELECT data_id, privilege_id, processed_dttm
    FROM PP.temp;

END;
