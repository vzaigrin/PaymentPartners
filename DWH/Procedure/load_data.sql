CREATE OR REPLACE PROCEDURE PP.LOAD_DATA (IN partner STRING, OUT dds INT64)
BEGIN

    -- Очищаем ODS_DATA
    DELETE FROM PP.ODS_DATA WHERE true;

    -- Добавляем id партнёров, bins, привелегий и городов
    -- Отбираем только новые записи
    INSERT INTO PP.ODS_DATA
    SELECT
        GENERATE_UUID() AS data_id
        , t.card_number
        , t.operation_ts
        , t.period_year
        , t.period_month
        , t.period_name
        , t.operation_country
        , t.operation_city
        , t.payment_total
        , t.payment_tariff
        , t.payment_main_client
        , t.payment_ps
        , t.payment_partner
        , t.payment_other_client
        , t.processed_dttm
        , p.partner_id
        , b.bin_id
        , l.privilege_id
        , c.city_id
        , t._hash
    FROM (
        SELECT
            o.*
            , MD5(CONCAT(bin, card_number, CAST(operation_ts AS STRING), CAST(period_year AS STRING), CAST(period_month AS STRING), period_name,
                operation_country, operation_city, CAST(payment_total AS STRING), CAST(payment_tariff AS STRING), CAST(payment_main_client AS STRING),
                CAST(payment_ps AS STRING), CAST(payment_partner AS STRING), CAST(payment_other_client AS STRING))) AS _hash
        FROM PP.TMP_DATA o
    ) t
    LEFT JOIN PP.SAT_PARTNERS p
    ON UPPER(p.partner_name) = partner
    INNER JOIN PP.SAT_BINS b
    ON b.bin = t.bin
    INNER JOIN PP.SAT_PRIVILEGES l
    ON l.privilege_type = t.privilege_type
    LEFT JOIN PP.SAT_CITY c
    ON t.operation_city = c.id
    LEFT JOIN PP.SAT_DATA s
    ON t._hash = s._hash
    WHERE s._hash IS NULL
        AND p.valid_to_dttm IS NULL
        AND b.valid_to_dttm IS NULL
        AND l.valid_to_dttm IS NULL
    ;

    SET dds = (SELECT count(*) FROM PP.ODS_DATA);

    -- Сохраняем данные в HUB_DATA
    INSERT INTO PP.HUB_DATA
    SELECT data_id, processed_dttm
    FROM PP.ODS_DATA
    ;

    -- Сохраняем данные в SAT_DATA
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
        , _hash
    FROM PP.ODS_DATA
    ;

    -- Сохраняем данные в линки
    INSERT INTO PP.LNK_DATA_PARTNERS
    SELECT data_id, partner_id, processed_dttm
    FROM PP.ODS_DATA
    WHERE partner_id IS NOT NULL
    ;

    INSERT INTO PP.LNK_DATA_BINS
    SELECT data_id, bin_id, processed_dttm
    FROM PP.ODS_DATA
    WHERE bin_id IS NOT NULL
    ;

    INSERT INTO PP.LNK_DATA_PRIVILEGES
    SELECT data_id, privilege_id, processed_dttm
    FROM PP.ODS_DATA
    WHERE privilege_id IS NOT NULL
    ;

    INSERT INTO PP.LNK_DATA_CITY
    SELECT data_id, city_id, processed_dttm
    FROM PP.ODS_DATA
    WHERE city_id IS NOT NULL
    ;

END;