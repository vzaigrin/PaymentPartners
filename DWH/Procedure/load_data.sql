CREATE OR REPLACE PROCEDURE PP.LOAD_DATA (OUT dds INT64)
BEGIN

    -- Отбираем только новые записи
    DELETE FROM PP.TMP_DATA_2 WHERE true;
    INSERT INTO PP.TMP_DATA_2
    SELECT t.*
    FROM PP.TMP_DATA t
    LEFT JOIN PP.SAT_DATA s
    ON t._hash = s._hash
    WHERE s._hash IS NULL
    ;

    SET dds = (SELECT count(*) FROM PP.TMP_DATA_2);

    -- Сохраняем данные в HUB_DATA
    INSERT INTO PP.HUB_DATA
    SELECT data_id, processed_dttm
    FROM PP.TMP_DATA_2
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
    FROM PP.TMP_DATA_2
    ;

    -- Сохраняем данные в линки
    INSERT INTO PP.LNK_DATA_PARTNERS
    SELECT data_id, partner_id, processed_dttm
    FROM PP.TMP_DATA_2
    ;

    INSERT INTO PP.LNK_DATA_BINS
    SELECT data_id, bin_id, processed_dttm
    FROM PP.TMP_DATA_2
    ;

    INSERT INTO PP.LNK_DATA_PRIVILEGES
    SELECT data_id, privilege_id, processed_dttm
    FROM PP.TMP_DATA_2
    ;

END;