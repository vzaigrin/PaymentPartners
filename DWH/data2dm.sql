-- Процедура заполнения таблицы DATA MART по данным DDS за конкретный период
CREATE OR REPLACE PROCEDURE PP.DATA2DM (pyear INT64, pmonth INT64)
BEGIN

    INSERT INTO PP.DM_REPORT
    SELECT
        partner_name
        , card_type
        , bank
        , operation_dt
        , EXTRACT(DAY FROM operation_dt) AS operation_day
        , operation_country
        , operation_city
        , privilege_type
        , partner_class
        , period_name
        , period_year
        , period_month
        , EXTRACT(WEEK FROM operation_dt) AS week_num
        , ROUND(SUM(COALESCE(payment_total,0))) AS payment_total
        , ROUND(SUM(COALESCE(payment_tariff,0))) AS payment_tariff
        , ROUND(SUM(COALESCE(payment_main_client,0))) AS payment_main_client
        , ROUND(SUM(COALESCE(payment_ps,0))) AS payment_ps
        , ROUND(SUM(COALESCE(payment_partner,0))) AS payment_partner
        , ROUND(SUM(COALESCE(payment_other_client,0))) AS payment_other_client
        , COUNT(*) AS trans_num
    FROM (
        SELECT
            p.partner_name
            , b.card_type
            , b.bank
            , CAST(d.operation_ts AS DATE) AS operation_dt
            , d.operation_country
            , d.operation_city
            , r.privilege_type
            , p.tag as partner_class
            , d.period_name
            , d.period_year
            , d.period_month
            , payment_total
            , payment_tariff
            , payment_main_client
            , payment_ps
            , payment_partner
            , payment_other_client
        FROM PP.SAT_DATA d
        JOIN PP.LNK_DATA_PARTNERS ldp
        ON d.data_id = ldp.data_id
        JOIN PP.SAT_PARTNERS p
        ON ldp.partner_id = p.partner_id
        JOIN PP.LNK_DATA_BINS ldb
        ON d.data_id = ldb.data_id
        JOIN PP.SAT_BINS b
        ON ldb.bin_id = b.bin_id
        JOIN PP.LNK_DATA_PRIVILEGES ldr
        ON d.data_id = ldr.data_id
        JOIN PP.SAT_PRIVILEGES r
        ON ldr.privilege_id = r.privilege_id
        WHERE period_year = pyear AND period_month = pmonth
    )
    GROUP BY
        partner_name
        , card_type
        , bank
        , operation_dt
        , operation_country
        , operation_city
        , privilege_type
        , partner_class
        , period_name
        , period_year
        , period_month
    ;
    
END;