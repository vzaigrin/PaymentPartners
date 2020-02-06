-- Процедура добавления партнёра
CREATE OR REPLACE PROCEDURE PP.ADD_PARTNER (IN partner STRING, IN tag STRING, OUT result STRING)
BEGIN
    DECLARE id STRING;
    DECLARE processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    SET id = (SELECT DISTINCT partner_id FROM PP.SAT_PARTNERS WHERE partner_name = partner);

    IF id IS NOT NULL
    THEN
        IF EXISTS (SELECT 1 FROM PP.SAT_PARTNERS WHERE partner_name = partner AND valid_to_dttm IS NULL)
        THEN
            SET result = 'Found active partner. Will not add';
            RETURN;
        ELSE
            SET result = 'Found inactive partner. Make it active';
            UPDATE PP.HUB_PARTNERS SET valid_to_dttm = CAST(NULL AS TIMESTAMP) WHERE partner_id = id;
            INSERT INTO PP.SAT_PARTNERS VALUES (id, partner, tag, processed, valid_from, CAST(NULL AS TIMESTAMP), MD5(CONCAT(partner, tag))); -- hash потом уберем
            RETURN;
        END IF;
    ELSE
        SET result = 'Add new partner';
        SET id = GENERATE_UUID();
        INSERT INTO PP.HUB_PARTNERS VALUES (id, processed, valid_from, CAST(NULL AS TIMESTAMP));
        INSERT INTO PP.SAT_PARTNERS VALUES (id, partner, tag, processed, valid_from, CAST(NULL AS TIMESTAMP), MD5(CONCAT(partner, tag))); -- hash потом уберем
        RETURN;
    END IF;

END;
