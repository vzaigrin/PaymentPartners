-- Процедура удаления партнёра. Делает партнёра неактивным.
CREATE OR REPLACE PROCEDURE PP.DELETE_PARTNER (IN partner STRING, OUT result STRING)
BEGIN
    DECLARE id STRING;
    DECLARE processed TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    SET id = (SELECT DISTINCT partner_id FROM PP.SAT_PARTNERS WHERE partner_name = partner);

    IF id IS NOT NULL
    THEN
        IF EXISTS (SELECT 1 FROM PP.SAT_PARTNERS WHERE partner_name = partner AND valid_to_dttm IS NULL)
        THEN
            SET result = 'Found active partner. Make it inactive';
            UPDATE PP.HUB_PARTNERS SET valid_to_dttm = processed WHERE partner_id = id;
            UPDATE PP.SAT_PARTNERS SET valid_to_dttm = processed WHERE partner_name = partner AND valid_to_dttm IS NULL;
            RETURN;
        ELSE
            SET result = 'Found inactive partner. Nothing to do';
            RETURN;
        END IF;
    ELSE
        SET result = 'Partner not found';
        RETURN;
    END IF;

END;
