-- Процедура проверки пользователя
CREATE OR REPLACE PROCEDURE PP.VALIDATE_SESSION (IN sid STRING, OUT result STRING)
BEGIN
    DECLARE id STRING;

    SET id = (SELECT DISTINCT id FROM PP.U_SESSIONS WHERE id = sid);

    IF id IS NOT NULL
    THEN
        IF EXISTS (SELECT 1 FROM PP.U_SESSIONS WHERE id = sid AND valid_to_dttm > CURRENT_TIMESTAMP())
        THEN
            SET result = 'Valid';
            RETURN;
        ELSE
            SET result = 'Invalid';
            RETURN;
        END IF;
    ELSE
        SET result = 'None';
        RETURN;
    END IF;

END;
