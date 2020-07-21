-- Процедура проверки пользователя
CREATE OR REPLACE PROCEDURE PP.VALIDATE_USER (IN in_username STRING, IN in_password STRING, OUT result STRING)
BEGIN
    DECLARE id STRING;

    SET id = (SELECT DISTINCT username FROM PP.U_USERS WHERE username = in_username AND password = MD5(in_password) AND valid_to_dttm IS NULL);

    IF id IS NOT NULL
    THEN
        SET result = 'Valid';
        RETURN;
    ELSE
        SET result = 'Invalid';
        RETURN;
    END IF;

END;
