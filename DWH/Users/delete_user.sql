-- Процедура удаления пользователя. Делает пользователя неактивным.
CREATE OR REPLACE PROCEDURE PP.DELETE_USER (IN in_username STRING, OUT result STRING)
BEGIN
    DECLARE id STRING;
    DECLARE proceed TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    SET id = (SELECT DISTINCT username FROM PP.U_USERS WHERE username = in_username);

    IF id IS NOT NULL
    THEN
        IF EXISTS (SELECT 1 FROM PP.U_USERS WHERE username = in_username AND valid_to_dttm IS NULL)
        THEN
            SET result = 'Found active user. Make it inactive';
            UPDATE PP.U_USERS SET processed_dttm = proceed WHERE username = in_username;
            UPDATE PP.U_USERS SET valid_to_dttm = proceed WHERE username = in_username;
            RETURN;
        ELSE
            SET result = 'Found inactive user. Nothing to do';
            RETURN;
        END IF;
    ELSE
        SET result = 'User not found';
        RETURN;
    END IF;

END;
