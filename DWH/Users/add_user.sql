-- Процедура добавления пользователя
CREATE OR REPLACE PROCEDURE PP.ADD_USER (IN in_username STRING, IN in_password STRING, IN in_role STRING, OUT result STRING)
BEGIN
    DECLARE id STRING;
    DECLARE proceed TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
    DECLARE valid_from TIMESTAMP DEFAULT CURRENT_TIMESTAMP();

    SET id = (SELECT DISTINCT username FROM PP.U_USERS WHERE username = in_username);

    IF id IS NOT NULL
    THEN
        IF EXISTS (SELECT 1 FROM PP.U_USERS WHERE username = in_username AND valid_to_dttm IS NULL)
        THEN
            SET result = 'Found active user. Will not add';
            RETURN;
        ELSE
            SET result = 'Found inactive user. Make it active';
            UPDATE PP.U_USERS SET password = MD5(in_password) WHERE username = in_username;
            UPDATE PP.U_USERS SET role = role WHERE username = in_username;
            UPDATE PP.U_USERS SET processed_dttm = proceed WHERE username = in_username;
            UPDATE PP.U_USERS SET valid_to_dttm = CAST(NULL AS TIMESTAMP) WHERE username = in_username;
            RETURN;
        END IF;
    ELSE
        SET result = 'Add new user';
        INSERT INTO PP.U_USERS VALUES (in_username, MD5(in_password), in_role, proceed, valid_from, CAST(NULL AS TIMESTAMP));
        RETURN;
    END IF;

END;
