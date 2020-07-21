-- Процедура закрывает сессию для пользователя
CREATE OR REPLACE PROCEDURE PP.CLOSE_SESSION (IN sid STRING)
BEGIN
    UPDATE PP.U_SESSIONS SET valid_to_dttm = CURRENT_TIMESTAMP() WHERE id = sid;
    RETURN;
END;
