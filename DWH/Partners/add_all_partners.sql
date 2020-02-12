DECLARE result STRING;
CALL PP.ADD_PARTNER ('cinema', 'Cinema', result);
SELECT result;
CALL PP.ADD_PARTNER ('retail', 'Retail', result);
SELECT result;
CALL PP.ADD_PARTNER ('taxi', 'Taxi', result);
SELECT result;
CALL PP.ADD_PARTNER ('telecom', 'Telecom', result);
SELECT result;
