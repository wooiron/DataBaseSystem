SELECT P.NAME
FROM POKEMON AS P, EVOLUTION AS E
WHERE E.BEFORE_ID > E.AFTER_ID
AND P.ID = E.BEFORE_ID;