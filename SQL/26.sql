SELECT P.NAME
FROM CATCHEDPOKEMON AS CP,
POKEMON AS P
WHERE CP.PID = P.ID
AND CP.NICKNAME LIKE '% %'
ORDER BY P.NAME DESC;