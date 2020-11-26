SELECT T.NAME
FROM TRAINER AS T JOIN 
CATCHEDPOKEMON AS CP
ON (T.ID = CP.OWNER_ID)
JOIN POKEMON AS P
ON (CP.PID = P.ID)
GROUP BY T.NAME,P.NAME
HAVING COUNT(P.NAME) >=2
ORDER BY T.NAME;