SELECT COUNT(DISTINCT CP.PID)
FROM TRAINER AS T,
CATCHEDPOKEMON AS CP,
POKEMON AS P
WHERE T.HOMETOWN = 'Sangnok City'
AND T.ID = CP.OWNER_ID
AND CP.PID = P.ID;