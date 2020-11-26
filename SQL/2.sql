SELECT NAME
FROM POKEMON 
WHERE
TYPE IN (SELECT P.TYPE
FROM POKEMON AS P
GROUP BY P.TYPE
HAVING COUNT(*) >= (SELECT COUNT(*)
FROM POKEMON AS P
GROUP BY P.TYPE
ORDER BY COUNT(*) DESC LIMIT 2,1))
ORDER BY NAME;