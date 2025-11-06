SELECT s.symbol, COUNT(*) AS option_count
FROM (
    SELECT s.*
    FROM option_snapshots s
    JOIN (
        SELECT osiKey, MAX(timestamp) AS max_ts
        FROM option_snapshots
        WHERE osiKey NOT IN (
            SELECT osiKey FROM option_snapshots WHERE daysToExpiration = 0
        )
        GROUP BY osiKey
    ) latest
    ON s.osiKey = latest.osiKey AND s.timestamp = latest.max_ts
    WHERE s.daysToExpiration > 5
) s
GROUP BY s.symbol
ORDER BY option_count DESC;
