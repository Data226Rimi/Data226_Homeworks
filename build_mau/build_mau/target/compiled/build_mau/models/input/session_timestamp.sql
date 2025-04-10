SELECT
    sessionId,
    ts
FROM USER_DB_CHEETAH.raw.session_timestamp
WHERE sessionId IS NOT NULL