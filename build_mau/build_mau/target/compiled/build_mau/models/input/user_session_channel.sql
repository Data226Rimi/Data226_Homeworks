SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_CHEETAH.raw.user_session_channel
WHERE sessionId IS NOT NULL