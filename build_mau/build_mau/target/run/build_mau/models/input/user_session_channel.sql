
  create or replace   view USER_DB_CHEETAH.analytics.user_session_channel
  
   as (
    SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_CHEETAH.raw.user_session_channel
WHERE sessionId IS NOT NULL
  );

