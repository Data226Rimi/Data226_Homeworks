
  create or replace   view USER_DB_CHEETAH.analytics.session_timestamp
  
   as (
    SELECT
    sessionId,
    ts
FROM USER_DB_CHEETAH.raw.session_timestamp
WHERE sessionId IS NOT NULL
  );

