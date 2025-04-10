
  create or replace   view USER_DB_GATOR.analytics.session_timestamp
  
   as (
    SELECT
    sessionId,
    ts
FROM USER_DB_GATOR.raw.session_timestamp
WHERE sessionId IS NOT NULL
  );

