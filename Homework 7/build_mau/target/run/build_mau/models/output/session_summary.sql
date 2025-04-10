
  
    

        create or replace transient table USER_DB_GATOR.analytics.session_summary
         as
        (WITH  __dbt__cte__user_session_channel as (
SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_GATOR.raw.user_session_channel
WHERE sessionId IS NOT NULL
),  __dbt__cte__session_timestamp as (
SELECT
    sessionId,
    ts
FROM USER_DB_GATOR.raw.session_timestamp
WHERE sessionId IS NOT NULL
), u AS (
    SELECT * FROM __dbt__cte__user_session_channel
), st AS (
    SELECT * FROM __dbt__cte__session_timestamp
)
SELECT u.userId, u.sessionId, u.channel, st.ts
FROM u
JOIN st ON u.sessionId = st.sessionId
        );
      
  