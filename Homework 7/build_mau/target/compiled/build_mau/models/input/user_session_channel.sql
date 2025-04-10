SELECT
    userId,
    sessionId,
    channel
FROM USER_DB_GATOR.raw.user_session_channel
WHERE sessionId IS NOT NULL