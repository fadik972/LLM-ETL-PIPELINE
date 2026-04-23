{{ config(materialized='table') }}

SELECT
    a.source,
    s.sentiment,
    COUNT(*) as article_count,
    ROUND(AVG(s.score)::numeric, 2) as avg_score,
    DATE_TRUNC('day', a.published_at) as published_day
FROM {{ source('pipeline', 'article_sentiment') }} s
JOIN {{ source('pipeline', 'raw_articles') }} a
    ON s.article_id = a.id
WHERE a.published_at IS NOT NULL
GROUP BY a.source, s.sentiment, DATE_TRUNC('day', a.published_at)
ORDER BY published_day DESC
