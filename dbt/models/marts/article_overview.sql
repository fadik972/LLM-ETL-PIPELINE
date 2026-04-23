{{ config(materialized='table') }}

SELECT
    a.id,
    a.source,
    a.title,
    a.url,
    a.published_at,
    s.sentiment,
    s.score,
    s.summary,
    COUNT(e.id) as entity_count
FROM {{ source('pipeline', 'raw_articles') }} a
LEFT JOIN {{ source('pipeline', 'article_sentiment') }} s
    ON a.id = s.article_id
LEFT JOIN {{ source('pipeline', 'extracted_entities') }} e
    ON a.id = e.article_id
WHERE a.is_processed = TRUE
GROUP BY a.id, a.source, a.title, a.url, a.published_at,
         s.sentiment, s.score, s.summary
ORDER BY a.published_at DESC
