{{ config(materialized='table') }}

SELECT
    entity_type,
    entity_value,
    COUNT(*) as mention_count,
    MAX(e.extracted_at) as last_seen
FROM {{ source('pipeline', 'extracted_entities') }} e
WHERE
    entity_value IS NOT NULL
    AND entity_value != ''
    AND LOWER(entity_value) != 'none'
    AND LOWER(entity_value) != 'n/a'
    AND LENGTH(entity_value) > 1
GROUP BY entity_type, entity_value
ORDER BY mention_count DESC
