WITH interaction_types AS (
    SELECT
        *
    FROM
        {{ source(
            'sunglass_store_raw',
            'interaction_types'
        ) }}
)
SELECT
    id,
    LOWER(
        REPLACE(
            interaction_type,
            '_',
            ' '
        )
    ) AS interactionType
FROM
    interaction_types
