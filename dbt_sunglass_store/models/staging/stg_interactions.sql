WITH interactions AS (
    SELECT
        *
    FROM
        {{ source(
            'sunglass_store_raw',
            'interactions'
        ) }}
)
SELECT
    {{ dbt_utils.generate_surrogate_key(
        ["user_id", "item_id", "interaction_id", "interaction_date"]
    ) }} AS id,
    user_id AS userId,
    item_id AS itemId,
    interaction_id AS interactionTypeId,
    CAST(
        interaction_date AS DATE
    ) AS interactionDate
FROM
    interactions
