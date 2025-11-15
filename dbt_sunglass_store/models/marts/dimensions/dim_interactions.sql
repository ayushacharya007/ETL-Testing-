WITH interactions AS (
    SELECT
        *
    FROM
        {{ ref("stg_interactions") }}
),
interaction_types AS(
    SELECT
        *
    FROM
        {{ ref('stg_interaction_types') }}
),
combine as (
    SELECT
        i.id,
        i.userId,
        i.itemId,
        it.interactionType,
        i.interactionDate
    FROM
        interactions AS i
        JOIN interaction_types AS it
        ON i.interactionTypeId = it.id
)
SELECT
    *
FROM
    combine
