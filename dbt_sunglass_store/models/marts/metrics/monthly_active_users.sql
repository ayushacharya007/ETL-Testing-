WITH dates AS(
    SELECT
        dateActual,
        to_char(
            dateActual,
            'YYYY-MM'
        ) AS monthYear
    FROM
        {{ ref('dim_dates') }}
),
active_users AS (
    SELECT
        d.monthYear,
        COUNT(
            DISTINCT di.userId
        ) AS userCount
    FROM
        {{ ref('dim_interactions') }} AS di
        JOIN dates AS d
        ON di.interactionDate = d.dateActual
    GROUP BY
        d.monthYear
)
SELECT
    *
FROM
    active_users
