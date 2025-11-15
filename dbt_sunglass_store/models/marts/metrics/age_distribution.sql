WITH age_group AS(
    SELECT
        CASE
            WHEN age < 18 THEN 'under 18'
            WHEN age BETWEEN 18
            AND 30 THEN '18-30'
            WHEN age BETWEEN 31
            AND 50 THEN '31-50'
            WHEN age > 50 THEN '50+'
            ELSE 'unknown'
        END AS ageGroup,
        COUNT(*) AS userCount
    FROM
        {{ ref('dim_users') }}
    GROUP BY
        1
)
SELECT
    *
FROM
    age_group
