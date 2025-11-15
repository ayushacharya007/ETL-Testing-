WITH total_rev AS (
    SELECT
        CAST(SUM(sales) AS DECIMAL(16, 2)) AS totalRevenue
    FROM
        {{ ref('fct_orders') }}
)
SELECT
    *
FROM
    total_rev
