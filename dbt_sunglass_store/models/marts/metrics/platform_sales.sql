WITH platform_sales AS(
    SELECT
        du.fromPlatform AS platform,
        CAST(SUM(fo.sales) AS DECIMAL(16, 2)) AS totalSales
    FROM
        {{ ref('dim_users') }} AS du
        JOIN {{ ref('fct_orders') }} AS fo
        ON du.id = fo.userId
    GROUP BY
        du.fromPlatform
)
SELECT
    *
FROM
    platform_sales
