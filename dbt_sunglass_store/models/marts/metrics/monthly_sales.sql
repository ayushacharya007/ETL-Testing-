WITH dates AS (
    SELECT
        dateActual,
        CONCAT(year, '-', LPAD(month :: text, 2, '0')) AS monthYear
    FROM
        {{ ref('dim_dates') }}
),
combine AS (
    SELECT
        d.monthYear,
        SUM(
            fo.sales
        ) AS totalSales
    FROM
        {{ ref('fct_orders') }} AS fo
        JOIN dates AS d
        ON fo.orderDate = d.dateActual
    GROUP BY
        monthYear
)
SELECT
    *
FROM
    combine
