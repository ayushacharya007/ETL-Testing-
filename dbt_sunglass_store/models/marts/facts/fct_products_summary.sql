WITH orders AS (
    SELECT
        itemId,
        ROUND((COUNT(itemId) * 1.0 / COUNT(DISTINCT orderDate)), 3) AS averagePurchasePerDay,
        COUNT(*) AS totalSold
    FROM
        {{ ref('stg_orders') }}
    GROUP BY
        itemId
),
products AS (
    SELECT
        dp.id,
        dp.productName AS productName,
        o.averagePurchasePerDay AS averagePurchasePerDay,
        o.totalSold AS totalSold,
        CAST(SUM(dp.price) AS DECIMAL(10, 2)) AS totalSales,
        CASE
            WHEN dp.discontinuedDate IS NOT NULL THEN (
                dp.discontinuedDate - dp.listDate
            )
            ELSE (
                CURRENT_DATE - dp.listDate
            )
        END AS daysInMarket
    FROM
        {{ ref('dim_products') }} AS dp
        LEFT JOIN orders AS o
        ON dp.id = o.itemId
    GROUP BY
        dp.id,
        dp.productName,
        o.averagePurchasePerDay,
        o.totalSold,
        dp.discontinuedDate,
        dp.listDate
),
interactions AS (
    SELECT
        itemId,
        COUNT(*) AS totalInteractions
    FROM
        {{ ref('dim_interactions') }}
    GROUP BY
        itemId
),
combine AS(
    SELECT
        p.id AS itemId,
        p.productName AS productName,
        COALESCE(
            i.totalInteractions,
            0
        ) AS totalInteractions,
        COALESCE(
            p.totalSold,
            0
        ) AS totalSold,
        COALESCE(
            p.totalSales,
            0.00
        ) AS totalSales,
        COALESCE(
            p.averagePurchasePerDay,
            0.000
        ) AS averagePurchasePerDay,
        p.daysInMarket AS daysInMarket
    FROM
        products AS p
        JOIN interactions AS i
        ON p.id = i.itemId
)
SELECT
    *
FROM
    combine
