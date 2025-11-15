WITH users AS (
    SELECT
        id,
        fullName
    FROM
        {{ ref('dim_users') }}
),
orders AS (
    SELECT
        so.userId,
        COUNT(*) AS totalOrders,
        CAST(SUM(dp.price) AS DECIMAL(10, 2)) AS totalSpend,
        MIN(
            so.orderDate
        ) AS firstPurchaseDate,
        MAX(
            so.orderDate
        ) AS lastPurchaseDate,
        (CURRENT_DATE - MIN(so.orderDate)) AS daysToFirstPurchase,
        (CURRENT_DATE - MAX(so.orderDate)) AS daysSinceLastPurchase
    FROM
        {{ ref('stg_orders') }} AS so
        JOIN {{ ref('dim_products') }} AS dp
        ON dp.id = so.itemId
    GROUP BY
        userId
),
interactions AS (
    SELECT
        userId,
        MIN(interactionDate) AS firstInteractionDate,
        MAX(interactionDate) AS lastInteractionDate,
        COUNT(*) AS totalInteractions,
        CASE
            WHEN (CURRENT_DATE - MAX(interactionDate)) < 100 THEN 'active'
            ELSE 'inactive'END AS stillActive
            FROM
                {{ ref('dim_interactions') }}
            GROUP BY
                userId
        ),
        FINAL AS (
            SELECT
                u.id AS userId,
                u.fullName AS fullName,
                COALESCE(
                    i.totalInteractions,
                    0
                ) AS totalInteractions,
                COALESCE(
                    o.totalOrders,
                    0
                ) AS totalOrders,
                COALESCE(
                    o.totalSpend,
                    0.00
                ) AS totalSpend,
                o.firstPurchaseDate AS firstPurchaseDate,
                COALESCE(
                    o.daysToFirstPurchase,
                    0
                ) AS daysToFirstPurchase,
                o.lastPurchaseDate AS lastPurchaseDate,
                COALESCE(
                    o.daysSinceLastPurchase,
                    0
                ) AS daysSinceLastPurchase,
                i.firstInteractionDate AS firstInteractionDate,
                i.lastInteractionDate AS lastInteractionDate,
                i.stillActive AS stillActive
            FROM
                users AS u
                LEFT JOIN orders AS o
                ON u.id = o.userId
                LEFT JOIN interactions AS i
                ON u.id = i.userId
        )
    SELECT
        *
    FROM
        FINAL
