WITH orders AS (
    SELECT
        so.id AS orderId,
        so.userId AS userId,
        so.itemId AS itemId,
        dp.price AS sales,
        so.orderDate AS orderDate
    FROM
        {{ ref('stg_orders') }} AS so
        JOIN {{ ref('dim_products') }} AS dp
        ON so.itemId = dp.id
)
SELECT
    *
FROM
    orders
