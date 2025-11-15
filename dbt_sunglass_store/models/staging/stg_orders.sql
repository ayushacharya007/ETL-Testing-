WITH orders AS (
    SELECT
        *
    FROM
        {{ source(
            'sunglass_store_raw',
            'orders'
        ) }}
)
SELECT
    order_id AS id,
    user_id AS userId,
    item_id AS itemId,
    CAST(
        purchase_date AS DATE
    ) AS orderDate,
    LOWER(
        REPLACE(
            payment_type,
            '_',
            ' '
        )
    ) AS paymentType
FROM
    orders
