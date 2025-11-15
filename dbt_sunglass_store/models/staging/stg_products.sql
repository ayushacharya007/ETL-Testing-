WITH products AS (
    SELECT
        *
    FROM
        {{ source(
            'sunglass_store_raw',
            'products'
        ) }}
)
SELECT
    item_id AS id,
    REPLACE(
        brand,
        '_',
        ' '
    ) AS brand,
    product_name AS productName,
    eye_size AS eyeSize,
    LOWER(lens_color) AS lensColor,
    CAST(price AS DECIMAL(10, 2)) AS price,
    polarized_glasses AS polarized,
    prescribed_glasses AS prescribed,
    LOWER(is_active) AS isActive,
    CAST(
        list_date AS DATE
    ) AS listDate,
    CAST(
        discontinued_date AS DATE
    ) AS discontinuedDate
FROM
    products
