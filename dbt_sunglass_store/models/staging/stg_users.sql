WITH users AS (
    SELECT
        *
    FROM
        {{ source(
            'sunglass_store_raw',
            'users'
        ) }}
)
SELECT
    user_id AS id,
    INITCAP(
        CONCAT(
            first_name,
            ' ',
            last_name
        )
    ) AS fullName,
    email,
    age,
    LOWER(gender) AS gender,
    CAST(
        post_code AS VARCHAR
    ) AS postCode,
    country,
    CAST(
        join_date AS DATE
    ) AS joinDate,
    REPLACE(
        from_platform,
        '_',
        ' '
    ) AS fromPlatform
FROM
    users
