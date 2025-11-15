WITH date_range AS (
    SELECT
        CAST(
            generate_series (
                '2019-01-01',
                '2025-12-12',
                INTERVAL '1 day'
            ) AS DATE
        ) AS dateList
)
SELECT
    dateList AS dateActual,
    to_char(
        dateList,
        'YYYY'
    ) AS year,
    to_char(
        dateList,
        'MM'
    ) AS month,
    to_char(
        dateList,
        'DD'
    ) AS day,
    lower(to_char (
        dateList,
        'Day'
    )) AS weekdayName,
    CAST(
        EXTRACT(
            dow
            FROM
                dateList
        ) AS text
    ) AS dayOfWeek,
    to_char(
        dateList,
        'Q'
    ) AS quarter,
    CASE
        WHEN EXTRACT(
            dow
            FROM
                dateList
        ) IN (
            0,
            6
        ) THEN 'weekend'
        ELSE 'weekday'
    END AS dayType
FROM
    date_range
