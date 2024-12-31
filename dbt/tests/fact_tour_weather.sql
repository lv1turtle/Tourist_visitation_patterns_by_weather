WITH is_dup AS (
    SELECT
        baseYmd,
        tmHour,
        spotName,
        ROW_NUMBER() OVER (PARTITION BY baseYmd, tmHour, spotName) AS row_num
    FROM
        {{ ref("fact_tour_weather") }}
)
SELECT * FROM is_dup WHERE row_num > 1