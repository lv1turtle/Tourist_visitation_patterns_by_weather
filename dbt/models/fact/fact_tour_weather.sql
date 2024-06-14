{{
    config(
        materialized = 'incremental',
        unique_key = 'baseYmd || tm_hour || spotName',
        incremental_strategy='delete+insert',
        on_schema_change='fail'
    )
}}
WITH src_weather_tot AS (
    SELECT
        a.spotName,
        tm,
        th3,
        ws,
        sky,
        rhm,
        pop,
        b.signguCode,
        b.signguNm,
        b.cityNm
    FROM {{ ref("src_weather_api") }} a
    LEFT JOIN {{ ref("dim_tourist_spot") }} b
    ON a.spotName = b.spotName
), src_tour_api AS (
    SELECT
        a.signguCode,
        b.cityNm,
        b.signguNm,
        baseYmd,
        daywkDivNm,
        touDivNm,
        touNum
    FROM {{ ref("src_tour_api") }} a
    JOIN {{ ref("src_signgu") }} b
    ON a.signguCode = b.signguCode
), tmp AS(
    SELECT
        baseYmd,
        date_part('hour',tm) tm_hour,
        spotName,
        th3,
        ws,
        sky,
        rhm,
        pop,
        daywkDivNm,
        touNum,
        touDivNm
    FROM src_weather_tot a
    JOIN src_tour_api b
    ON CONCAT(a.cityNm,a.signguNm) = CONCAT(b.cityNm,b.signguNm)
    AND TO_CHAR(a.tm,'YYYYMMDD') = TO_CHAR(b.baseYmd,'YYYYMMDD')
)
SELECT * FROM tmp
WHERE baseYmd is not NULL
{% if is_incremental() %}
    AND TO_CHAR(baseYmd,'YYYYMMDD') not in (SELECT DISTINCT TO_CHAR(baseYmd,'YYYYMMDD') from {{ this }})
{% endif %}