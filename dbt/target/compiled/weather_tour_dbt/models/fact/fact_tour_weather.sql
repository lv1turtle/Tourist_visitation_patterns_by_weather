
WITH  __dbt__cte__src_weather_api as (
WITH src_weather_api AS (
    SELECT * FROM wnsldjqja.weather_info
)
SELECT
    spotName,
    tm,
    courseAreaId,
    spotAreaName,
    th3,
    ws,
    sky,
    rhm,
    pop
FROM
    src_weather_api
),  __dbt__cte__src_signgu as (
WITH src_signgu AS (
    SELECT * FROM wnsldjqja.signgu
)
SELECT 
    "시군구코드" signguCode, 
    "시도명" cityNm,
    "시군구명" signguNm
FROM
    src_signgu
),  __dbt__cte__src_tour_api as (
WITH src_tour_api AS (
    SELECT * FROM wnsldjqja.tourism
), src_signgu AS(
    SELECT * FROM __dbt__cte__src_signgu
)
SELECT 
    left(signguCode,5) signguCode,
    baseYmd,
    daywkDivNm,
    touDivNm,
    touNum
FROM src_tour_api
), src_weather_tot AS (
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
    FROM __dbt__cte__src_weather_api a
    LEFT JOIN "dev"."wnsldjqja"."dim_tourist_spot" b
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
    FROM __dbt__cte__src_tour_api a
    JOIN __dbt__cte__src_signgu b
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

    AND TO_CHAR(baseYmd,'YYYYMMDD') not in (SELECT DISTINCT TO_CHAR(baseYmd,'YYYYMMDD') from "dev"."wnsldjqja"."fact_tour_weather")
