WITH  __dbt__cte__src_signgu as (
WITH src_signgu AS (
    SELECT * FROM wnsldjqja.signgu
)
SELECT 
    "시군구코드" signguCode, 
    "시도명" cityNm,
    "시군구명" signguNm
FROM
    src_signgu
), src_tour_api AS (
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