WITH src_tour_api AS (
    SELECT * FROM wnsldjqja.tourism
), src_signgu AS(
    SELECT * FROM {{ ref("src_signgu") }}
)
SELECT 
    left(signguCode,5) signguCode,
    baseYmd,
    daywkDivNm,
    touDivNm,
    touNum
FROM src_tour_api
