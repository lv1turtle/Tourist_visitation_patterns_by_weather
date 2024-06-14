WITH dim_tourist_spot AS (
    SELECT * FROM "dev"."wnsldjqja"."dim_tourist_spot"
), fact_tour_weather AS(
    SELECT * FROM "dev"."wnsldjqja"."fact_tour_weather"
), tmp1 AS(
    SELECT
        a.spotName,
        signguNm,
        baseYmd,
        CASE WHEN extract('MONTH' from baseYmd) in (3,4,5) THEN 'SPRING'
        WHEN extract('MONTH' from baseYmd) in (6,7,8) THEN 'SUMMER'
        WHEN extract('MONTH' from baseYmd) in (9,10,11) THEN 'FALL'
        ELSE 'WINTER'
        END AS season,
        tm_hour,
        th3,
        ws,
        sky,
        rhm,
        pop,
        daywkDivNm,
        touNum,
        touDivNm,
        cityNm,
        signguCode,
        deptId
    FROM fact_tour_weather a
    LEFT JOIN dim_tourist_spot b
    ON a.spotName = b.spotName
)
SELECT
    signguCode,
    cityNm,
    signguNm,
    baseYmd,
    season,
    tm_hour,
    touDivNm,
    daywkDivNm,
    deptId,
    AVG(touNum) touNum,
    ROUND(AVG(th3),0) th3,
    ROUND(AVG(ws),0) ws,
    ROUND(AVG(sky),0) sky,
    ROUND(AVG(rhm),0) rhm,
    ROUND(AVG(pop),0) pop
FROM tmp1
GROUP BY signguCode, cityNm, signguNm, baseYmd, season, tm_hour, touDivNm, daywkDivNm, deptId