WITH analytics_tour_weather_by_city AS (
    SELECT * FROM {{ ref("analytics_tour_weather_by_city") }}
)
SELECT
    signguCode,
    cityNm,
    signguNm,
    baseYmd,
    season,
    tm_hour,
    daywkDivNm,
    deptId,
    SUM(touNum) totTouNum,
    ROUND(AVG(th3),0) th3,
    ROUND(AVG(ws),0) ws,
    ROUND(AVG(sky),0) sky,
    ROUND(AVG(rhm),0) rhm,
    ROUND(AVG(pop),0) pop
FROM analytics_tour_weather_by_city
GROUP BY signguCode, cityNm, signguNm, baseYmd, season, tm_hour, daywkDivNm, deptId
