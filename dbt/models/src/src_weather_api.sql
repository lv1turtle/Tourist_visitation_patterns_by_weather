WITH src_weather_api AS (
    SELECT * FROM {{source("src","weather_api")}}
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

