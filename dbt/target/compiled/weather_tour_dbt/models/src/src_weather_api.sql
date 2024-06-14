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