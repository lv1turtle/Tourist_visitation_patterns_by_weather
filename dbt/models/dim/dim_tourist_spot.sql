WITH sg AS (
    SELECT
        signguCode,
        cityNm,
        signguNm
    FROM {{ ref("src_signgu") }}
), ts AS(
    SELECT 
        spotName,
        signguCode,
        thema
    FROM {{ ref("src_tourist_spot") }}
), kd AS(
    SELECT * FROM {{ ref("src_korea_dept") }}
), tmp AS(
SELECT
    ts.spotName,
    ts.thema,
    sg.signguCode,
    sg.cityNm,
    sg.signguNm
FROM ts
LEFT JOIN sg ON ts.signguCode = sg.signguCode
)

SELECT
    spotName,
    thema,
    signguCode,
    tmp.cityNm,
    signguNm,
    deptId
FROM tmp
LEFT JOIN kd ON tmp.cityNm = kd.cityNm