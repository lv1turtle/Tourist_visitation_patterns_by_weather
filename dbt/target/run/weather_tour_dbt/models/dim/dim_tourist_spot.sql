
  
    

  create  table
    "dev"."wnsldjqja"."dim_tourist_spot__dbt_tmp"
    
    
    
  as (
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
),  __dbt__cte__src_tourist_spot as (
WITH src_tourist_spot AS (
    SELECT * FROM wnsldjqja.tourist_spot 
)
SELECT 
    "관광지명" spotName,
    "시/군/구 코드" signguCode,
    "테마명" thema
FROM
    src_tourist_spot
),  __dbt__cte__src_korea_dept as (
WITH src_korea_dept AS (
    SELECT * FROM wnsldjqja.korea_dept
)
SELECT 
    "지역코드" deptId, 
    "지역이름" cityNm
FROM
    src_korea_dept
), sg AS (
    SELECT
        signguCode,
        cityNm,
        signguNm
    FROM __dbt__cte__src_signgu
), ts AS(
    SELECT 
        spotName,
        signguCode,
        thema
    FROM __dbt__cte__src_tourist_spot
), kd AS(
    SELECT * FROM __dbt__cte__src_korea_dept
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
  );
  