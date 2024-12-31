WITH src_korea_dept AS (
    SELECT * FROM {{source("src","korea_dept")}}
)
SELECT 
    "지역코드" deptId, 
    "지역이름" cityNm
FROM
    src_korea_dept