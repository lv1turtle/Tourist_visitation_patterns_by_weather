WITH src_signgu AS (
    SELECT * FROM wnsldjqja.signgu
)
SELECT 
    "시군구코드" signguCode, 
    "시도명" cityNm,
    "시군구명" signguNm
FROM
    src_signgu