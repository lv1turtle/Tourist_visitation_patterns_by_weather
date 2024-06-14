WITH src_tourist_spot AS (
    SELECT * FROM wnsldjqja.tourist_spot 
)
SELECT 
    "관광지명" spotName,
    "시/군/구 코드" signguCode,
    "테마명" thema
FROM
    src_tourist_spot