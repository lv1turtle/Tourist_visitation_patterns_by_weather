
      
        
            delete from "dev"."wnsldjqja"."fact_tour_weather"
            where (
                baseYmd || tm_hour || spotName) in (
                select (baseYmd || tm_hour || spotName)
                from "fact_tour_weather__dbt_tmp101518751770"
            );

        
    

    insert into "dev"."wnsldjqja"."fact_tour_weather" ("baseymd", "tm_hour", "spotname", "th3", "ws", "sky", "rhm", "pop", "daywkdivnm", "tounum", "toudivnm")
    (
        select "baseymd", "tm_hour", "spotname", "th3", "ws", "sky", "rhm", "pop", "daywkdivnm", "tounum", "toudivnm"
        from "fact_tour_weather__dbt_tmp101518751770"
    )
  