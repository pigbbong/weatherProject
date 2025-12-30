----------------------------------------------------------
-- 초단기예보
----------------------------------------------------------

-- 날씨 뷰
CREATE OR REPLACE VIEW weather_ultrashortfcst_cond_view AS
WITH temp_daily AS (
    SELECT
        city,
        MAX("T1H") AS max_tmp,
        MIN("T1H") AS min_tmp,
        MAX(geom) AS geom
    FROM weather_ultrashort_fcst
    GROUP BY city
),
weather_freq AS (
    SELECT
        city,
        split_part(icon_map, '_', 1) AS weather_key,
        COUNT(*) AS weather_frequency
    FROM weather_ultrashort_fcst
    GROUP BY city, split_part(icon_map, '_', 1)
),
weather_ranked AS (
    SELECT
        w.city,
        w.icon_map,
        f.weather_frequency,
		f.weather_key,
        CASE
			WHEN split_part(w.icon_map, '_', 1) = '번개,뇌우' THEN 6
            WHEN split_part(w.icon_map, '_', 1) IN ('강한눈','강한비') THEN 5
            WHEN split_part(w.icon_map, '_', 1) = '눈비'              THEN 4
            WHEN split_part(w.icon_map, '_', 1) IN ('비','눈')        THEN 3
            WHEN split_part(w.icon_map, '_', 1) IN ('약한눈','약한비') THEN 2
            ELSE 1
        END AS weather_priority
    FROM weather_ultrashort_fcst w
    JOIN weather_freq f
      ON w.city = f.city
     AND f.weather_key = split_part(w.icon_map, '_', 1)
),
weather_daily AS (
    SELECT
        city,
        icon_map,
		weather_key,
        ROW_NUMBER() OVER (
            PARTITION BY city
            ORDER BY weather_priority DESC, weather_frequency DESC
        ) AS rn
    FROM weather_ranked
)
SELECT
    t.city AS 도시,
    t.max_tmp || ' ~ ' || t.min_tmp AS 기온,
    w.icon_map AS 아이콘,
	w.weather_key AS 날씨상태,
    t.geom,
	ST_AsGeoJSON(t.geom)::json AS geometry
FROM temp_daily t
JOIN weather_daily w
  ON t.city = w.city
WHERE w.rn = 1;


-- 강수 뷰
CREATE OR REPLACE VIEW weather_ultrashortfcst_rain_view AS
WITH rain_ranked AS (
    SELECT
        city,
        CASE 
			WHEN "RN1" NOT IN ('0mm', '1mm 미만', '30~50mm', '50mm 이상') 
			THEN "RN1" || 'mm' 
			ELSE "RN1"
		END AS "RN1",
        ROW_NUMBER() OVER (
            PARTITION BY city
            ORDER BY
                CASE
                    WHEN "RN1" IS NULL THEN 0.0
                    ELSE
                        CASE
                            WHEN "RN1" = '0mm' THEN 0.0
                            WHEN "RN1" = '1mm 미만' THEN 0.5
                            WHEN "RN1" = '30~50mm' THEN 30.0
                            WHEN "RN1" = '50mm 이상' THEN 50.0
                            ELSE regexp_replace("RN1", '[^0-9\.]', '', 'g')::numeric(4, 1)
                        END
                END DESC
        ) AS rn,
		geom
    FROM weather_ultrashort_fcst
)
SELECT
    city AS 도시,
    "RN1" AS 강수량,
    geom
FROM rain_ranked
WHERE rn = 1;


-- 바람 뷰
CREATE OR REPLACE VIEW weather_ultrashortfcst_wind_view AS
SELECT
	도시,
	풍향,
	풍속,
	geom
FROM (
	SELECT
	    city AS 도시,
		CASE
		    WHEN ("VEC" BETWEEN 349 AND 360)
		      OR ("VEC" BETWEEN 0 AND 11) THEN '북풍'
		    WHEN "VEC" BETWEEN 12  AND 33  THEN '북북동'
		    WHEN "VEC" BETWEEN 34  AND 56  THEN '북동'
		    WHEN "VEC" BETWEEN 57  AND 78  THEN '동북동'
		    WHEN "VEC" BETWEEN 79  AND 101 THEN '동풍'
		    WHEN "VEC" BETWEEN 102 AND 123 THEN '동남동'
		    WHEN "VEC" BETWEEN 124 AND 146 THEN '동남풍'
		    WHEN "VEC" BETWEEN 147 AND 168 THEN '남남동'
		    WHEN "VEC" BETWEEN 169 AND 191 THEN '남풍'
		    WHEN "VEC" BETWEEN 192 AND 213 THEN '남남서'
		    WHEN "VEC" BETWEEN 214 AND 236 THEN '남서풍'
		    WHEN "VEC" BETWEEN 237 AND 258 THEN '서남서'
		    WHEN "VEC" BETWEEN 259 AND 281 THEN '서풍'
		    WHEN "VEC" BETWEEN 282 AND 303 THEN '서북서'
		    WHEN "VEC" BETWEEN 304 AND 326 THEN '북서풍'
		    WHEN "VEC" BETWEEN 327 AND 348 THEN '북북서'
		    ELSE NULL
		END AS 풍향,
		"WSD" AS 풍속,
		ROW_NUMBER () OVER (PARTITION BY city ORDER BY "WSD" DESC) AS rn,
		geom
	FROM weather_ultrashort_fcst
) t
WHERE rn = 1;


-- 습도 뷰
CREATE OR REPLACE VIEW weather_ultrashortfcst_humid_view AS
SELECT
	city AS 도시,
	ROUND(AVG("REH"), 0) AS 습도,
	geom
FROM weather_ultrashort_fcst
GROUP BY city, basetime, geom;

select * from weather_ultrashortfcst_humid_view;

----------------------------------------------------------
-- 단기예보
----------------------------------------------------------

-- 날씨 뷰
CREATE OR REPLACE VIEW weather_shortfcst_cond_view AS
WITH temp_daily AS (
    SELECT
        city,
        fcstdate,
        MAX("TMP") AS max_tmp,
        MIN("TMP") AS min_tmp,
        MAX(geom) AS geom
    FROM weather_short_fcst
    GROUP BY city, fcstdate
),
weather_freq AS (
    SELECT
        city,
        fcstdate,
        split_part(icon_map, '_', 1) AS weather_key,
        COUNT(*) AS weather_frequency
    FROM weather_short_fcst
    GROUP BY city, fcstdate, split_part(icon_map, '_', 1)
),
weather_ranked AS (
    SELECT
        w.city,
        w.fcstdate,
        w.icon_map,
        f.weather_frequency,
		f.weather_key,
        CASE
            WHEN split_part(w.icon_map, '_', 1) IN ('강한눈','강한비') THEN 5
            WHEN split_part(w.icon_map, '_', 1) = '눈비'              THEN 4
            WHEN split_part(w.icon_map, '_', 1) IN ('비','눈')        THEN 3
            WHEN split_part(w.icon_map, '_', 1) IN ('약한눈','약한비') THEN 2
            ELSE 1
        END AS weather_priority
    FROM weather_short_fcst w
    JOIN weather_freq f
      ON w.city = f.city
     AND w.fcstdate = f.fcstdate
     AND split_part(w.icon_map, '_', 1) = f.weather_key
),
weather_daily AS (
    SELECT
        city,
        fcstdate,
        icon_map,
		weather_key,
        ROW_NUMBER() OVER (
            PARTITION BY city, fcstdate
            ORDER BY weather_priority DESC, weather_frequency DESC
        ) AS rn
    FROM weather_ranked
)
SELECT
    t.city 도시,
    t.fcstdate,
    t.max_tmp || ' ~ ' || t.min_tmp AS 기온,
    w.icon_map AS 아이콘,
	w.weather_key AS 날씨상태,
    t.geom
FROM temp_daily t
JOIN weather_daily w
  ON t.city = w.city
 AND t.fcstdate = w.fcstdate
WHERE w.rn = 1;


-- 강수 뷰
CREATE OR REPLACE VIEW weather_shortfcst_rain_view AS
WITH rain_ranked AS (
    SELECT
        city,
        fcstdate,
        "PCP",
        "POP",
        geom,
        ROW_NUMBER() OVER (
            PARTITION BY city, fcstdate
            ORDER BY
                CASE
                    WHEN "PCP" IS NULL THEN 0
                    WHEN "PCP" = '0mm' THEN 0
                    WHEN "PCP" = '1mm 미만' THEN 0.5
                    WHEN "PCP" = '30~50mm' THEN 30
                    WHEN "PCP" = '50mm 이상' THEN 50
                    ELSE regexp_replace("PCP", '[^0-9\.]', '', 'g')::numeric
                END DESC, "POP" DESC
        ) AS rn
    FROM weather_short_fcst
),
snow_ranked AS (
    SELECT
        city,
        fcstdate,
        "SNO",
        ROW_NUMBER() OVER (
            PARTITION BY city, fcstdate
            ORDER BY
                CASE
                    WHEN "SNO" IS NULL THEN 0
                    WHEN "SNO" = '0cm' THEN 0
                    WHEN "SNO" = '0.5cm 미만' THEN 0.25
                    WHEN "SNO" = '5.0cm 이상' THEN 5
                    ELSE regexp_replace("SNO", '[^0-9\.]', '', 'g')::numeric
                END DESC
        ) AS rn
    FROM weather_short_fcst
)
SELECT
    r.city AS 도시,
    r.fcstdate,

    /* 글피 예보 강수량 정성코드 해석 */
    CASE
        WHEN r."PCP" = '1' THEN '시간당 3mm 미만'
        WHEN r."PCP" = '2' THEN '시간당 3~15mm'
        WHEN r."PCP" = '3' THEN '시간당 15mm 이상'
        ELSE r."PCP"
    END AS 강수량,

    /* 글피 예보 강설량 정성코드 해석 */
    CASE
        WHEN s."SNO" = '1' THEN '시간당 1cm 미만'
        WHEN s."SNO" = '2' THEN '시간당 1cm 이상'
        ELSE s."SNO"
    END AS 강설량,

    r."POP" AS 강수확률,
    r.geom

FROM rain_ranked r
JOIN snow_ranked s
  ON r.city = s.city
 AND r.fcstdate = s.fcstdate
 AND s.rn = 1
WHERE r.rn = 1;


-- 바람 뷰
CREATE OR REPLACE VIEW weather_shortfcst_wind_view AS
SELECT
	도시,
	fcstdate,
	풍향,
	풍속,
	geom
FROM (
	SELECT
	    city AS 도시,
		fcstdate,
		CASE
		    WHEN ("VEC" BETWEEN 349 AND 360)
		      OR ("VEC" BETWEEN 0 AND 11) THEN '북풍'
		    WHEN "VEC" BETWEEN 12  AND 33  THEN '북북동'
		    WHEN "VEC" BETWEEN 34  AND 56  THEN '북동'
		    WHEN "VEC" BETWEEN 57  AND 78  THEN '동북동'
		    WHEN "VEC" BETWEEN 79  AND 101 THEN '동풍'
		    WHEN "VEC" BETWEEN 102 AND 123 THEN '동남동'
		    WHEN "VEC" BETWEEN 124 AND 146 THEN '동남풍'
		    WHEN "VEC" BETWEEN 147 AND 168 THEN '남남동'
		    WHEN "VEC" BETWEEN 169 AND 191 THEN '남풍'
		    WHEN "VEC" BETWEEN 192 AND 213 THEN '남남서'
		    WHEN "VEC" BETWEEN 214 AND 236 THEN '남서풍'
		    WHEN "VEC" BETWEEN 237 AND 258 THEN '서남서'
		    WHEN "VEC" BETWEEN 259 AND 281 THEN '서풍'
		    WHEN "VEC" BETWEEN 282 AND 303 THEN '서북서'
		    WHEN "VEC" BETWEEN 304 AND 326 THEN '북서풍'
		    WHEN "VEC" BETWEEN 327 AND 348 THEN '북북서'
		    ELSE NULL
		END AS 풍향,
		"WSD" AS 풍속,
		ROW_NUMBER () OVER (PARTITION BY city, fcstdate ORDER BY "WSD" DESC) AS rn,
		geom
	FROM weather_short_fcst
) t
WHERE rn = 1;


-- 습도 뷰
CREATE OR REPLACE VIEW weather_shortfcst_humid_view AS
SELECT
	city AS 도시,
	fcstdate,
	ROUND(AVG("REH"), 0) AS 습도,
	geom
FROM weather_short_fcst
GROUP BY city, fcstdate, geom;