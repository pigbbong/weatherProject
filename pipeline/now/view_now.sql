-- drop view weather_now_score_view;
-- drop view weather_now_dust_view;
-- drop view weather_now_cond_view;
-- drop view weather_now_detail_view;

-- 현재 날씨와 외출 점수를 매기는 뷰
CREATE OR REPLACE VIEW weather_now_score_view AS
-- 계절 분류
WITH base AS (
    SELECT
        *,
        CASE 
            WHEN month IN (3,4,5) THEN 'spring'
            WHEN month IN (6,7,8) THEN 'summer'
            WHEN month IN (9,10,11) THEN 'autumn'
            ELSE 'winter'
        END AS season
    FROM weather_now
),

-- 계절점수 공통 감점 요인
common_deduction AS (
    SELECT
        *,
        regexp_replace(wind_speed, '[^0-9\.]', '', 'g')::numeric AS ws,
		-- 미세먼지, 초미세먼지
		CASE
		    WHEN fine_dust LIKE '%좋음%' THEN 0
		    WHEN fine_dust LIKE '%보통%' THEN 10
		    WHEN fine_dust LIKE '%나쁨%' AND fine_dust NOT LIKE '%매우%' THEN 30
		    WHEN fine_dust LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm10_deduct,

		CASE
		    WHEN ultra_fine_dust LIKE '%좋음%' THEN 0
		    WHEN ultra_fine_dust LIKE '%보통%' THEN 15
		    WHEN ultra_fine_dust LIKE '%나쁨%' AND ultra_fine_dust NOT LIKE '%매우%' THEN 35
		    WHEN ultra_fine_dust LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm25_deduct,
		
        -- 자외선
        CASE uv
            WHEN '좋음' THEN 0
            WHEN '보통' THEN 15
            WHEN '높음' THEN 35
            WHEN '매우높음' THEN 55
            WHEN '위험' THEN 85
            ELSE 10
        END AS uv_deduct,

        -- 풍속
        CASE
            WHEN regexp_replace(wind_speed, '[^0-9\.]', '', 'g')::numeric < 4 THEN 0
            WHEN regexp_replace(wind_speed, '[^0-9\.]', '', 'g')::numeric < 7 THEN 10
            WHEN regexp_replace(wind_speed, '[^0-9\.]', '', 'g')::numeric < 10 THEN 25
            WHEN regexp_replace(wind_speed, '[^0-9\.]', '', 'g')::numeric < 14 THEN 40
            ELSE 55
        END AS wind_deduct_common,

        -- 날씨 상태
		CASE
		    WHEN status LIKE '%눈%' AND status LIKE '%비%' THEN 50
		    WHEN status LIKE '%눈%' THEN 50
		    WHEN status LIKE '%소나기%' THEN 50
		    WHEN status LIKE '%비%' THEN 50
		    WHEN status LIKE '%황사%' THEN 70
			WHEN status LIKE '%안개%'  THEN 25
			WHEN status LIKE '%박무%' THEN 25
			ELSE 0
		END AS status_deduct,

		-- 강수량
		CASE
			WHEN rain_mm < 3 THEN 0
			WHEN rain_mm < 15 THEN 15
			WHEN rain_mm < 30 THEN 25
			ELSE 40
		END AS rain_deduct
	FROM base
	
),

-- 계절별 감점 요인
season_deduction AS (
    SELECT
        *,
        CASE season
            WHEN 'spring' THEN
                (
                    CASE 
                        WHEN temperature BETWEEN 5 AND 23 THEN 0
                        ELSE 20
                    END
                    +
                    CASE 
                        WHEN humidity BETWEEN '30%' AND '75%' THEN 0
                        ELSE 15 
                    END
                )
            WHEN 'summer' THEN
                (
                    CASE
                        WHEN temperature >= 35 THEN 55
                        WHEN temperature >= 33 THEN 35
                        WHEN temperature BETWEEN 30 AND 33 THEN 15
                        WHEN temperature BETWEEN 26 AND 30 THEN 5
                        ELSE 0
                    END
                    +
                    CASE 
                        WHEN humidity <= '55%' THEN 0
                        WHEN humidity <= '70%' THEN 10
                        ELSE 25
                    END
                )
            WHEN 'autumn' THEN
                (
                    CASE 
                        WHEN temperature BETWEEN 15 AND 23 THEN 0
                        WHEN temperature BETWEEN 10 AND 26 THEN 10
                        ELSE 20
                    END
                    +
                    CASE 
                        WHEN humidity BETWEEN '30%' AND '70%' THEN 0
                        ELSE 10
                    END
                )
            WHEN 'winter' THEN
                (
                    CASE
                        WHEN temperature <= -15 THEN 55
                        WHEN temperature <= -12 THEN 35
                        WHEN temperature BETWEEN -12 AND -5 THEN 15
                        WHEN temperature BETWEEN -5 AND 2 THEN 5
                        ELSE 0
                    END
                )
        END AS season_deduct
    FROM common_deduction
)

SELECT	
    city AS 도시,
	geom,

    CASE
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + uv_deduct + wind_deduct_common + season_deduct + status_deduct + rain_deduct
        )) >= 80 THEN '외출하기 좋음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + uv_deduct + wind_deduct_common + season_deduct + status_deduct + rain_deduct
        )) >= 60 THEN '외출하기 괜찮음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + uv_deduct + wind_deduct_common + season_deduct + status_deduct + rain_deduct
        )) >= 40 THEN '외출양호'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + uv_deduct + wind_deduct_common + season_deduct + status_deduct + rain_deduct
        )) >= 20 THEN '외출하기 별로임'
        ELSE '외출하기 안좋음'
    END AS 등급

FROM season_deduction;

-- 현재 날씨 상태
CREATE OR REPLACE VIEW weather_now_cond_view AS 
SELECT
	city AS 도시,
	status AS 날씨상태,
	temperature AS 기온,
	icon AS 아이콘,
	geom
FROM
	weather_now;

-- 현재 미세먼지 상태
CREATE OR REPLACE VIEW weather_now_dust_view AS
SELECT
	city AS 도시,
	fine_dust AS 미세먼지,
	ultra_fine_dust AS 초미세먼지,
	geom
FROM
	weather_now;

-- 현재 날씨 상세 정보
CREATE OR REPLACE VIEW weather_now_detail_view AS 
SELECT
	city AS 도시,
	humidity AS 습도,
	rain_mm AS 강수,
	wind_direction AS 풍향,
	wind_speed AS 풍속,
	uv AS 자외선,
	geom
FROM
	weather_now;
	
	