-------------------------------------------------
-- 내일 오전
-------------------------------------------------

-- 내일 오전 날씨
CREATE OR REPLACE VIEW weather_tomorrow_am_score_view AS
-- 계절 분류
WITH base AS (
    SELECT
		month,
		city,
		temperature_am,
		status_am,
		fine_dust_am,
		ultra_fine_dust_am,
		rain_prob_am,
		max_rain_mm_am,
		icon_am,
		lat,
		lng,
		geom,
        CASE 
            WHEN month IN (3,4,5) THEN 'spring'
            WHEN month IN (6,7,8) THEN 'summer'
            WHEN month IN (9,10,11) THEN 'autumn'
            ELSE 'winter'
        END AS season
    FROM weather_after
    WHERE forecast = '내일'
),

-- 계절점수 공통 감점 요인
common_deduction AS (
    SELECT
        *,
		-- 미세먼지, 초미세먼지
		CASE
		    WHEN fine_dust_am LIKE '%좋음%' THEN 0
		    WHEN fine_dust_am LIKE '%보통%' THEN 10
		    WHEN fine_dust_am LIKE '%나쁨%' AND fine_dust_am NOT LIKE '%매우%' THEN 30
		    WHEN fine_dust_am LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm10_deduct,

		CASE
		    WHEN ultra_fine_dust_am LIKE '%좋음%' THEN 0
		    WHEN ultra_fine_dust_am LIKE '%보통%' THEN 15
		    WHEN ultra_fine_dust_am LIKE '%나쁨%' AND ultra_fine_dust_am NOT LIKE '%매우%' THEN 35
		    WHEN ultra_fine_dust_am LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm25_deduct,

        -- 날씨 상태
		CASE
		    WHEN status_am LIKE '%눈%' AND status_am LIKE '%비%' THEN 50
		    WHEN status_am LIKE '%눈%' THEN 50
		    WHEN status_am LIKE '%소나기%' THEN 50
		    WHEN status_am LIKE '%비%' THEN 50
		    WHEN status_am LIKE '%황사%' THEN 70
			WHEN status_am LIKE '%안개%' THEN 25
			WHEN status_am LIKE '%박무%' THEN 25
			ELSE 0
		END AS status_deduct,

		-- 강수량 (강수확률 반영)
		CASE
			WHEN max_rain_mm_am < 3 THEN 0
			WHEN max_rain_mm_am < 15 THEN (15 * regexp_replace(rain_prob_am, '[^0-9]', '', 'g')::numeric / 100)
			WHEN max_rain_mm_am < 30 THEN (25 * regexp_replace(rain_prob_am, '[^0-9]', '', 'g')::numeric / 100)
			ELSE (40 * regexp_replace(rain_prob_am, '[^0-9]', '', 'g')::numeric / 100)
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
                        WHEN temperature_am BETWEEN 5 AND 23 THEN 0
                        ELSE 20
                    END
				)
            WHEN 'summer' THEN
                (
                    CASE
                        WHEN temperature_am >= 35 THEN 55
                        WHEN temperature_am >= 33 THEN 35
                        WHEN temperature_am BETWEEN 30 AND 33 THEN 15
                        WHEN temperature_am BETWEEN 26 AND 30 THEN 5
                        ELSE 0
                    END
				)
            WHEN 'autumn' THEN
                (
                    CASE 
                        WHEN temperature_am BETWEEN 15 AND 23 THEN 0
                        WHEN temperature_am BETWEEN 10 AND 26 THEN 10
                        ELSE 20
                    END
                )
            WHEN 'winter' THEN
                (
                    CASE
                        WHEN temperature_am <= -15 THEN 55
                        WHEN temperature_am <= -12 THEN 35
                        WHEN temperature_am BETWEEN -12 AND -5 THEN 15
                        WHEN temperature_am BETWEEN -5 AND 2 THEN 5
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
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 80 THEN '외출하기 좋음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 60 THEN '외출하기 괜찮음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 40 THEN '외출양호'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 20 THEN '외출하기 별로임'
        ELSE '외출하기 안좋음'
    END AS 등급

FROM season_deduction;


-- 내일 오전 날씨 상태
CREATE OR REPLACE VIEW weather_tomorrow_am_cond_view AS 
SELECT
	city AS 도시,
	status_am AS 날씨상태,
	temperature_am AS 기온,
	icon_am AS 아이콘,
	geom
FROM
	weather_after
WHERE
	forecast = '내일';


-- 내일 오전 미세먼지
CREATE OR REPLACE VIEW weather_tomorrow_am_dust_view AS
SELECT
	city AS 도시,
	fine_dust_am AS 미세먼지,
	ultra_fine_dust_am AS 초미세먼지,
	geom
FROM
	weather_after
WHERE
	forecast = '내일';


-- 내일 오전 강수 정보
CREATE OR REPLACE VIEW weather_tomorrow_am_rain_view AS 
SELECT
	city AS 도시,
	rain_prob_am AS 강수확률,
	max_rain_mm_am AS 최대강수량,
	geom
FROM
	weather_after
WHERE
	forecast = '내일';


-------------------------------------------------
-- 내일 오후
-------------------------------------------------

-- 내일 오후 날씨
CREATE OR REPLACE VIEW weather_tomorrow_pm_score_view AS
-- 계절 분류
WITH base AS (
    SELECT
		month,
		city,
		temperature_pm,
		status_pm,
		fine_dust_pm,
		ultra_fine_dust_pm,
		rain_prob_pm,
		max_rain_mm_pm,
		icon_pm,
		lat,
		lng,
		geom,
        CASE 
            WHEN month IN (3,4,5) THEN 'spring'
            WHEN month IN (6,7,8) THEN 'summer'
            WHEN month IN (9,10,11) THEN 'autumn'
            ELSE 'winter'
        END AS season
    FROM weather_after
    WHERE forecast = '내일'
),

-- 계절점수 공통 감점 요인
common_deduction AS (
    SELECT
        *,
		-- 미세먼지, 초미세먼지
		CASE
		    WHEN fine_dust_pm LIKE '%좋음%' THEN 0
		    WHEN fine_dust_pm LIKE '%보통%' THEN 10
		    WHEN fine_dust_pm LIKE '%나쁨%' AND fine_dust_pm NOT LIKE '%매우%' THEN 30
		    WHEN fine_dust_pm LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm10_deduct,

		CASE
		    WHEN ultra_fine_dust_pm LIKE '%좋음%' THEN 0
		    WHEN ultra_fine_dust_pm LIKE '%보통%' THEN 15
		    WHEN ultra_fine_dust_pm LIKE '%나쁨%' AND ultra_fine_dust_pm NOT LIKE '%매우%' THEN 35
		    WHEN ultra_fine_dust_pm LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm25_deduct,

        -- 날씨 상태
		CASE
		    WHEN status_pm LIKE '%눈%' AND status_pm LIKE '%비%' THEN 50
		    WHEN status_pm LIKE '%눈%' THEN 50
		    WHEN status_pm LIKE '%소나기%' THEN 50
		    WHEN status_pm LIKE '%비%' THEN 50
		    WHEN status_pm LIKE '%황사%' THEN 70
			WHEN status_pm LIKE '%안개%' THEN 25
			WHEN status_pm LIKE '%박무%' THEN 25
			ELSE 0
		END AS status_deduct,

		-- 강수량 (강수확률 반영)
		CASE
			WHEN max_rain_mm_pm < 3 THEN 0
			WHEN max_rain_mm_pm < 15 THEN (15 * regexp_replace(rain_prob_pm, '[^0-9]', '', 'g')::numeric / 100)
			WHEN max_rain_mm_pm < 30 THEN (25 * regexp_replace(rain_prob_pm, '[^0-9]', '', 'g')::numeric / 100)
			ELSE (40 * regexp_replace(rain_prob_pm, '[^0-9]', '', 'g')::numeric / 100)
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
                        WHEN temperature_pm BETWEEN 5 AND 23 THEN 0
                        ELSE 20
                    END
				)
            WHEN 'summer' THEN
                (
                    CASE
                        WHEN temperature_pm >= 35 THEN 55
                        WHEN temperature_pm >= 33 THEN 35
                        WHEN temperature_pm BETWEEN 30 AND 33 THEN 15
                        WHEN temperature_pm BETWEEN 26 AND 30 THEN 5
                        ELSE 0
                    END
                )
            WHEN 'autumn' THEN
                (
                    CASE 
                        WHEN temperature_pm BETWEEN 15 AND 23 THEN 0
                        WHEN temperature_pm BETWEEN 10 AND 26 THEN 10
                        ELSE 20
                    END
                )
            WHEN 'winter' THEN
                (
                    CASE
                        WHEN temperature_pm <= -15 THEN 55
                        WHEN temperature_pm <= -12 THEN 35
                        WHEN temperature_pm BETWEEN -12 AND -5 THEN 15
                        WHEN temperature_pm BETWEEN -5 AND 2 THEN 5
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
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 80 THEN '외출하기 좋음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 60 THEN '외출하기 괜찮음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 40 THEN '외출양호'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 20 THEN '외출하기 별로임'
        ELSE '외출하기 안좋음'
    END AS 등급

FROM season_deduction;


-- 내일 오후 날씨 상태
CREATE OR REPLACE VIEW weather_tomorrow_pm_cond_view AS 
SELECT
	city AS 도시,
	status_pm AS 날씨상태,
	temperature_pm AS 기온,
	icon_pm AS 아이콘,
	geom
FROM
	weather_after
WHERE
	forecast = '내일';


-- 내일 오후 미세먼지
CREATE OR REPLACE VIEW weather_tomorrow_pm_dust_view AS
SELECT
	city AS 도시,
	fine_dust_pm AS 미세먼지,
	ultra_fine_dust_pm AS 초미세먼지,
	geom
FROM
	weather_after
WHERE
	forecast = '내일';


-- 내일 오후 강수 정보
CREATE OR REPLACE VIEW weather_tomorrow_pm_rain_view AS 
SELECT
	city AS 도시,
	rain_prob_pm AS 강수확률,
	max_rain_mm_pm AS 최대강수량,
	geom
FROM
	weather_after
WHERE
	forecast = '내일';


-------------------------------------------------
-- 모레 오전
-------------------------------------------------

-- 모레 오전 날씨
CREATE OR REPLACE VIEW weather_dayafter_am_score_view AS
-- 계절 분류
WITH base AS (
    SELECT
		month,
		city,
		temperature_am,
		status_am,
		fine_dust_am,
		ultra_fine_dust_am,
		rain_prob_am,
		max_rain_mm_am,
		icon_am,
		lat,
		lng,
		geom,
        CASE 
            WHEN month IN (3,4,5) THEN 'spring'
            WHEN month IN (6,7,8) THEN 'summer'
            WHEN month IN (9,10,11) THEN 'autumn'
            ELSE 'winter'
        END AS season
    FROM weather_after
    WHERE forecast = '모레'
),

-- 계절점수 공통 감점 요인
common_deduction AS (
    SELECT
        *,
		-- 미세먼지, 초미세먼지
		CASE
		    WHEN fine_dust_am LIKE '%좋음%' THEN 0
		    WHEN fine_dust_am LIKE '%보통%' THEN 10
		    WHEN fine_dust_am LIKE '%나쁨%' AND fine_dust_am NOT LIKE '%매우%' THEN 30
		    WHEN fine_dust_am LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm10_deduct,

		CASE
		    WHEN ultra_fine_dust_am LIKE '%좋음%' THEN 0
		    WHEN ultra_fine_dust_am LIKE '%보통%' THEN 15
		    WHEN ultra_fine_dust_am LIKE '%나쁨%' AND ultra_fine_dust_am NOT LIKE '%매우%' THEN 35
		    WHEN ultra_fine_dust_am LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm25_deduct,

        -- 날씨 상태
		CASE
		    WHEN status_am LIKE '%눈%' AND status_am LIKE '%비%' THEN 50
		    WHEN status_am LIKE '%눈%' THEN 50
		    WHEN status_am LIKE '%소나기%' THEN 50
		    WHEN status_am LIKE '%비%' THEN 50
		    WHEN status_am LIKE '%황사%' THEN 70
			WHEN status_am LIKE '%안개%' THEN 25
			WHEN status_am LIKE '%박무%' THEN 25
			ELSE 0
		END AS status_deduct,

		-- 강수량 (강수확률 반영)
		CASE
			WHEN max_rain_mm_am < 3 THEN 0
			WHEN max_rain_mm_am < 15 THEN (15 * regexp_replace(rain_prob_am, '[^0-9]', '', 'g')::numeric / 100)
			WHEN max_rain_mm_am < 30 THEN (25 * regexp_replace(rain_prob_am, '[^0-9]', '', 'g')::numeric / 100)
			ELSE (40 * regexp_replace(rain_prob_am, '[^0-9]', '', 'g')::numeric / 100)
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
                        WHEN temperature_am BETWEEN 5 AND 23 THEN 0
                        ELSE 20
                    END
                )
            WHEN 'summer' THEN
                (
                    CASE
                        WHEN temperature_am >= 35 THEN 55
                        WHEN temperature_am >= 33 THEN 35
                        WHEN temperature_am BETWEEN 30 AND 33 THEN 15
                        WHEN temperature_am BETWEEN 26 AND 30 THEN 5
                        ELSE 0
                    END
                )
            WHEN 'autumn' THEN
                (
                    CASE 
                        WHEN temperature_am BETWEEN 15 AND 23 THEN 0
                        WHEN temperature_am BETWEEN 10 AND 26 THEN 10
                        ELSE 20
                    END
                )
            WHEN 'winter' THEN
                (
                    CASE
                        WHEN temperature_am <= -15 THEN 55
                        WHEN temperature_am <= -12 THEN 35
                        WHEN temperature_am BETWEEN -12 AND -5 THEN 15
                        WHEN temperature_am BETWEEN -5 AND 2 THEN 5
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
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 80 THEN '외출하기 좋음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 60 THEN '외출하기 괜찮음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 40 THEN '외출양호'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 20 THEN '외출하기 별로임'
        ELSE '외출하기 안좋음'
    END AS 등급

FROM season_deduction;


-- 모레 오전 날씨 상태
CREATE OR REPLACE VIEW weather_dayafter_am_cond_view AS 
SELECT
	city AS 도시,
	status_am AS 날씨상태,
	temperature_am AS 기온,
	icon_am AS 아이콘,
	geom
FROM
	weather_after
WHERE
	forecast = '모레';


-- 모레 오전 미세먼지
CREATE OR REPLACE VIEW weather_dayafter_am_dust_view AS
SELECT
	city AS 도시,
	fine_dust_am AS 미세먼지,
	ultra_fine_dust_am AS 초미세먼지,
	geom
FROM
	weather_after
WHERE
	forecast = '모레';


-- 모레 오전 강수 정보
CREATE OR REPLACE VIEW weather_dayafter_am_rain_view AS 
SELECT
	city AS 도시,
	rain_prob_am AS 강수확률,
	max_rain_mm_am AS 최대강수량,
	geom
FROM
	weather_after
WHERE
	forecast = '모레';


-------------------------------------------------
-- 모레 오후
-------------------------------------------------

-- 모레 오후 날씨
CREATE OR REPLACE VIEW weather_dayafter_pm_score_view AS
-- 계절 분류
WITH base AS (
    SELECT
		month,
		city,
		temperature_pm,
		status_pm,
		fine_dust_pm,
		ultra_fine_dust_pm,
		rain_prob_pm,
		max_rain_mm_pm,
		icon_pm,
		lat,
		lng,
		geom,
        CASE 
            WHEN month IN (3,4,5) THEN 'spring'
            WHEN month IN (6,7,8) THEN 'summer'
            WHEN month IN (9,10,11) THEN 'autumn'
            ELSE 'winter'
        END AS season
    FROM weather_after
    WHERE forecast = '내일'
),

-- 계절점수 공통 감점 요인
common_deduction AS (
    SELECT
        *,
		-- 미세먼지, 초미세먼지
		CASE
		    WHEN fine_dust_pm LIKE '%좋음%' THEN 0
		    WHEN fine_dust_pm LIKE '%보통%' THEN 10
		    WHEN fine_dust_pm LIKE '%나쁨%' AND fine_dust_pm NOT LIKE '%매우%' THEN 30
		    WHEN fine_dust_pm LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm10_deduct,

		CASE
		    WHEN ultra_fine_dust_pm LIKE '%좋음%' THEN 0
		    WHEN ultra_fine_dust_pm LIKE '%보통%' THEN 15
		    WHEN ultra_fine_dust_pm LIKE '%나쁨%' AND ultra_fine_dust_pm NOT LIKE '%매우%' THEN 35
		    WHEN ultra_fine_dust_pm LIKE '%매우나쁨%' THEN 50
		    ELSE 5
		END AS pm25_deduct,

        -- 날씨 상태
		CASE
		    WHEN status_pm LIKE '%눈%' AND status_pm LIKE '%비%' THEN 50
		    WHEN status_pm LIKE '%눈%' THEN 50
		    WHEN status_pm LIKE '%소나기%' THEN 50
		    WHEN status_pm LIKE '%비%' THEN 50
		    WHEN status_pm LIKE '%황사%' THEN 70
			WHEN status_pm LIKE '%안개%' THEN 25
			WHEN status_pm LIKE '%박무%' THEN 25
			ELSE 0
		END AS status_deduct,

		-- 강수량 (강수확률 반영)
		CASE
			WHEN max_rain_mm_pm < 3 THEN 0
			WHEN max_rain_mm_pm < 15 THEN (15 * regexp_replace(rain_prob_pm, '[^0-9]', '', 'g')::numeric / 100)
			WHEN max_rain_mm_pm < 30 THEN (25 * regexp_replace(rain_prob_pm, '[^0-9]', '', 'g')::numeric / 100)
			ELSE (40 * regexp_replace(rain_prob_pm, '[^0-9]', '', 'g')::numeric / 100)
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
                        WHEN temperature_pm BETWEEN 5 AND 23 THEN 0
                        ELSE 20
                    END
                )
            WHEN 'summer' THEN
                (
                    CASE
                        WHEN temperature_pm >= 35 THEN 55
                        WHEN temperature_pm >= 33 THEN 35
                        WHEN temperature_pm BETWEEN 30 AND 33 THEN 15
                        WHEN temperature_pm BETWEEN 26 AND 30 THEN 5
                        ELSE 0
                    END
                )
            WHEN 'autumn' THEN
                (
                    CASE 
                        WHEN temperature_pm BETWEEN 15 AND 23 THEN 0
                        WHEN temperature_pm BETWEEN 10 AND 26 THEN 10
                        ELSE 20
                    END
                )
            WHEN 'winter' THEN
                (
                    CASE
                        WHEN temperature_pm <= -15 THEN 55
                        WHEN temperature_pm <= -12 THEN 35
                        WHEN temperature_pm BETWEEN -12 AND -5 THEN 15
                        WHEN temperature_pm BETWEEN -5 AND 2 THEN 5
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
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 80 THEN '외출하기 좋음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 60 THEN '외출하기 괜찮음'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 40 THEN '외출양호'
        WHEN GREATEST(0, 100 - (
            pm10_deduct + pm25_deduct + season_deduct + status_deduct + rain_deduct
        )) >= 20 THEN '외출하기 별로임'
        ELSE '외출하기 안좋음'
    END AS 등급

FROM season_deduction;


-- 모레 오후 날씨 상태
CREATE OR REPLACE VIEW weather_dayafter_pm_cond_view AS 
SELECT
	city AS 도시,
	status_pm AS 날씨상태,
	temperature_pm AS 기온,
	icon_pm AS 아이콘,
	geom
FROM
	weather_after
WHERE
	forecast = '모레';


-- 모레 오후 미세먼지
CREATE OR REPLACE VIEW weather_dayafter_pm_dust_view AS
SELECT
	city AS 도시,
	fine_dust_pm AS 미세먼지,
	ultra_fine_dust_pm AS 초미세먼지,
	geom
FROM
	weather_after
WHERE
	forecast = '모레';


-- 모레 오후 강수 정보
CREATE OR REPLACE VIEW weather_dayafter_pm_rain_view AS 
SELECT
	city AS 도시,
	rain_prob_pm AS 강수확률,
	max_rain_mm_pm AS 최대강수량,
	geom
FROM
	weather_after
WHERE
	forecast = '모레';
