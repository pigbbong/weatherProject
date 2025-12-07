CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS weather_after (
    year integer,
    month integer,
    day integer,
    hour integer,
    weekday text,
	forecast text,
    province text,
    city text,
    temperature_am numeric(4,1),
    status_am text,
    fine_dust_am text,
    ultra_fine_dust_am text,
	rain_prob_am text,
	max_rain_mm_am numeric(4,1),
	icon_am text,
    temperature_pm numeric(4,1),
    status_pm text,
    fine_dust_pm text,
    ultra_fine_dust_pm text,
	rain_prob_pm text,
	max_rain_mm_pm numeric(4,1),
	icon_pm text,
    lat numeric(10,6),
    lng numeric(10,6),
    geom geometry(Point, 4326),
    PRIMARY KEY (day, hour, forecast, city)
);

CREATE TABLE IF NOT EXISTS weather_after_monthly (
    year integer,
    month integer,
    day integer,
    hour integer,
    weekday text,
	forecast text,
    province text,
    city text,
    temperature_am numeric(4,1),
    status_am text,
    fine_dust_am text,
    ultra_fine_dust_am text,
	rain_prob_am text,
	max_rain_mm_am numeric(4,1),
	icon_am text,
    temperature_pm numeric(4,1),
    status_pm text,
    fine_dust_pm text,
    ultra_fine_dust_pm text,
	rain_prob_pm text,
	max_rain_mm_pm numeric(4,1),
	icon_pm text,
    lat numeric(10,6),
    lng numeric(10,6),
    PRIMARY KEY (year, month, day, hour, forecast, city)
);


CREATE TABLE IF NOT EXISTS weather_after_monthly_temp (
    year integer,
    month integer,
    day integer,
    hour integer,
    weekday text,
	forecast text,
    province text,
    city text,
    temperature_am numeric(4,1),
    status_am text,
    fine_dust_am text,
    ultra_fine_dust_am text,
	rain_prob_am text,
	max_rain_mm_am numeric(4,1),
	icon_am text,
    temperature_pm numeric(4,1),
    status_pm text,
    fine_dust_pm text,
    ultra_fine_dust_pm text,
	rain_prob_pm text,
	max_rain_mm_pm numeric(4,1),
	icon_pm text,
    lat numeric(10,6),
    lng numeric(10,6),
    PRIMARY KEY (year, month, day, hour, forecast, city)
);


CREATE OR REPLACE FUNCTION set_weather_geom()
RETURNS trigger AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lng IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.lng, NEW.lat), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_set_geom_after
BEFORE INSERT OR UPDATE ON weather_after
FOR EACH ROW
EXECUTE FUNCTION set_weather_geom();
