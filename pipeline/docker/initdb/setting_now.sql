CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS weather_now (
    year integer,
    month integer,
    day integer,
    hour integer,
	minute integer,
    weekday text,
    province text,
    city text,
    temperature numeric(4,1),
    status text,
	rain_mm numeric(4,1),
    humidity text,
    wind_direction text,
    wind_speed text,
    fine_dust text,
    ultra_fine_dust text,
    uv text,
    lat numeric(10,6),
    lng numeric(10,6),
    icon text,
    geom geometry(Point, 4326),
    PRIMARY KEY (city)
);

CREATE TABLE IF NOT EXISTS weather_now_daily (
    year integer,
    month integer,
    day integer,
    hour integer,
	minute integer,
    weekday text,
    province text,
    city text,
    temperature numeric(4,1),
    status text,
	rain_mm numeric(4,1),
    humidity text,
    wind_direction text,
    wind_speed text,
    fine_dust text,
    ultra_fine_dust text,
    uv text,
    lat numeric(10,6),
    lng numeric(10,6),
    icon text,
    PRIMARY KEY (hour, minute, city)
);


CREATE TABLE IF NOT EXISTS weather_now_daily_temp (
    year integer,
    month integer,
    day integer,
    hour integer,
	minute integer,
    weekday text,
    province text,
    city text,
    temperature numeric(4,1),
    status text,
	rain_mm numeric(4,1),
    humidity text,
    wind_direction text,
    wind_speed text,
    fine_dust text,
    ultra_fine_dust text,
    uv text,
    lat numeric(10,6),
    lng numeric(10,6),
    icon text,
    PRIMARY KEY (hour, minute, city)
);


CREATE TABLE IF NOT EXISTS weather_now_monthly (
    year integer,
    month integer,
    day integer,
    hour integer,
	minute integer,
    weekday text,
    province text,
    city text,
    temperature numeric(4,1),
    status text,
	rain_mm numeric(4,1),
    humidity text,
    wind_direction text,
    wind_speed text,
    fine_dust text,
    ultra_fine_dust text,
    uv text,
    lat numeric(10,6),
    lng numeric(10,6),
    icon text,
    PRIMARY KEY (day, hour, minute, city)
);


CREATE TABLE IF NOT EXISTS weather_now_monthly_temp (
    year integer,
    month integer,
    day integer,
    hour integer,
	minute integer,
    weekday text,
    province text,
    city text,
    temperature numeric(4,1),
    status text,
	rain_mm numeric(4,1),
    humidity text,
    wind_direction text,
    wind_speed text,
    fine_dust text,
    ultra_fine_dust text,
    uv text,
    lat numeric(10,6),
    lng numeric(10,6),
    icon text,
    PRIMARY KEY (day, hour, minute, city)
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

CREATE OR REPLACE TRIGGER trg_set_geom
BEFORE INSERT OR UPDATE ON weather_now
FOR EACH ROW
EXECUTE FUNCTION set_weather_geom();