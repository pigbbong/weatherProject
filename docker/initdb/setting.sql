CREATE EXTENSION IF NOT EXISTS postgis;

CREATE TABLE IF NOT EXISTS weather_ultrashort_fcst (
    city        text,
    province    text,
    lat         numeric(10,6),
    lng         numeric(10,6),
    basetime    varchar(12),
    fcsthour   	integer,
    icon_map    text,
    "PTY"       integer,
    "SKY"       integer,
    "T1H"       numeric(4,1),
    "REH"       integer,
    "RN1"       text,
	"VEC"		integer,
    "WSD"       numeric(5,1),
    geom        geometry(Point, 4326),
    PRIMARY KEY (city, basetime, fcsthour)
);

CREATE INDEX IF NOT EXISTS idx_ultrashort_geom
ON weather_ultrashort_fcst
USING GIST (geom);


CREATE TABLE IF NOT EXISTS weather_short_fcst (
    city        text,
    province    text,
    lat         numeric(10,6),
    lng         numeric(10,6),
    basetime    varchar(12),
    fcstdate    varchar(12),
	fcsthour	varchar(12),
    icon_map    text,
    "PTY"       integer,
    "SKY"       integer,
    "TMP"       numeric(4,1),
    "REH"       integer,
    "PCP"       text,
    "SNO"       text,
    "POP"       integer,
    "VEC"       integer,
    "WSD"       numeric(5,1),
    geom        geometry(Point, 4326),
    PRIMARY KEY (city, basetime, fcstdate, fcsthour)
);

CREATE INDEX IF NOT EXISTS idx_short_geom
ON weather_short_fcst
USING GIST (geom);


CREATE OR REPLACE FUNCTION set_weather_geom()
RETURNS trigger AS $$
BEGIN
    IF NEW.lat IS NOT NULL AND NEW.lng IS NOT NULL THEN
        NEW.geom = ST_SetSRID(ST_MakePoint(NEW.lng, NEW.lat), 4326);
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE TRIGGER trg_set_geom_ultraShort_fcst
BEFORE INSERT OR UPDATE ON weather_ultraShort_fcst
FOR EACH ROW
EXECUTE FUNCTION set_weather_geom();

CREATE OR REPLACE TRIGGER trg_set_geom_short_fcst
BEFORE INSERT OR UPDATE ON weather_short_fcst
FOR EACH ROW
EXECUTE FUNCTION set_weather_geom();