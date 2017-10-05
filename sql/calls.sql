-- generate the timestampz for a gtfs schedule date and time
CREATE OR REPLACE FUNCTION wall_timez(d date, t interval, zone text)
    RETURNS timestamp with time zone AS $$
        SELECT ($1 + '12:00'::time)::timestamp without time zone at time zone $3 - interval '12 HOURS' + $2
    $$
LANGUAGE SQL IMMUTABLE;

CREATE INDEX pos_vid ON rt_vehicle_positions (vehicle_id);
CREATE INDEX pos_trip_id ON rt_vehicle_positions (trip_id);
CREATE INDEX pos_vid_date ON rt_vehicle_positions (trip_start_date, vehicle_id);

CREATE OR REPLACE FUNCTION safe_locate
  (route geometry, point geometry, start numeric, finish numeric, length numeric)
  RETURNS numeric AS $$
    -- Multiply the fractional distance also the substring by the substring,
    -- then add the start distance
    SELECT GREATEST(0, start) + ST_LineLocatePoint(
      ST_LineSubstring(route, GREATEST(0, start / length), LEAST(1, finish / length)),
      point
    )::numeric * (
      -- The absolute distance between start and finish
      LEAST(length, finish) - GREATEST(0, start)
    );
  $$ LANGUAGE SQL;

-- call time is a timestampz, will be passed into the db as a UTC datetime
CREATE TABLE IF NOT EXISTS calls (
  trip_id text not null,
  deviation interval,
  call_time timestamp with time zone not null,
  stop_id text,
  vehicle_id text not null,
  direction_id integer,
  route_id text,
  date date,
  feed_index int,
  source text,
  CONSTRAINT calls_pkey PRIMARY KEY (vehicle_id, call_time)
);
CREATE INDEX calls_rds ON calls (route_id, direction_id, stop_id);
