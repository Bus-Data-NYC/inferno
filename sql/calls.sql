-- generate the timestampz for a gtfs schedule date and time
CREATE OR REPLACE FUNCTION wall_time(d date, t interval, zone text)
    RETURNS timestamp with time zone AS $$
        SELECT ($1 + '12:00'::time)::timestamp without time zone at time zone $3 - interval '12 HOURS' + $2
    $$
LANGUAGE SQL IMMUTABLE;

CREATE INDEX pos_vid ON rt_vehicle_positions (vehicle_id);
CREATE INDEX pos_trip_id ON rt_vehicle_positions (trip_id);
CREATE INDEX pos_vid_date ON rt_vehicle_positions (trip_start_date, vehicle_id);

-- call time is a timestampz, will be passed into the db as a UTC datetime
CREATE TABLE IF NOT EXISTS calls (
  vehicle_id text not null,
  call_time timestamp with time zone not null,
  trip_id text not null,
  route_id text,
  direction_id integer,
  stop_id text,
  source text,
  deviation interval,
  CONSTRAINT calls_pkey PRIMARY KEY (vehicle_id, call_time)
);
CREATE INDEX calls_rds_index ON calls (route_id, direction_id, stop_id);
