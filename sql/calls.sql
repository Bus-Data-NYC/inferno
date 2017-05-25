-- call time is a timestampz, will be passed into the db as a UTC datetime
CREATE TABLE IF NOT EXISTS calls (
  vehicle_id text not null,
  call_time timestamp with time zone not null,
  trip_id text not null,
  route_id text,
  direction_id text,
  stop_id text,
  source text,
  CONSTRAINT calls_pkey PRIMARY KEY (vehicle_id, call_time)
);
