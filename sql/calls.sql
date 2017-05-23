CREATE TABLE IF NOT EXISTS calls (
  vehicle_id text not null,
  call_time timestamp without time zone not null,
  trip_id text not null,
  route_id text,
  direction_id text,
  stop_id text,
  source text,
  CONSTRAINT calls_pkey PRIMARY KEY (vehicle_id, call_time)
);
