DROP TABLE IF EXISTS positions;
DROP TABLE IF EXISTS gtfs_trips cascade;
DROP TABLE IF EXISTS gtfs_stop_times cascade;
DROP TABLE IF EXISTS gtfs_stop_distances_along_shape;

BEGIN;

CREATE TABLE positions (
    timestamp_utc TIMESTAMP WITH TIME ZONE NOT NULL,
    vehicle_id TEXT NOT NULL,
    latitude NUMERIC(8, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    bearing NUMERIC(5, 2),
    progress INTEGER,
    service_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    block TEXT,
    stop_id TEXT,
    dist_along_route NUMERIC(8, 2),
    dist_from_stop NUMERIC(8, 2),
    CONSTRAINT position_time_bus PRIMARY KEY (timestamp_utc, vehicle_id)
);
CREATE INDEX pos_vid ON positions (vehicle_id);
CREATE INDEX pos_sdate ON positions (service_date);
CREATE INDEX pos_trip_id ON positions (trip_id);
CREATE INDEX pos_time ON positions (timestamp_utc);

CREATE TABLE gtfs_trips (
  feed_index int not null,
  route_id text not null,
  service_id text not null,
  trip_id text not null,
  trip_headsign text,
  direction_id int,
  block_id text,
  shape_id text,
  trip_short_name text,
  wheelchair_accessible int,
  CONSTRAINT gtfs_trips_pkey PRIMARY KEY (feed_index, trip_id)
);
CREATE INDEX gtfs_trips_trip_id ON gtfs_trips (trip_id);
CREATE INDEX gtfs_trips_service_id ON gtfs_trips (feed_index, service_id);

CREATE TABLE gtfs_stop_times (
  feed_index int not null,
  trip_id text not null,
  -- Check that casting to time interval works.
  arrival_time interval CHECK (arrival_time::interval = arrival_time::interval),
  departure_time interval CHECK (departure_time::interval = departure_time::interval),
  stop_id text,
  stop_sequence int not null,
  stop_headsign text,
  pickup_type int,
  drop_off_type int,
  shape_dist_traveled double precision,
  timepoint int,
  CONSTRAINT gtfs_stop_times_pkey PRIMARY KEY (feed_index, trip_id, stop_sequence)
);
CREATE INDEX gtfs_stop_times_key ON gtfs_stop_times (trip_id, stop_id);

CREATE TABLE gtfs_stop_distances_along_shape (
  feed_index integer not null,
  shape_id text,
  stop_id text,
  pct_along_shape numeric,
  dist_along_shape numeric
);
CREATE INDEX gtfs_stop_dist_along_shape_index ON gtfs_stop_distances_along_shape
  (feed_index, shape_id);

COMMIT;
