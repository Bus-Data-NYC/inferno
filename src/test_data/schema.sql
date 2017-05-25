DROP TABLE IF EXISTS gtfs_trips cascade;
DROP TABLE IF EXISTS gtfs_stop_times cascade;
DROP TABLE IF EXISTS gtfs_stop_distances_along_shape;

BEGIN;

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
  timepoint int REFERENCES gtfs_timepoints (timepoint),

  -- unofficial features
  -- the following are not in the spec
  continuous_drop_off int default null,
  continuous_pickup  int default null,
  arrival_time_seconds int default null,
  departure_time_seconds int default null,
  CONSTRAINT gtfs_stop_times_pkey PRIMARY KEY (feed_index, trip_id, stop_sequence)
);
CREATE INDEX gtfs_stop_times_key ON gtfs_stop_times (trip_id, stop_id);
CREATE INDEX arr_time_index ON gtfs_stop_times (arrival_time_seconds);
CREATE INDEX dep_time_index ON gtfs_stop_times (departure_time_seconds);

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
