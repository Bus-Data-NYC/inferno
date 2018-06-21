DROP TABLE IF EXISTS rt_vehicle_positions;

CREATE TABLE rt_vehicle_positions (
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    vehicle_id TEXT NOT NULL,
    latitude NUMERIC(8, 6) NOT NULL,
    longitude NUMERIC(9, 6) NOT NULL,
    bearing NUMERIC(5, 2),
    progress INTEGER,
    trip_start_date DATE NOT NULL,
    trip_id TEXT NOT NULL,
    block TEXT,
    stop_id TEXT,
    dist_along_route NUMERIC(8, 2),
    dist_from_stop NUMERIC(8, 2),
    CONSTRAINT position_time_bus PRIMARY KEY (timestamp, vehicle_id)
);

CREATE SCHEMA gtfs;

CREATE TABLE gtfs.feed_info (
  feed_index serial PRIMARY KEY, -- tracks uploads, avoids key collisions
  feed_publisher_name text default null,
  feed_publisher_url text default null,
  feed_timezone text default null,
  feed_lang text default null,
  feed_version text default null,
  feed_start_date date default null,
  feed_end_date date default null,
  feed_id text default null,
  feed_contact_url text default null,
  feed_download_date date,
  feed_file text
);
CREATE TABLE gtfs.agency (
  feed_index integer REFERENCES gtfs.feed_info (feed_index),
  agency_id text default '',
  agency_name text default null,
  agency_url text default null,
  agency_timezone text default null,
  -- optional
  agency_lang text default null,
  agency_phone text default null,
  agency_fare_url text default null,
  agency_email text default null,
  bikes_policy_url text default null,
  CONSTRAINT gtfs_agency_pkey PRIMARY KEY (feed_index, agency_id)
);

CREATE TABLE gtfs.calendar (
  feed_index integer not null,
  service_id text,
  monday int not null,
  tuesday int not null,
  wednesday int not null,
  thursday int not null,
  friday int not null,
  saturday int not null,
  sunday int not null,
  start_date date not null,
  end_date date not null,
  CONSTRAINT gtfs_calendar_pkey PRIMARY KEY (feed_index, service_id),
  CONSTRAINT gtfs_calendar_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);
CREATE INDEX gtfs_calendar_service_id ON gtfs.calendar (service_id);

CREATE TABLE gtfs.trips (
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

  CONSTRAINT gtfs_trips_pkey PRIMARY KEY (feed_index, trip_id),
  CONSTRAINT gtfs_trips_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);

CREATE INDEX gtfs_trips_trip_id ON gtfs.trips (trip_id);
CREATE INDEX gtfs_trips_service_id ON gtfs.trips (feed_index, service_id);

CREATE TABLE gtfs.stop_times (
  feed_index int not null,
  trip_id text not null,
  arrival_time interval CHECK (arrival_time::interval = arrival_time::interval),
  departure_time interval CHECK (departure_time::interval = departure_time::interval),
  stop_id text,
  stop_sequence int not null,
  stop_headsign text,
  pickup_type int,
  drop_off_type int,
  shape_dist_traveled numeric(10, 2),
  timepoint int,
  CONSTRAINT gtfs_stop_times_pkey PRIMARY KEY (feed_index, trip_id, stop_sequence),
  CONSTRAINT gtfs_stop_times_feed_fkey FOREIGN KEY (feed_index)
    REFERENCES gtfs.feed_info (feed_index) ON DELETE CASCADE
);
CREATE INDEX gtfs_stop_times_key ON gtfs.stop_times (feed_index, trip_id, stop_id);

CREATE TABLE gtfs.stops (
  feed_index int not null,
  stop_id text,
  stop_name text default null,
  stop_desc text default null,
  stop_lat double precision,
  stop_lon double precision,
  zone_id text,
  stop_url text,
  stop_code text,
  stop_street text,
  stop_city text,
  stop_region text,
  stop_postcode text,
  stop_country text,
  stop_timezone text,
  direction text,
  position text default null,
  parent_station text default null,
  wheelchair_boarding integer default null,
  wheelchair_accessible integer default null,
  CONSTRAINT gtfs_stops_pkey PRIMARY KEY (feed_index, stop_id)
);
SELECT AddGeometryColumn('gtfs', 'stops', 'the_geom', 4326, 'POINT', 2);

CREATE TABLE gtfs.shape_geoms (
  feed_index int not null,
  shape_id text not null,
  length numeric(12, 2) not null,
  CONSTRAINT gtfs_shape_geom_pkey PRIMARY KEY (feed_index, shape_id)
);
-- Add the_geom column to the gtfs.shape_geoms table - a 2D linestring geometry
SELECT AddGeometryColumn('gtfs', 'shape_geoms', 'the_geom', 4326, 'LINESTRING', 2);
