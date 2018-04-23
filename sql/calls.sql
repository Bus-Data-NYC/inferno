/*
Copyright 2017-18 TransitCenter http://transitcenter.org

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

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
  run_index bigint,
  deviation interval,
  call_time timestamp with time zone not null,
  stop_id text,
  vehicle_id text not null,
  direction_id integer,
  date date,
  feed_index int,
  source char(1),
  CONSTRAINT calls_pkey PRIMARY KEY (vehicle_id, call_time)
);
CREATE SEQUENCE IF NOT EXISTS run_index CACHE 3 NO CYCLE OWNED BY calls.run_index;

CREATE INDEX calls_run ON calls (run_index);
