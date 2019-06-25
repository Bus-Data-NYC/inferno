# Copyright 2017, 2018, 2019 TransitCenter http://transitcenter.org

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

shell = bash

PYTHON = python3.5

PGUSER ?= $(USER)
PGDATABASE ?= $(PGUSER)
PSQLFLAGS = $(PGDATABASE)
PSQL = psql $(PSQLFLAGS)

export PGUSER PGDATABASE

CALLS = calls
POSITIONS = rt_vehicle_positions

months = 01 02 03 04 05 06 07 08 09 10 11 12

years = 2017 2018

.PHONY: all calls-day-% calls-% init load-test clear-test

all:

$(foreach y,$(years),$(addprefix calls-$(y)-,$(months))): calls-%:
	$(MAKE) calls-day-$*-{01..$(shell date -d "$*-1 + 1 month - 1 day" "+%d")}

calls-day-%:
	$(PYTHON) src/inferno.py $* --calls-table $(CALLS) --positions-table $(POSITIONS) $(INFERNOFLAGS)

test: | clean-test load-test
	$(PYTHON) -m coverage run src/test.py -q

load-test:
	psql inferno -f sql/calls.sql
	psql inferno -f src/test_data/schema.sql
	psql inferno -c "\copy rt_vehicle_positions \
		(timestamp,vehicle_id,latitude,longitude,trip_start_date,trip_id,stop_id,dist_along_route,dist_from_stop) \
		FROM 'src/test_data/positions.csv' WITH (FORMAT CSV, HEADER TRUE)"
	psql inferno -f src/test_data/trips.sql
	psql inferno -f src/test_data/shape_geoms.sql
	psql inferno -c "\copy gtfs.stop_times \
		(feed_index,trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type,shape_dist_traveled) \
		FROM 'src/test_data/stop_times.csv' WITH (FORMAT CSV, HEADER TRUE)"

clean-test:
	-psql inferno -c "drop table calls"
	-psql inferno -c 'drop schema gtfs cascade'

init:
	$(PSQL) -f sql/calls.sql
	$(PYTHON) -m pip install -r requirements.txt
