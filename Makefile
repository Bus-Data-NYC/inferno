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
SHELL = /bin/bash

python = python3
psql = psql

CALLS     ?= inferno.calls
POSITIONS ?= rt.vehicle_positions

months = 01 02 03 04 05 06 07 08 09 10 11 12

years = 2017 2018 2019 2020 2021

.PHONY: all calls-day-% calls-% init load-test clear-test

all:

$(foreach y,$(years),$(addprefix calls-$(y)-,$(months))): calls-%:
	$(MAKE) calls-day-$*-{01..$(shell date -d "$*-1 + 1 month - 1 day" "+%d")}

calls-day-%:
	./src/inferno.py $* --calls $(CALLS) --positions $(POSITIONS) $(INFERNOFLAGS)

test: ; coverage run src/test.py

load-test:
	psql -f src/test_data/schema.sql
	psql -c "\copy rt.vehicle_positions \
		(timestamp, vehicle_id, latitude, longitude, trip_start_date, trip_id, stop_id, dist_along_route, dist_from_stop) \
		from 'src/test_data/positions.csv' (format csv, header on)"
	psql -c "\copy gtfs.stop_times \
		(feed_index, trip_id, arrival_time, departure_time, stop_id, stop_sequence, pickup_type, drop_off_type, shape_dist_traveled) \
		from 'src/test_data/stop_times.csv' (format csv, header on)"
	psql -f src/test_data/data.sql

init:
	$(psql) -f sql/calls.sql
	$(python) -m pip install -r requirements.txt
