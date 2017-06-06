shell = bash

PYTHON = python3.5

PSQLFLAGS =
DATABASE = nycbus
PSQL = psql $(DATABASE) $(PSQLFLAGS)

TABLE = calls

months = 01 02 03 04 05 06 07 08 09 10 11 12

years = 2015 2016 2017

.PHONY: all calls-day-% calls-% init load-test clear-test

all:

$(foreach y,$(years),$(addprefix calls-$(y)-,$(months))): calls-%:
	$(MAKE) calls-day-$*-{01..$(shell date -d "$*-1 + 1 month - 1 day" "+%d")}

calls-day-%:
	$(PYTHON) src/inferno.py "dbname=$(DATABASE) $(PSQLFLAGS)" $* --table $(TABLE)

test: | clean-test load-test
	$(PYTHON) -m coverage run src/test.py -q

load-test:
	psql inferno -f src/test_data/positions.sql
	psql inferno -f src/test_data/trips.sql
	psql inferno -f src/test_data/shape_geoms.sql
	psql inferno -f src/test_data/stop_times.sql

clean-test:
	psql inferno -c "truncate calls, positions, gtfs_trips, gtfs_stop_times, \
		gtfs_calendar, gtfs_feed_info, gtfs_agency, gtfs_shape_geoms cascade;"

init:
	$(PSQL) -f sql/calls.sql
	$(PYTHON) -m pip install -r requirements.txt
