shell = bash

PSQLFLAGS =
DATABASE = nycbus
PSQL = psql $(DATABASE) $(PSQLFLAGS)

TABLE = calls
CLIENTS = client

months = 01 02 03 04 05 06 07 08 09 10 11 12

.PHONY: load-% calls-day-% calls-2016-% calls-2017-% init

.SECONDEXPANSION:

$(addprefix calls-2016-,$(months)): calls-2016-%:
	$(MAKE) $(addprefix calls-day-2016-$*-,$(shell cal $* 2016 | \
		xargs | awk '{print $$NF}' | xargs -n 1 /usr/bin/seq -w 1))

calls-day-%:
	python3 src/imputecalls.py "dbname=$(DATABASE);$(PSQLFLAGS)" $(TABLE) $*

init: sql/calls.sql
	$(PSQL) < $<
