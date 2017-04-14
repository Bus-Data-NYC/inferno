shell = bash

MYSQLFLAGS =
DATABASE = nycbus
MYSQL = mysql $(DATABASE) $(MYSQLFLAGS)

TABLE = calls
CLIENTS = client

months = 01 02 03 04 05 06 07 08 09 10 11 12

.PHONY: load-% calls-day-% calls-2016-% init

.SECONDEXPANSION:

$(addprefix calls-2016-,$(months)): calls-2016-%:
	$(MAKE) $(addprefix calls-day-2016-$*-,$(shell cal $* 2016 | xargs | awk '{print $$NF}' | xargs seq -w 1))

calls-day-%:
	python3 src/imputecalls.py $(CLIENTS) $(TABLE) $*

load-%: sql/generate_ref.sql
	{ echo 'SET @feed_index = $*;' ; cat $< ; } | \
	$(MYSQL)

init: sql/bus_db_schema.sql 
	$(MYSQL) < $<
