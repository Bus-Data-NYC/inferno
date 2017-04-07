shell = bash

MYSQLFLAGS =
DATABASE = nycbus
MYSQL = mysql $(DATABASE) $(MYSQLFLAGS)

.PHONY: load-% calls_% init

calls_%: ; python src/imputecalls.py $(DATABASE) $*

load-%: sql/generate_ref.sql
	{ echo 'SET @feed_index = $*;' ; cat $< ; } | \
	$(MYSQL)

init: sql/bus_db_schema.sql 
	$(MYSQL) < $<
