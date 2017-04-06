shell = bash

MYSQLFLAGS =
DATABASE = nycbus
MYSQL = mysql $(DATABASE) $(MYSQLFLAGS)

.PHONY: load-% init

load-%: sql/generate_ref.sql
	{ echo 'SET @feed_index = $*;' ; cat $(word 2,$^) ; } | \
	$(MYSQL)

init: sql/bus_db_schema.sql 
	$(MYSQL) < $<
