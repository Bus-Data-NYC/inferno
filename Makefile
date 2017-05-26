shell = bash

PYTHON = python3.5

PSQLFLAGS =
DATABASE = nycbus
PSQL = psql $(DATABASE) $(PSQLFLAGS)

TABLE = calls

months = 01 02 03 04 05 06 07 08 09 10 11 12

years = 2015 2016 2017

.PHONY: all load-% calls-day-% calls-2016-% calls-2017-% init

all:

$(foreach y,$(years),$(addprefix calls-$(y)-,$(months))): calls-%:
	$(MAKE) calls-day-$*-{01..$(shell date -d "$*-1 + 1 month - 1 day" "+%d")}

calls-day-%:
	$(PYTHON) src/inferno.py "dbname=$(DATABASE) $(PSQLFLAGS)" $* --table $(TABLE)

test:
	$(PYTHON) -m coverage run src/test.py

init:
	$(PSQL) -f sql/calls.sql
	$(PYTHON) -m pip install -r requirements.txt
