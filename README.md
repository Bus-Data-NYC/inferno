# Inferno

Transit call inferrer.

## Requirements
* Python 3.5+
* PostGreSQL

## Setup

Set [PG* environment variables](https://www.postgresql.org/docs/current/static/libpq-envars.html):
```
PGHOST=host
PGDATABASE=db
PGUSER=name
export PGHOST PGDATABASE PGUSER
```

## Quickstart

* Download or scrape position data for at least a calendar day (e.g. with .
* Download GTFS schedule data and load into Postgres with [gtfs-sql-importer](https://github.com/fitnr/gtfs-sql-importer).
* Prepare a Postgres database for Inferno with the command `make init`.
* Run `make calls-day-YYYY-MM-DD`.

## Methodology

The goal of this script is to use bus position data to infer when buses made calls at stops as described in a GTFS feed. The call time at each stop is interpolated using the time and distance traveled of each position. If necessary, the call time is extrapolated at the start and end of the route.

The script runs for a single day of positions at a time. Here's a step-by-step walkthrough of the algorithm:

* Find all the vehicles that begin trips on a particular date.
* For each vehicle, fetch the positions. Snap each position to the route, and calculate a distance along the route. Partition the positions by trip, discard those that would show buses traveling backwards. The set of positions that match a single trip is called a run.
* For each run, fetch the stop positions for the scheduled run. Then impute the time that the bus reached the stops based on the observed times and distances. This assumes that each bus stop is a single point, and that the bus moves at a constant speed between each observation.
* If stops at either end of the scheduled trip are before (or after) the observations, preform a linear extrapolation using the first few (or last few) observations.
* Save the positions to the `calls` table.

### Data issues

* Positions at the beginning and the end of routes are frequently unavailable.
* Fields `bearing`, `dist_along_route` and `dist_from_stop` are unreliable. They're basically only useful as rough estimations. The latter two fields aren't available for GTFS-RT data.
* Bus changes `trip_id` for a number of stops in the middle of a run. Currently not dealt with effectively, the same bus may be inferred to have made two simultaneous runs.
* A `trip_id` occurs twice in the same day. The `run_index` field is designed to cope with this.

## License

Copyright 2017-18 TransitCenter. Made available under the Apache 2.0 license.
