# How to run mytransit from scratch (on Ubuntu 16 with GCC, MySQL, and Python2.7 installed)

For this example, we'll be inferring arrival/departure times for 2016-05-25.

I have `.my.cnf` set with a default username and password - content of ~/.my.cnf:

    [client]
    user=YOUR_DB_USERNAME
    password=YOUR_DB_PASSWORD

If you prefer to pass MySQL authentication on the command line (less secure), use:

    make task MYSQLFLAGS="-u username -ppassword"

MySQL should have timezone tables loaded; e.g.:

    mysql_tzinfo_to_sql /usr/share/zoneinfo | mysql -u root mysql

# Generate reference tables

    make init

Load data for a particular feed_index (N):

    make load-N

TODO: use triggers to automate this.

# Check for missing records

If "records_with_valid_trip" is ever less than "records", the difference is Bus Time positions with trip_ids not in any GTFS, which are ignored. In the past, this has been exceptionally rare, but these numbers should be monitored for any significant differences (trip_ids could be manually corrected - in the cases I've seen, replacing a particular hyphen in a trip_id with an underscore fixes the problem).

    cd ../performance

Turn the `ref_stop_times` table into binary data for the calls-inferer. `make_stop_times_bin.sh` uses `make_stop_times_tsv.sql`, which explicitly sets the file paths where the contents of the database tables are initially dumped - this works with latest MySQL on Ubuntu 16, but file paths may need to be changed depending on version/permissions.

    ./make_stop_times_bin.sh DB_NAME

Some constants need to be set in `mytransit.c` manually before compiling - I never got around to having it properly ascertain these values. Set DB_NAME, DB_USER, and DB_PASS to their respective values. Set `MAX_TRIP_INDEX` to the value you get from `select max(trip_index) from ref_trips;` [178272] and set `STOP_TIMES_SIZE` to the value you get from `select count(1) from ref_stop_times;` [6994433].

To compile, you may need `libmysqlclient` installed (e.g, `sudo apt install libmysqlclient-dev -y`).

`gcc mytransit.c -o mytransit -std=c99 -O3 $(mysql_config --cflags --libs)`

Now to infer calls and put them in the database. We'll need to have a `calls` table. I had the following definition (in same DB_NAME database):
````mysql
CREATE TABLE calls (
  vehicle_id smallint not null,
  trip_index int not null,
  stop_sequence tinyint unsigned not null,
  call_time datetime not null,
  dwell_time smallint not null,
  source char(1) not null,
  rds_index smallint unsigned not null,
  deviation smallint not null,
  PRIMARY KEY (vehicle_id,call_time,stop_sequence),
  KEY rds (rds_index,call_time),
  KEY call_time (call_time)
) ENGINE=MyISAM DEFAULT CHARSET=latin1
PARTITION BY HASH (12*YEAR(call_time)+MONTH(call_time)) PARTITIONS 60; # I believe this partition engine is deprecated now, but should still work
````

`date_range_calls.sh` has the DB_NAME hardcoded in, so be sure to change that before continuing. Note that it will actually look for Bus Time entries from a few hours before the start of the specified date (to ensure overlap so everything is captured).

Finally, let's infer the "calls":
    sh ./date_range_calls.sh 2016-05-25  # this is a UTC date

(`date_range_calls.sh` also accepts an extra argument to indicate the number of additional dates to infer calls for.)

The calls table should now be populated with arrival/departure time estimates!
