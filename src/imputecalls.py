#!/user/bin/env python
import sys
import os.path
from collections import Counter
import MySQLdb
import MySQLdb.cursors
try:
    import configparser
except ImportError:
    from six.moves import configparser

'''
Goal: from bustime positions, impute stop calls. Each output row should contain:
vehicle_id, trip_index, stop_sequence, arrival_time, departure_time, source (??).
'''

# Maximum elapsed time between positions before we declare a new run
MAX_TIME_BETWEEN_STOPS = 60 * 30

# when dist_from_stop < 3048 cm (100 feet) considered "at stop" by MTA
STOP_THRESHOLD = 3048
MAX_POSITIONS = 100 * 60 * 2
MAX_CALLS = 100 * 60 * 2
TRIP_INDEX_OFFSET = 0
MAX_TRIP_INDEX = 178272  # TODO set dynamically
STOP_TIMES_SIZE = 6994433  # TODO set dynamically

VEHICLE_QUERY = """SELECT
    UNIX_TIMESTAMP(timestamp_utc) timestamp,
    progress,
    rt.trip_index trip,
    next_stop_id next_stop,
    round(100 * dist_along_route) dist_along_route,
    round(100 * dist_from_stop) dist_from_stop,
    pattern_id pattern,
    coalesce(stop_sequence, -1) stop_sequence
FROM positions p
    INNER JOIN ref_trips rt ON (rt.trip_id = p.trip_id)
    INNER JOIN ref_trip_patterns tp ON (rt.trip_index = tp.trip_index)
    LEFT JOIN ref_stop_times st ON (
        rt.trip_index = st.trip_index
        AND p.next_stop_id = st.stop_id
    )
WHERE
    vehicle_id = %s
    AND p.timestamp_utc BETWEEN %s AND %s
ORDER BY timestamp_utc
"""

INSERT = """INSERT INTO calls
    (vehicle_id, trip_index, stop_sequence, call_time, dwell_time, source, rds_index)
    VALUES ({}, {}, %s, %s, %s, %s, {})"""

'''
+------------+----------+-------+--------------+------------------+----------------+---------+----------+
| timestamp  | progress | trip  | next_stop_id | dist_along_route | dist_from_stop | pattern | stop_seq |
+------------+----------+-------+--------------+------------------+----------------+---------+----------+
| 1477973306 | 0        | 85484 | 802131       | 878732           | 2243           | 437     | 29       |
| 1477973368 | 0        | 85614 | 102793       | 644              | 564            | 438     | 1        |
| 1477973400 | 0        | 85614 | 102795       | 49898            | 48183          | 438     | 2        |
| 1477973556 | 0        | 85614 | 102795       | 49898            | 5354           | 438     | 2        |
| 1477973588 | 0        | 85614 | 102796       | 118244           | 60712          | 438     | 3        |
| 1477973651 | 0        | 85614 | 102796       | 118244           | 727            | 438     | 3        |
| 1477973713 | 0        | 85614 | 102797       | 145640           | 2523           | 438     | 4        |
| 1477973744 | 0        | 85614 | 102797       | 145640           | 721            | 438     | 4        |
| 1477973806 | 0        | 85614 | 102799       | 196479           | 20410          | 438     | 6        |
+------------+----------+-------+--------------+------------------+----------------+---------+----------+
'''
'''
output fields:
    vehicle_id
    trip_index
    stop_sequence
    call_time
    dwell_time
    source
    rds_index
    deviation
'''


def get_config(filename=None):
    filename = os.path.expanduser(filename or "~/.my.cnf")
    cp = configparser.ConfigParser()
    with open(filename) as f:
        try:
            cp.read_file(f)
        except AttributeError:
            cp.readfp(f)

        if cp.has_section('client'):
            return {
                "host": cp.get('client', 'host', fallback='localhost'),
                "passwd": cp.get('client', 'password'),
                "port": cp.get('client', 'port', fallback=3306),
                "user": cp.get('client', 'user'),
            }
        else:
            return {}


def filter_positions(cursor, vehicle_id, start, end):
    '''
    Compile list of positions for a vehicle, using a list of positions
    and filtering based on positions that reflect change in pattern or next_stop.
    Generates a list of preliminary information:
        vehicle
        trip index
        stop sequence
        arrival min
        arrival max
        departure min
        departure max
    '''
    # load up cursor with every position for vehicle
    cursor.execute(VEHICLE_QUERY, (vehicle_id, start, end))

    runs = []
    prev = {}
    position = cursor.fetchone()

    while position is not None:
        if (position['pattern'] != prev.get('pattern') or
                position['stop_sequence'] < prev.get('stop_sequence') or
                position['timestamp'] > prev.get('timestamp', 0) + MAX_TIME_BETWEEN_STOPS):

            runs.append([])

            # last position on previous run
            if len(prev) and len(runs) > 1 and runs[-2][-1] != prev:
                runs[-2].append(prev)

            # first position on the new run
            runs[-1].append(position)

        # if we're approaching a stop
        elif (position['dist_from_stop'] and position['dist_from_stop'] < STOP_THRESHOLD):
            # if we were approaching the same stop last time, remove that one
            if (position['next_stop'] == runs[-1][-1].get('next_stop') and
                    position['dist_from_stop'] < runs[-1][-1].get('dist_from_stop')):
                runs[-1].pop()

            # append the current stop
            runs[-1].append(position)

        elif position['next_stop'] != prev.get('next_stop'):
            runs[-1].append(position)

        prev = position
        position = cursor.fetchone()

    print((runs))

    return runs


def fetch_vehicles(cursor, start, end):
    cursor.execute(
        "SELECT DISTINCT vehicle_id FROM positions WHERE timestamp_utc BETWEEN %s AND %s",
        (start, end)
    )
    return [row['vehicle_id'] for row in cursor.fetchall()]


def main(db_name, start_date, end_date):
    # connect to MySQL
    config = get_config()
    source = MySQLdb.connect(db=db_name, cursorclass=MySQLdb.cursors.DictCursor, **config)
    cursor = source.cursor()

    sink = MySQLdb.connect(db=db_name, **config)

    # Get distinct vehicles from MySQL
    vehicles = fetch_vehicles(cursor, start_date, end_date)

    # Run query for every vehicle
    for vehicle_id in vehicles:
        print(vehicle_id)
        runs = filter_positions(cursor, vehicle_id, start_date, end_date)

        # each run will become a trip
        for run in runs:
            # list of calls to be written
            # each call is a list of this format:
            # [stop_sequence, call_time, dwell_time, source]
            calls = []

            # get the scheduled list of trips for this run
            trip_index = Counter([x['trip'] for x in run]).most_common(1)
            cursor.execute("SELECT * FROM ref_stop_times where trip_index = %s", (trip_index,))
            trip_stops = cursor.fetchall()

            # TODO extrapolate back to start of run
            call_time = None
            calls.append([trip_stops[0]['stop_sequence'], call_time, -2, 'S'])

            for sched_stop in trip_stops[1:-1]:
                # TODO 
                if None:
                    # get 2 positions that are on either side of this guy
                    method = 'C'
                    dwell_time = None
                    call_time = None

                # TODO
                else:
                    # if there aren't any, interpolate between surrounding stops
                    method = 'I'
                    dwell_time = None
                    call_time = None

                calls.append([sched_stop['stop_sequence'], call_time, dwell_time, method])

            # TODO extrapolate to end of run
            call_time = None
            calls.append([trip_stops[-1]['stop_sequence'], call_time, -1, 'E'])

            # write calls to sink
            insert = INSERT.format(vehicle_id, trip_index, trip_stops[0]['rds_index'])
            sink.cursor().executemany(insert, calls)
            sink.cursor().commit()


if __name__ == '__main__':
    main(*sys.argv[1:])
