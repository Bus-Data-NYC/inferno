#!/user/bin/env python
import sys
import os.path
from collections import Counter
from itertools import groupby, izip, tee
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

# when dist_from_stop < 3048 cm (100 feet) considered "at stop" by MTA --NJ
# this is not correct! It's only that the sign displays "at stop"
# beginning at 100 ft
STOP_THRESHOLD = 30.48

VEHICLE_QUERY = """SELECT
    CONVERT_TZ(p.timestamp_utc, 'UTC', 'EST') AS datetime,
    progress,
    rt.trip_index trip,
    next_stop_id next_stop,
    dist_from_stop,
    pattern_id pattern,
    dist_along_route

FROM positions p
    INNER JOIN ref_trips rt ON (rt.trip_id = p.trip_id)
    INNER JOIN ref_trip_patterns tp ON (rt.trip_index = tp.trip_index)
    LEFT JOIN ref_stop_times st ON (
        rt.trip_index = st.trip_index
        AND p.next_stop_id = st.stop_id
    )
WHERE vehicle_id = %s
    AND service_date = %s
ORDER BY timestamp_utc
"""

INSERT = """INSERT INTO calls
    (vehicle_id, trip_index, stop_sequence, call_time, source, rds_index, deviation)
    VALUES ({}, {}, %s, %s, %s, {}, 555)"""

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


def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)


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


def filter_positions(cursor, vehicle_id, date):
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
    cursor.execute(VEHICLE_QUERY, (vehicle_id, date))

    runs = []
    prev = {}
    position = cursor.fetchone()

    while position is not None:
        position['departure'] = position['datetime']

        # If patterns differ, stop sequence goes down, or half an hour passed
        if (position['pattern'] != prev.get('pattern') or
                position['stop_sequence'] < prev.get('stop_sequence') or
                position['datetime'] > prev.get('datetime') + MAX_TIME_BETWEEN_STOPS):

            # start a new run
            runs.append([])

            # add last position to previous run
            if len(prev) and len(runs) > 1 and runs[-2][-1] != prev:
                runs[-2].append(prev)

            # add current position to the new run
            runs[-1].append(position)

        # if we're approaching a stop
        elif (position['dist_from_stop'] is not None and position['dist_from_stop'] < STOP_THRESHOLD):
            # if we were approaching the same stop last time, remove that one
            if (position['next_stop'] == runs[-1][-1].get('next_stop') and
                    position['dist_from_stop'] < runs[-1][-1].get('dist_from_stop')):
                runs[-1].pop()

            # append the current stop
            runs[-1].append(position)

        # If the stop changed
        elif position['next_stop'] != prev.get('next_stop'):
            # If we didn't capture the previous stop, do so now
            if runs[-1][-1] != prev:
                runs[-1].append(prev)

            runs[-1].append(position)

        # If the bus didn't move, update departure time
        if (position['dist_from_stop'] == prev.get('dist_from_stop') and
                position['next_stop'] == prev.get('next_stop')):
            prev['departure'] = position['departure']

        prev = position
        position = cursor.fetchone()

    return [enumerate(run) for run in runs]


def fetch_vehicles(cursor, date):
    cursor.execute("SELECT DISTINCT vehicle_id FROM positions WHERE service_date = %s", (date,))
    return [row['vehicle_id'] for row in cursor.fetchall()]


def extrapolate_back(stops, positions):
    return 1


def extrapolate_forward(stops, positions):
    return 1


def main(db_name, date):
    # connect to MySQL
    config = get_config()
    source = MySQLdb.connect(db=db_name, cursorclass=MySQLdb.cursors.DictCursor, **config)
    cursor = source.cursor()

    sink = MySQLdb.connect(db=db_name, **config)

    # Get distinct vehicles from MySQL
    vehicles = fetch_vehicles(cursor, date)

    # Run query for every vehicle
    for vehicle_id in vehicles:
        print(vehicle_id)
        runs = filter_positions(cursor, vehicle_id, date)

        # each run will become a trip
        for run in runs:
            # list of calls to be written
            # each call is a list of this format:
            # [stop_sequence, call_time, dwell_time, source]
            calls = []

            # get the scheduled list of trips for this run
            trip_index = Counter([x['trip'] for x in run]).most_common(1)
            cursor.execute(
                """SELECT stop_id id, rds_index, stop_sequence
                FROM ref_stop_times WHERE trip_index = %s""",
                (trip_index,)
            )
            trip_stops = cursor.fetchall()

            call_time = extrapolate_back(trip_stops, run)
            calls.append([trip_stops[0]['stop_sequence'], call_time, -2, 'S'])

            # pairwise iteration: scheduled stoptime and next scheduled stoptime
            for stoptime, next_stoptime in pairwise(trip_stops[1:]):

                try:
                    i, last_before = [
                        (i, p) for i, p in run if p['stop_sequence'] <= stoptime['stop_sequence']
                    ].pop()

                    _, first_after = run[1 + i]

                except IndexError:
                    continue

                # got positions that are on either side of this guy
                if (last_before['next_stop'] == stoptime['stop_index'] and
                        first_after['next_stop'] == next_stoptime['stop_index']):

                    method = 'C'
                    elapsed = first_after['arrival'] - last_before['departure']
                    call_time = last_before['departure'] + elapsed / 2

                # if there aren't any, we'll interpolate between surrounding stops
                else:
                    method = 'I'
                    call_time = None

                calls.append([stoptime['stop_sequence'], call_time, method])

            # use groupby to work on groups that need interpolating
            for m, grouper in groupby(calls, lambda x: x[2]):
                if m != 'I':
                    continue

                group = list(grouper)

            # TODO extrapolate to end of run
            call_time = extrapolate_forward(trip_stops, run)
            calls.append([trip_stops[-1]['stop_sequence'], call_time, -1, 'E'])

            # write calls to sink
            insert = INSERT.format(vehicle_id, trip_index, trip_stops[0]['rds_index'])
            sink.cursor().executemany(insert, calls)
            sink.cursor().commit()


if __name__ == '__main__':
    main(*sys.argv[1:])
