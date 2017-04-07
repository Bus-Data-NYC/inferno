#!/user/bin/env python
from __future__ import division
import sys
import os.path
from datetime import timedelta
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
MAX_TIME_BETWEEN_STOPS = timedelta(seconds=60 * 30)

# when dist_from_stop < 3048 cm (100 feet) considered "at stop" by MTA --NJ
# this is not correct! It's only that the sign displays "at stop"
# beginning at 100 ft
STOP_THRESHOLD = 30.48

# Purposefully ignoring daylight savings for now
VEHICLE_QUERY = """SELECT
    CONVERT_TZ(p.timestamp_utc, 'UTC', 'EST') AS arrival,
    st.arrival_time scheduled_time,
    rt.trip_index trip,
    next_stop_id next_stop,
    dist_from_stop,
    pattern_id pattern,
    dist_along_route,
    stop_sequence
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
    (vehicle_id, trip_index, rds_index, stop_sequence, call_time, source, deviation)
    VALUES ({}, {}, %s, %s, %s, %s, 555)"""

DEFAULT_LOGIN = {
    'host': 'localhost',
    'port': 3306,
    'user': 'ec2-user'
}

def pairwise(iterable):
    "s -> (s0,s1), (s1,s2), (s2, s3), ..."
    a, b = tee(iterable)
    next(b, None)
    return izip(a, b)


def get_config(filename=None):
    filename = os.path.expanduser(filename or "~/.my.cnf")
    cp = configparser.ConfigParser(defaults=DEFAULT_LOGIN)
    with open(filename) as f:
        try:
            cp.read_file(f)
        except AttributeError:
            cp.readfp(f)

        if cp.has_section('client'):
            return {
                "host": cp.get('client', 'host'),
                "passwd": cp.get('client', 'password'),
                "port": cp.getint('client', 'port'),
                "user": cp.get('client', 'user'),
            }
        else:
            return {}


def common(lis):
    return Counter(lis).most_common(1)[0][0]


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
        position['departure'] = position['arrival']

        # If patterns differ, stop sequence goes down, or half an hour passed
        runchange = (position['pattern'] != prev.get('pattern') or
                           (position['stop_sequence'] or -2) < prev.get('stop_sequence', -1) or
                           position['arrival'] > prev.get('arrival') + MAX_TIME_BETWEEN_STOPS)

        if runchange:
            # start a new run
            runs.append([])

            # add last position to previous run
            # and information about current arrival
            if len(prev) and len(runs) > 1 and runs[-2][-1] != prev:
                prev['next'] = position
                runs[-2].append(prev)

            # add current position to the new run
            # and information about previous arrival
            position['previous'] = prev
            runs[-1].append(position)

        # if we're approaching a stop
        elif position['dist_from_stop'] is not None and position['dist_from_stop'] < STOP_THRESHOLD:
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
            prev['departure'] = position['arrival']

        prev = position
        position = cursor.fetchone()

    return [enumerate(run) for run in runs]


def fetch_vehicles(cursor, date):
    # TODO remove debugging
    cursor.execute("""SELECT distinct vehicle_id FROM positions
        WHERE service_date = %s and vehicle_id = 6677""", (date,))
    return [row['vehicle_id'] for row in cursor.fetchall()]


def get_last_before(sequence, stop_number):
    '''Get the last position in a sequence where stop_sequence is <= the stop_number'''
    # This will raise an IndexError if something goes wrong
    i, position = [(i, p) for i, p in sequence
                   if p['stop_sequence'] <= stop_number].pop()

    return i, position


def interpolate(stop_number, last_before, first_after):
    '''Interpolate the particular (missing) stop number between the two positions'''
    elapsed = (first_after['arrival'] - last_before['departure']).total_seconds()
    sched_elapsed = (first_after['scheduled_time'] - last_before['scheduled_time']).total_seconds()

    return last_before['departure'] + (sched_elapsed / elapsed) * (stop_number - last_before['stop_sequence'])


def generate_calls(run, stoptimes):
    '''
    list of calls to be written
    Args:
        run: list generated from enumerate(positions)
        stoptimes: list of scheduled stoptimes for this trip
    '''
    # each call is a list of this format:
    # [rds_index, stop_sequence, call_time, source]
    calls = []

    dictwriter = csv.DictWriter(sys.stderr, ['arrival', 'stop_sequence', 'next_stop', 'dist_from_stop'])

    # pairwise iteration: scheduled stoptime and next scheduled stoptime
    for stoptime, next_stoptime in pairwise(stoptimes):
        method = None

        try:
            i, last_before = get_last_before(run, stoptime['stop_sequence'])
        except IndexError:
            try:
                # use the prev position as a phantom zeroth call
                last_before = run[0]['prev']
                i = -1
                method = 'S'
            except AttributeError:
                continue

        try:
            _, first_after = run[i + 1]
        except IndexError:
            try:
                # use the next position as a phantom (n+1)th call
                first_after = run[-1]['next']
                method = 'E'
            except AttributeError:
                continue

        # got positions that are on either side of this guy
        if (last_before['next_stop'] == stoptime['id'] and
                first_after['next_stop'] == next_stoptime['id']):

            method = 'C'
            elapsed = first_after['arrival'] - last_before['departure']
            call_time = last_before['departure'] + elapsed / 2

        # if there aren't any, we'll interpolate between surrounding stops
        else:
            method = method or 'I'
            call_time = interpolate(stoptime['stop_sequence'], last_before, first_after)

        calls.append([stoptime['rds_index'], stoptime['stop_sequence'], call_time, method])

    recorded_stops = [c[1] for c in calls]
    for stoptime in stoptimes:
        if stoptime['stop_sequence'] in recorded_stops:
            continue

        # Otherwise, we must do more imputing!
        raise ValueError("need to do more imputing")

    # DEBUG
    dictwriter.writerows([[
        {
            'arrival': c['arrival'],
            'stop_sequence': c['stop_sequence'],
            'next_stop': c['next_stop'],
            'dist_from_stop': c['dist_from_stop']
            } for c in call] for call in calls])

    return calls

def main(db_name, date):
    # connect to MySQL
    config = get_config()
    source = MySQLdb.connect(db=db_name, cursorclass=MySQLdb.cursors.DictCursor, **config)
    cursor = source.cursor()

    sink = MySQLdb.connect(db=db_name, **config).cursor()

    # Get distinct vehicles from MySQL
    vehicles = fetch_vehicles(cursor, date)

    import csv
    writer = csv.writer(sys.stderr)

    # Run query for every vehicle (returns list in memory)
    for vehicle_id in vehicles:
        # returns list in memory
        runs = filter_positions(cursor, vehicle_id, date)

        # each run will become a trip
        for run in runs:
            run = list(run)

            # get the scheduled list of trips for this run
            trip_index = common([x[1]['trip'] for x in run])
            cursor.execute("""SELECT stop_id id, arrival_time AS time, rds_index, stop_sequence
                FROM ref_stop_times WHERE trip_index = %s""", (trip_index,))

            calls = generate_calls(run, cursor.fetchall())

            # write calls to sink
            # insert = INSERT.format(vehicle_id, trip_index)
            # sink.executemany(insert, calls)
            # sink.commit()


if __name__ == '__main__':
    main(*sys.argv[1:])
