#!/user/bin/env python3
from __future__ import division
import sys
import os
from datetime import datetime
from multiprocessing import Pool
import logging
from datetime import timedelta
from collections import Counter
from itertools import chain, compress, cycle
import numpy as np
import MySQLdb
import MySQLdb.cursors
import pytz

'''
Goal: from bustime positions, impute stop calls. Each output row should contain:
vehicle_id, trip_index, stop_sequence, arrival_time, departure_time, source (??).
'''

logger = logging.getLogger()
logger.setLevel(logging.INFO)
loghandler = logging.StreamHandler(sys.stdout)
logformatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
loghandler.setFormatter(logformatter)
logger.addHandler(loghandler)

# Maximum elapsed time between positions before we declare a new run
MAX_TIME_BETWEEN_STOPS = timedelta(seconds=60 * 30)

# when dist_from_stop < 30.48 m (100 feet) considered "at stop" by MTA --NJ
# this is not correct! It's only that the sign displays "at stop"
# beginning at 100 ft. Nevertheless, we're doing 100 ft
STOP_THRESHOLD = 30.48

# Purposefully ignoring daylight savings for now
VEHICLE_QUERY = """SELECT
    p.timestamp_utc AS arrival,
    st.arrival_time scheduled_time,
    rt.trip_index trip,
    next_stop_id next_stop,
    tp.pattern_id pattern,
    dist_from_stop,
    GREATEST(0, dist_along_route + d.distance - dist_from_stop) AS distance,
    service_date,
    vehicle_id,
    stop_sequence AS seq
FROM positions p
    INNER JOIN ref_trips rt ON (rt.trip_id = p.trip_id)
    LEFT JOIN ref_trip_patterns tp ON (rt.trip_index = tp.trip_index)
    INNER JOIN ref_stop_times st ON (
        rt.trip_index = st.trip_index
        AND p.next_stop_id = st.stop_id
    )
    LEFT JOIN ref_stop_dist_between d ON (
        p.next_stop_id = d.rds_index
        AND tp.pattern_id = d.pattern_id
    )
WHERE vehicle_id = %s
    AND (
        service_date = %s
        OR (
            DATE(CONVERT_TZ(p.timestamp_utc, 'UTC', 'EST')) = DATE_SUB(%s, INTERVAL 1 DAY)
            AND TIME(CONVERT_TZ(p.timestamp_utc, 'UTC', 'EST')) > '23:00:00'
        )
        OR (
            DATE(CONVERT_TZ(p.timestamp_utc, 'UTC', 'EST')) = DATE_ADD(%s, INTERVAL 1 DAY)
            AND TIME(CONVERT_TZ(p.timestamp_utc, 'UTC', 'EST')) < '04:30:00'
        )
    )
ORDER BY timestamp_utc
"""

SELECT_VEHICLE = """SELECT DISTINCT vehicle_id
    FROM positions WHERE service_date = %s"""

SELECT_TRIP_INDEX = """SELECT
    st.stop_id id, arrival_time AS time, rds_index, stop_sequence AS seq, shape_dist_traveled
    FROM ref_stop_times st
    LEFT JOIN ref_trips t USING (trip_index)
    LEFT JOIN ref_stop_distances d ON (st.stop_id = d.stop_id and d.shape_id = t.shape_id)
    WHERE trip_index = %s
ORDER BY stop_sequence ASC"""

INSERT = """INSERT IGNORE INTO {}
    (vehicle_id, trip_index, rds_index, stop_sequence, call_time, source, deviation)
    VALUES ({}, {}, %s, %s, %s, %s, 555)"""

EPOCH = datetime.utcfromtimestamp(0)
EST = pytz.timezone('US/Eastern')


def to_unix(dt):
    return (dt - EPOCH).total_seconds()


def common(lis):
    return Counter(lis).most_common(1)[0][0]


def mask(positions, key):
    filt = (key(x, y) for x, y in zip(positions[1:], positions))
    ch = chain([True], filt)
    return list(compress(positions, ch))


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
    cursor.execute(VEHICLE_QUERY, (vehicle_id, date, date, date))

    runs = []
    prev = {}
    position = cursor.fetchone()

    while position is not None:
        position['departure'] = position['arrival']

        # If patterns differ, stop sequence goes down, or half an hour passed
        if (position['pattern'] != prev.get('pattern') or
                (position['seq'] or -2) < prev.get('seq', -1)):
            # start a new run
            runs.append([])

        if len(runs[-1]) and position['distance'] < runs[-1]['distance']:
            pass

        else:
            # append the current stop
            runs[-1].append(position)

        position = cursor.fetchone()

    # filter out any runs that start the next day
    # mask runs to eliminate out-of-order stop sequences
    runs = [mask(run, lambda x, y: x['seq'] >= y['seq']) for run in runs if run[0]['service_date'].isoformat() == date]

    return runs


def fetch_vehicles(cursor, date):
    cursor.execute(SELECT_VEHICLE, (date,))
    return [row['vehicle_id'] for row in cursor.fetchall()]


def extrapolate(p1, p2, x):
    '''
    Given two coordinates (float, timestamp),
    extrapolate the projected y based on a x-value
    For our narrow purposes, ys are assumed to be datetimes.
    assumed p1x, p2x, x are in a sequence
    '''
    dx0 = p2[0] - p1[0]
    dy0 = p2[1] - p1[1]
    dx1 = x - p2[0]

    return p2[1] + timedelta(seconds=dx1 * (dy0.total_seconds() / dx0))


def extrap_forward(calls, stoptimes):
    # the (n-1)th imputed call
    ssn_ = calls[-2][1]

    three_stop_times = [x for x in stoptimes if x['seq'] >= ssn_][:3]

    p1 = [three_stop_times[0]['shape_dist_traveled'], calls[-2][1]]
    p2 = [three_stop_times[1]['shape_dist_traveled'], calls[-1][1]]
    x = three_stop_times[2]['shape_dist_traveled']

    return [
        stoptimes[-1]['rds_index'],
        stoptimes[-1]['seq'],
        extrapolate(p1, p2, x),
        'E']


def extrap_back(calls, stoptimes):
    ss1 = calls[2][1]
    # first will be extrapolated using next two
    three_stop_times = [x for x in stoptimes if x['seq'] <= ss1][-3:]
    # latest
    p1 = [three_stop_times[2]['shape_dist_traveled'], calls[2][2]]
    # next earliest
    p2 = [three_stop_times[1]['shape_dist_traveled'], calls[1][2]]
    # earliest
    x0 = three_stop_times[0]['shape_dist_traveled']

    return [
        three_stop_times[0]['rds_index'],
        three_stop_times[0]['seq'],
        extrapolate(p1, p2, x0),
        'S']


def generate_calls(run, stoptimes):
    '''
    list of calls to be written
    Args:
        run: list generated from enumerate(positions)
        stoptimes: list of scheduled stoptimes for this trip
    '''
    # each call is a list of this format:
    # [rds_index, stop_sequence, datetime, source]

    obs_distances = [p['distance'] for p in run]
    obs_times = [to_unix(p['timestamp_utc']) for p in run]

    # purposefully avoid the first and last stops
    stop_positions = [x['shape_dist_traveled'] for x in stoptimes]

    # set start index to the stop that first position (P.0) is approaching
    try:
        si = stoptimes.index([x for x in stoptimes if x['seq'] == run[0]['seq']][0])
    except (IndexError, AttributeError):
        si = len(stoptimes)

    # set end index to the stop approached by the last position (P.n) (which means it won't be used in interp)
    try:
        ei = [x for x in stoptimes if x['seq'] == run[-1]['seq']][0]
    except (AttributeError, IndexError):
        ei = None

    interpolated = np.interp(stop_positions[si:ei], obs_distances, obs_times)
    calls = [
        [stop['rds_index'],
         stop['seq'],
         datetime.utcfromtimestamp(secs).replace(tzinfo=pytz.UTC).astimezone(EST),
         'I']
        for stop, secs in zip(stoptimes, interpolated)
    ]

    if len(calls) == 0:
        return []

    # Extrapolate forward to the next stop after the positions
    try:
        if ei < len(stoptimes):
            call = extrap_forward(calls, stoptimes)
            calls.append(call)

    except Exception as err:
        logging.error(repr(err))
        logging.error('failed end-extrapolation. v_id=%s, trip=%d',
                      run[0]['vehicle_id'], run[0]['trip'])
        logging.error('captured %d stops', len(calls))

    # Extrapolate back for the first stop before the positions
    try:
        if si > 0:
            call = extrap_back(calls, stoptimes)
            calls.insert(0, call)

    except Exception as err:
        logging.error(repr(err))
        logging.error('failed start-extrapolation. v_id=%s, trip=%d',
                      run[0]['vehicle_id'], run[0]['trip'])
        logging.error('captured %d stops', len(calls))

    return calls


def conf(section):
    return {
        'cursorclass': MySQLdb.cursors.DictCursor,
        'read_default_file': '~/.my.cnf',
        'read_default_group': section
    }


def process_vehicle(vehicle_id, table, date, rsect, wsect):
    source = MySQLdb.connect(**conf(rsect))
    sink = MySQLdb.connect(**conf(wsect))

    print('STARTING', vehicle_id, file=sys.stderr)
    with source.cursor() as cursor:
        # returns list in memory
        runs = filter_positions(cursor, vehicle_id, date)
        lenc = 0

        with sink.cursor() as sinker:
            # each run will become a trip
            for run in runs:
                if len(run) == 0:
                    continue
                elif len(run) <= 3:
                    logging.info('short run (%d positions), v_id=%s, %s',
                                 len(run), vehicle_id, run[0]['arrival'])
                    continue

                # get the scheduled list of trips for this run
                trip_index = common([x['trip'] for x in run])

                cursor.execute(SELECT_TRIP_INDEX, (trip_index,))
                calls = generate_calls(run, cursor.fetchall())

                # write calls to sink
                insert = INSERT.format(table, vehicle_id, trip_index)
                sinker.executemany(insert, calls)
                lenc += len(calls)

            print('COMMIT', vehicle_id, lenc, file=sys.stderr)
            sink.commit()

    sink.close()
    source.close()


def main(congfig_sections, table, date, vehicle=None):
    # connect to MySQL
    sections = congfig_sections.split(',')

    if vehicle:
        vehicles = [vehicle]
    else:
        conn = MySQLdb.connect(**conf(sections[0]))
        with conn.cursor() as cursor:
            vehicles = fetch_vehicles(cursor, date)
        conn.close()

    itervehicles = zip(vehicles,
                       cycle([table]),
                       cycle([date]),
                       cycle(sections),
                       cycle([sections[0]])
                       )

    with Pool(os.cpu_count()) as pool:
        pool.starmap(process_vehicle, itervehicles)

    print("SUCCESS: Committed %s" % date, file=sys.stderr)


if __name__ == '__main__':
    main(*sys.argv[1:])
