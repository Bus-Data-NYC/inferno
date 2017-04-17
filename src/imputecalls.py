#!/user/bin/env python3
from __future__ import division
import sys
import os
from datetime import datetime
from multiprocessing import Pool
import logging
from datetime import timedelta
from collections import Counter
from itertools import chain, cycle, tee
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
    pattern_id pattern,
    GREATEST(0, dist_along_route + d.distance - dist_from_stop) AS dist,
    service_date,
    vehicle_id,
    stop_sequence
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
    st.stop_id id, arrival_time AS time, rds_index, stop_sequence, shape_dist_traveled
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


def pairwise(iterable, default=None):
    "s -> (s0,s1), (s1,s2), (s2, s3), ... (s'n, None)"
    a, b = tee(iterable)
    next(b, None)
    return zip(a, chain(b, [default]))


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
    cursor.execute(VEHICLE_QUERY, (vehicle_id, date, date, date))

    runs = []
    prev = {}
    position = cursor.fetchone()

    while position is not None:
        position['departure'] = position['arrival']

        # If patterns differ, stop sequence goes down, or half an hour passed
        if (position['pattern'] != prev.get('pattern') or
                (position['stop_sequence'] or -2) < prev.get('stop_sequence', -1) or
                position['arrival'] > prev.get('arrival') + MAX_TIME_BETWEEN_STOPS):
            # start a new run
            runs.append([])

        if position['distance'] < runs[-1]['distance']:
            pass

        else:
            # append the current stop
            runs[-1].append(position)

        position = cursor.fetchone()

    # filter out any runs that start the next day
    runs = [run for run in runs if run[0]['service_date'].isoformat() == date]

    return runs


def fetch_vehicles(cursor, date):
    cursor.execute(SELECT_VEHICLE, (date,))
    return [row['vehicle_id'] for row in cursor.fetchall()]


def get_last_before(sequence, stop_number):
    '''Get the last position in a sequence where stop_sequence is <= the stop_number'''
    # This will raise an IndexError if something goes wrong
    i, position = [(i, p) for i, p in enumerate(sequence)
                   if p['stop_sequence'] <= stop_number].pop()

    return i, position


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


def find_call(positions, stoptime, next_stoptime):
    '''
    Find a "captured" call based on a scheduled stop times and a run of positions.
    The following scheduled stop time is also used to ensure correct ordering
    '''
    # each call is a list of this format:
    # [rds_index, stop_sequence, datetime, source]
    try:
        i, last_before = get_last_before(positions, stoptime['stop_sequence'])
        first_after = positions[i + 1]
    except IndexError:
        return

    next_stoptime = next_stoptime or {'id': first_after['next_stop']}

    # got positions that are on either side of this guy
    if (last_before['next_stop'] == stoptime['id'] and
            first_after['next_stop'] == next_stoptime['id']):
        method = 'C'
        elapsed = first_after['arrival'] - last_before['departure']
        call_time = last_before['departure'] + timedelta(seconds=elapsed.total_seconds() / 2)

        return [stoptime['rds_index'], stoptime['stop_sequence_original'], call_time, method]


def impute_calls(stop_sequence, calls, stoptimes):
    '''
    Impute missing calls beginning at stop_sequence
    The following scheduled stop time is also used to ensure correct ordering
    '''
    # each call is a list of this format:
    # [rds_index, stop_sequence, datetime, source]
    prev_call = max([c for c in calls if c[1] < stop_sequence], key=lambda x: x[1])
    next_call = min([c for c in calls if c[1] > stop_sequence], key=lambda x: x[1])

    st_dict = {x['stop_sequence_original']: x for x in stoptimes}
    seq2orig = {x['stop_sequence']: x['stop_sequence_original'] for x in stoptimes}
    next_st, prev_st = st_dict[next_call[1]], st_dict[prev_call[1]]

    # Duration between the two stops divided by the scheduled duration
    ratio = (next_call[2] - prev_call[2]).total_seconds() / \
        max(1., (next_st['time'] - prev_st['time']).total_seconds())

    output = []
    deltasum = timedelta(seconds=0)
    # Interpolate the particular (missing) stop number between the two positions
    for seq in range(prev_st['stop_sequence'] + 1, next_st['stop_sequence']):
        orig_seq = seq2orig[seq]
        orig_seq_prev = seq2orig[seq - 1]
        try:
            # scheduled time between this stop and immediate previous stop
            scheduled_call_elapsed = (st_dict[orig_seq]['time'] - st_dict[orig_seq_prev]['time']).total_seconds()

            # delta is schedule * ratio
            deltasum += timedelta(seconds=scheduled_call_elapsed * ratio)

            output.append([st_dict[orig_seq]['rds_index'],
                           st_dict[orig_seq]['stop_sequence_original'],
                           prev_call[2] + deltasum,
                           'I'])

        except KeyError as err:
            logging.error('KeyError (2) seq: %s, key: %s', seq, err)
            raise err

    return output


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
        si = stoptimes.index([x for x in stoptimes if x['stop_sequence'] == run[0]['stop_sequence']][0])
    except (IndexError, AttributeError):
        si = len(stoptimes)

    # set end index to the stop approached by the last position (P.n) (which means it won't be used in interp)
    try:
        ei = [x for x in stoptimes if x['stop_sequence'] == run[-1]['stop_sequence']][0]
    except (AttributeError, IndexError):
        ei = None

    interpolated = np.interp(stop_positions[si:ei], obs_distances, obs_times)
    calls = [
        [stop['rds_index'],
         stop['stop_sequence'],
         datetime.utcfromtimestamp(secs).replace(tzinfo=pytz.UTC).astimezone(EST),
         'I']
        for stop, secs in zip(stoptimes, interpolated)
    ]

    if len(calls) == 0:
        return []

    # Extrapolate forward to the next stop after the positions
    try:
        if ei < len(stoptimes):
            # the (n-1)th imputed call
            ssn_ = calls[-2][1]

            three_stop_times = [x for x in stoptimes if x['stop_sequence'] >= ssn_][:3]

            p1 = [three_stop_times[0]['shape_dist_traveled'], calls[-2][1]]
            p2 = [three_stop_times[1]['shape_dist_traveled'], calls[-1][1]]
            x = three_stop_times[2]['shape_dist_traveled']

            calls.append([
                stoptimes[-1]['rds_index'],
                stoptimes[-1]['stop_sequence_original'],
                extrapolate(p1, p2, x),
                'E']
            )

    except Exception as err:
        logging.error(repr(err))
        logging.error('failed end-extrapolation. v_id=%s, trip=%d',
                      run[0]['vehicle_id'], run[0]['trip'])
        logging.error('captured %d stops', len(calls))

    # Extrapolate back for the first stop before the positions
    try:
        if si > 0:
            ss1 = calls[2][1]
            # first will be extrapolated using next two
            three_stop_times = [x for x in stoptimes if x['stop_sequence'] <= ss1][-3:]
            # latest
            p1 = [three_stop_times[2]['shape_dist_traveled'], calls[2][2]]
            # next earliest
            p2 = [three_stop_times[1]['shape_dist_traveled'], calls[1][2]]
            # earliest
            x0 = three_stop_times[0]['shape_dist_traveled']

            calls.insert(0, [
                three_stop_times[0]['rds_index'],
                three_stop_times[0]['stop_sequence'],
                extrapolate(p1, p2, x0),
                'S']
            )

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
