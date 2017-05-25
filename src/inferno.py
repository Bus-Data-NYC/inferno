#!/user/bin/env python3
from __future__ import division
import sys
import os
from datetime import datetime
from multiprocessing import Pool
import logging
import warnings
from datetime import timedelta
from collections import Counter
from itertools import chain, compress, cycle
import numpy as np
import pytz
import psycopg2


logger = logging.getLogger()
logger.setLevel(logging.INFO)
loghandler = logging.StreamHandler(sys.stdout)
logformatter = logging.Formatter(fmt='%(levelname)s: %(message)s')
loghandler.setFormatter(logformatter)
logger.addHandler(loghandler)

warnings.simplefilter('ignore', np.RankWarning)

DEC2FLOAT = psycopg2.extensions.new_type(
    psycopg2.extensions.DECIMAL.values,
    'DEC2FLOAT',
    lambda value, curs: float(value) if value is not None else None
)

# Maximum elapsed time between positions before we declare a new run
MAX_TIME_BETWEEN_STOPS = timedelta(seconds=60 * 30)

# when dist_from_stop < 30.48 m (100 feet) considered "at stop" by MTA --NJ
# this is not correct! It's only that the sign displays "at stop"
# beginning at 100 ft. Nevertheless, we're doing 100 ft
STOP_THRESHOLD = 30.48

VEHICLE_QUERY = """SELECT
    timestamp_utc AS timestamp,
    vehicle_id,
    trip_id,
    service_date,
    stop_id next_stop,
    stop_sequence seq,
    dist_along_route,
    dist_along_shape,
    dist_from_stop,
    dist_along_shape - dist_from_stop AS distance
FROM positions p
    LEFT JOIN gtfs_trips USING (trip_id)
    LEFT JOIN gtfs_stop_times st USING (feed_index, trip_id, stop_id)
    INNER JOIN gtfs_stop_distances_along_shape USING (feed_index, shape_id, stop_id)
WHERE
    vehicle_id = %(vehicle)s
    AND (
        service_date = date %(date)s
        -- give leeway for broken service_date at the very end or start of day
        OR timestamp_utc
            BETWEEN date %(date)s - interval '1 HOURS'
            AND date %(date)s + interval '29 HOURS'
    )
ORDER BY
    trip_id,
    stop_sequence,
    timestamp_utc
"""

SELECT_VEHICLE = """SELECT DISTINCT vehicle_id
    FROM positions WHERE service_date = %s"""

SELECT_TRIP_INDEX = """SELECT
    stop_id id,
    arrival_time AS time,
    route_id,
    gtfs_trips.direction_id,
    stop_id,
    stop_sequence AS seq,
    dist_along_shape
FROM gtfs_trips
    LEFT JOIN gtfs_stop_times USING (feed_index, trip_id)
    LEFT JOIN gtfs_stops USING (feed_index, stop_id)
    LEFT JOIN gtfs_stop_distances_along_shape USING (feed_index, shape_id, stop_id)
WHERE trip_id = %s
ORDER BY stop_sequence ASC;
"""

INSERT = """INSERT INTO {}
    (vehicle_id, trip_id, route_id, direction_id, stop_id, call_time, source)
    VALUES ({}, '{}', %(route_id)s, %(direction_id)s, %(stop_id)s, %(call_time)s, %(source)s)"""

EPOCH = datetime.utcfromtimestamp(0)


def to_unix(dt: datetime) -> int:
    return (dt - EPOCH).total_seconds()


def common(lis: list):
    return Counter(lis).most_common(1)[0][0]


def mask2(lis: list, key: function) -> list:
    '''
    Create a mask on `lis` using the `key` function.
    `key` will be evaluated on pairs of items in `lis`.
    Returned list will only include items where `key` evaluates to True.
    '''
    filt = (key(x, y) for x, y in zip(lis[1:], lis))
    ch = chain([True], filt)
    return list(compress(lis, ch))


def desc2fn(description: list) -> list:
    return [d[0] for d in description]


def filter_positions(cursor, date, vehicle=None):
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
    runs = []
    prev = {}

    # load up cursor with every position for vehicle
    cursor.execute(VEHICLE_QUERY, {'vehicle': vehicle, 'date': date})
    if cursor.rowcount == 0:
        logging.warn('No rows found for %s on %s', vehicle, date)
        return []

    fieldnames = desc2fn(cursor.description)
    position = dict(zip(fieldnames, cursor.fetchone()))

    while position is not None:
        # If trip IDs differ
        if (position['trip_id'] != prev.get('trip_id', None)):
            if prev.get('trip_id', None):
                # finish old run
                runs[-1].append(prev)
            # start a new run
            runs.append([])

        elif prev.get('next_stop') != position['next_stop']:
            # append the previous stop
            runs[-1].append(prev)

        prev = position

        try:
            position = dict(zip(fieldnames, cursor.fetchone()))
        except TypeError:
            position = None

    if len(runs):
        # append very last position
        runs[-1].append(prev)

    # filter out any runs that start the next day
    # mask runs to eliminate out-of-order stop sequences
    runs = [mask2(run, lambda x, y: x['seq'] >= y['seq']) for run in runs
            if run[0]['service_date'].isoformat() == date
            ]

    return runs


def get_stoptimes(cursor, tripid):
    cursor.execute(SELECT_TRIP_INDEX, (tripid,))
    fieldnames = desc2fn(cursor.description)
    return [dict(zip(fieldnames, row)) for row in cursor.fetchall()]


def call(stoptime, seconds, method=None):
    '''
    Returns a dict with route, direction, stop, call time and source.
    Call time is in UTC.
    '''
    calltime = datetime.utcfromtimestamp(seconds).replace(tzinfo=pytz.UTC)
    return {
        'route_id': stoptime['route_id'],
        'direction_id': stoptime['direction_id'],
        'stop_id': stoptime['stop_id'],
        'call_time': calltime,
        'source': method or 'I'
    }


def generate_calls(run: list, stoptimes: list) -> list:
    '''
    list of calls to be written
    Args:
        run: list generated from enumerate(positions)
        stoptimes: list of scheduled stoptimes for this trip
    '''
    obs_distances = [p['distance'] for p in run]
    obs_times = [to_unix(p['timestamp']) for p in run]
    stop_positions = [x['dist_along_shape'] for x in stoptimes]

    # set start index to the stop that first position (P.0) is approaching
    try:
        si = stoptimes.index([x for x in stoptimes if x['seq'] == run[0]['seq']][0])
    except (IndexError, AttributeError):
        si = 0

    # set end index to the stop approached by the last position (P.n) (which means it won't be used in interp)
    try:
        ei = stoptimes.index([x for x in stoptimes if x['seq'] == run[-1]['seq']][0])
    except (AttributeError, IndexError):
        ei = len(stoptimes)

    if len(stop_positions[si:ei]) == 0:
        return []

    interpolated = np.interp(stop_positions[si:ei], obs_distances, obs_times)
    calls = [call(stop, secs) for stop, secs in zip(stoptimes[si:ei], interpolated)]

    if len(run) > 3:
        # Extrapolate forward to the next stop after the positions
        if ei < len(stoptimes):
            try:
                coefficients = np.polyfit(obs_distances[-3:], obs_times[-3:], 1)
                extrapolated = np.poly1d(coefficients)(stop_positions[ei])
                calls.append(call(stoptimes[ei], extrapolated, 'E'))
            except ValueError:
                pass
            except TypeError:
                logging.error('Error extrapolating early stops. index: %s', ei)
                logging.error('Stop position %s', stop_positions[ei])

        # Extrapolate back for a single stop before the positions
        if si > 0:
            coefficients = np.polyfit(obs_distances[:3], obs_times[:3], 1)
            try:
                extrapolated = np.poly1d(coefficients)(stop_positions[si])
                calls.insert(0, call(stoptimes[si], extrapolated, 'S'))
            except ValueError:
                pass
            except TypeError:
                logging.error('Error extrapolating early stops. index: %s', si)
                logging.error('Stop position %s', stop_positions[si])

    return calls


def track_vehicle(vehicle_id, table, date, connectionstring):
    with psycopg2.connect(connectionstring) as conn:
        logging.info('STARTING %s', vehicle_id)
        with conn.cursor() as cursor:
            runs = filter_positions(cursor, date, vehicle_id)
            # Counter is just for logging.
            lenc = 0

            # each run will become a trip
            for run in runs:
                if len(run) == 0:
                    continue
                elif len(run) <= 2:
                    logging.debug('short run (%d positions), v_id=%s, %s',
                                 len(run), vehicle_id, run[0]['timestamp'])
                    continue

                # Assume most common trip is the correct one.
                trip_id = common([x['trip_id'] for x in run])

                # Get the scheduled list of stops for this trip.
                stoptimes = get_stoptimes(cursor, trip_id)

                # Generate (infer) calls.
                calls = generate_calls(run, stoptimes)

                # write calls to sink
                insert = INSERT.format(table, vehicle_id, trip_id)
                cursor.executemany(insert, calls)
                lenc += len(calls)
                conn.commit()

            logging.info('COMMIT %s (%s calls)', vehicle_id, lenc)


def main(connectionstring: str, table, date, vehicle=None):
    psycopg2.extensions.register_type(DEC2FLOAT)

    if vehicle:
        vehicles = [vehicle]
    else:
        with psycopg2.connect(connectionstring) as conn:
            with conn.cursor() as cursor:
                cursor.execute(SELECT_VEHICLE, (date,))
                vehicles = [x[0] for x in cursor.fetchall()]

    itervehicles = zip(vehicles,
                       cycle([table]),
                       cycle([date]),
                       cycle([connectionstring])
                       )

    for i in itervehicles:
        track_vehicle(*i)

    with Pool(os.cpu_count()) as pool:
        pool.starmap(track_vehicle, itervehicles)

    logging.info("SUCCESS: Committed %s", date)


if __name__ == '__main__':
    main(*sys.argv[1:])
