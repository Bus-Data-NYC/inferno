#!/user/bin/env python3.5
from __future__ import division
import argparse
import sys
import os
from typing import Callable
from datetime import datetime, timedelta
from multiprocessing import Pool
import logging
import warnings
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

# Doing one complicated thing in this query.
# Some bus routes are loops with tails (e.g. B74):
#    +--+
#    |  |---- (start and end)
#    +——+
# ST_LineLocatePoint can't handle this, so we use the mostly-untrustworthy
# "positions"."dist_along_route" column to limit the shape_geom LineString to
# just the half of the line.
VEHICLE_QUERY = """WITH a AS (SELECT
    EXTRACT(EPOCH FROM timestamp_utc) AS timestamp,
    vehicle_id,
    trip_id,
    service_date,
    stop_id next_stop,
    stop_sequence seq,
    dist_along_route,
    ST_Length(s.the_geom::geography) AS length,
    the_geom,
    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS position
FROM positions p
    LEFT JOIN gtfs_trips USING (trip_id)
    LEFT JOIN gtfs_stop_times st USING (feed_index, trip_id, stop_id)
    LEFT JOIN gtfs_shape_geoms s USING (feed_index, shape_id)
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
) SELECT
    timestamp,
    vehicle_id,
    trip_id,
    service_date,
    next_stop,
    seq,
    ROUND((ST_LineLocatePoint(ST_LineSubstring(the_geom, GREATEST(0, dist_along_route / length - 0.25),
        LEAST(1, dist_along_route / length + 0.25)), position) * length)::NUMERIC, 2) distance
FROM a
"""

SELECT_VEHICLE = """SELECT DISTINCT vehicle_id
    FROM positions WHERE service_date = %s"""

SELECT_STOPTIMES = """SELECT
    stop_id AS id,
    wall_time(date %(date)s, arrival_time, agency_timezone) AS datetime,
    route_id,
    direction_id,
    stop_sequence AS seq,
    dist_along_shape distance
FROM gtfs_trips
    LEFT JOIN gtfs_agency USING (feed_index)
    LEFT JOIN gtfs_stop_times USING (feed_index, trip_id)
    LEFT JOIN gtfs_stops USING (feed_index, stop_id)
    LEFT JOIN gtfs_stop_distances_along_shape USING (feed_index, shape_id, stop_id)
WHERE trip_id = %(trip)s
    AND feed_index = (
        SELECT MAX(feed_index)
        FROM gtfs_trips
            LEFT JOIN gtfs_calendar USING (feed_index, service_id)
        WHERE trip_id = %(trip)s
            AND date %(date)s BETWEEN start_date and end_date
    )
ORDER BY stop_sequence ASC
"""

SELECT_STOPTIMES_PLAIN = """SELECT DISTINCT
    stop_id id,
    wall_time(date %(date)s, arrival_time, agency_timezone) AS datetime,
    route_id,
    direction_id,
    stop_sequence AS seq,
    dist_along_shape distance
FROM gtfs_trips
    LEFT JOIN gtfs_agency USING (feed_index)
    LEFT JOIN gtfs_stop_times USING (feed_index, trip_id)
    LEFT JOIN gtfs_stops USING (feed_index, stop_id)
    LEFT JOIN gtfs_stop_distances_along_shape USING (feed_index, shape_id, stop_id)
WHERE trip_id = %(trip)s
ORDER BY stop_sequence ASC;
"""

INSERT = """INSERT INTO {}
    (vehicle_id, trip_id, route_id, direction_id, stop_id, call_time, source, deviation)
    VALUES (%(vehicle)s, %(trip)s, %(route_id)s, %(direction_id)s, %(stop_id)s, %(call_time)s, %(source)s, %(deviation)s)"""


def common(lis: list):
    return Counter(lis).most_common(1)[0][0]


def mask2(lis: list, key: Callable) -> list:
    '''
    Create a mask on `lis` using the `key` function.
    `key` will be evaluated on pairs of items in `lis`.
    Returned list will only include items where `key` evaluates to True.
    '''
    filt = (key(x, y) for x, y in zip(lis[1:], lis))
    ch = chain([True], filt)
    return list(compress(lis, ch))


def desc2fn(description: tuple) -> tuple:
    '''Extract tuple of field names from psycopg2 cursor.description.'''
    return tuple(d.name for d in description)


def compare_seq(x, y):
    try:
        return x['seq'] >= y['seq']
    except TypeError:
        # Be lenient when there's bad data: return True when None.
        return x['seq'] is None or y['seq'] is None


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
        logging.warning('No rows found for %s on %s', vehicle, date)
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
    runs = [mask2(run, compare_seq) for run in runs
            if run[0]['service_date'].isoformat() == date
            and len(run) > 2
            ]

    return runs


def get_stoptimes(cursor, tripid, date):
    fields = {'trip': tripid, 'date': date}
    cursor.execute(SELECT_STOPTIMES, fields)
    fieldnames = desc2fn(cursor.description)

    # The feed indices where corrput. Run this again without checking the date range.
    if cursor.rowcount == 0:
        logging.warning("Couldn't find any stoptimes in date range, running simple query: %s", tripid)
        logging.warning(cursor.query.decode('utf8'))
        cursor.execute(SELECT_STOPTIMES_PLAIN, fields)
        fieldnames = desc2fn(cursor.description)

    return [dict(zip(fieldnames, row)) for row in cursor.fetchall()]


def extrapolate(x, y, vals):
    coefficients = np.polyfit(x, y, 1)
    return np.poly1d(coefficients)(vals)


def call(stoptime, seconds, method=None):
    '''
    Returns a dict with route, direction, stop, call time and source.
    Call time is in UTC.
    '''
    calltime = datetime.utcfromtimestamp(seconds).replace(tzinfo=pytz.UTC)
    return {
        'route_id': stoptime['route_id'],
        'direction_id': stoptime['direction_id'],
        'stop_id': stoptime['id'],
        'call_time': calltime,
        'deviation': calltime - stoptime['datetime'],
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
    obs_times = [p['timestamp'] for p in run]
    stop_positions = [x['distance'] for x in stoptimes]
    stop_seq = [x['seq'] for x in stoptimes]

    # set start index to the stop that first position (P.0) is approaching
    try:
        si = stop_seq.index(min(x['seq'] for x in run))
    except (TypeError, ValueError):
        si = 0

    # set end index to the stop approached by the last position (P.n) (which means it won't be used in interp)
    try:
        ei = stop_seq.index(max(x['seq'] for x in run))
    except (TypeError, ValueError):
        ei = len(stoptimes) - 1

    if len(stop_positions[si:ei]) == 0:
        return []

    calls = []

    # Extrapolate back for stops that occurred before observed positions.
    if si > 0:
        try:
            extrapolated = extrapolate(obs_distances[:4], obs_times[:4], stop_positions[:si])
            calls = [call(st, ex, 'S') for ex, st in zip(extrapolated, stoptimes[:si])]
        except ValueError:
            pass
        except TypeError:
            logging.error('Error extrapolating early stops. index: %s', si)
            logging.error('positions %s, sequence: %s', stop_positions[:si], stop_seq[:si])

    # Interpolate main chunk of positions.
    interpolated = np.interp(stop_positions[si:ei], obs_distances, obs_times)
    calls.extend([call(stop, secs) for stop, secs in zip(stoptimes[si:ei], interpolated)])

    # Extrapolate forward to the stops after the observed positions.
    if ei < len(stoptimes):
        try:
            extrapolated = extrapolate(obs_distances[-4:], obs_times[-4:], stop_positions[ei:])
            calls.extend([call(st, ex, 'E') for ex, st in zip(extrapolated, stoptimes[ei:])])
        except ValueError:
            pass
        except TypeError:
            logging.error('Error extrapolating late stops. index: %s', ei)
            logging.error('positions %s, sequence: %s', stop_positions[ei:], stop_seq[ei:])

    try:
        assert increasing([x['call_time'] for x in calls])
    except AssertionError:
        import pdb; pdb.set_trace()

    return calls

def increasing(L):
    return all(x <= y for x, y in zip(L, L[1:]))


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
                stoptimes = get_stoptimes(cursor, trip_id, date)

                if any(x['distance'] is None for x in stoptimes):
                    logging.warning('Missing stoptimes for %s', trip_id)
                    continue

                # Generate (infer) calls.
                calls = generate_calls(run, stoptimes)

                # write calls to sink
                cursor.executemany(
                    INSERT.format(table),
                    [dict(trip=trip_id, vehicle=vehicle_id, **c) for c in calls]
                )
                lenc += len(calls)
                conn.commit()

            logging.info('COMMIT %s (%s calls)', vehicle_id, lenc)


def main():
    # connectionstring: str, table, date, vehicle=None
    parser = argparse.ArgumentParser()
    parser.add_argument('connectionstring', type=str)
    parser.add_argument('date', type=str)
    parser.add_argument('--table', type=str, default='calls')
    parser.add_argument('--vehicle', type=str)

    args = parser.parse_args()

    psycopg2.extensions.register_type(DEC2FLOAT)

    if args.vehicle:
        vehicles = [args.vehicle]
    else:
        with psycopg2.connect(args.connectionstring) as conn:
            with conn.cursor() as cursor:
                cursor.execute(SELECT_VEHICLE, (args.date,))
                vehicles = [x[0] for x in cursor.fetchall()]

    itervehicles = zip(vehicles,
                       cycle([args.table]),
                       cycle([args.date]),
                       cycle([args.connectionstring])
                       )

    for i in itervehicles:
        track_vehicle(*i)

    with Pool(os.cpu_count()) as pool:
        pool.starmap(track_vehicle, itervehicles)

    logging.info("SUCCESS: Committed %s", args.date)


if __name__ == '__main__':
    main()
