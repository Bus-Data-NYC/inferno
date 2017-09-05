#!/user/bin/env python3.5
from __future__ import division
import sys
import os
from bisect import bisect, bisect_left
from typing import Callable
from datetime import datetime, timedelta
from multiprocessing import Pool
import logging
import warnings
from collections import Counter, namedtuple
from itertools import cycle
import argparse
import psycopg2
from psycopg2.extras import NamedTupleCursor
import numpy as np
import pytz


logger = logging.getLogger()
logger.setLevel(logging.INFO)
loghandler = logging.StreamHandler(sys.stdout)
logformatter = logging.Formatter(fmt='%(levelname)s (%(lineno)3d) %(message)s')
loghandler.setFormatter(logformatter)
logger.addHandler(loghandler)

warnings.simplefilter('ignore')

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

# Minimum distance between positions when extrapolating.
# When zero, identical positions are allowed, which can produce crazy results
MIN_EXTRAP_DIST = 5

# Doing one complicated thing in this query.
# Some bus routes are loops with tails (e.g. B74):
#    +--+
#    |  |---- (start and end)
#    +——+
# ST_LineLocatePoint can't handle this, so we use the mostly-untrustworthy
# "positions"."dist_along_route" column to limit the shape_geom LineString to
# just the half of the line.
VEHICLE_QUERY = """WITH service AS (
    -- give leeway for broken trip_start_date/service_date at the very end or start of day
    -- get service date in agency timezone for valid comparison to timestamp_utc
    SELECT
        feed_index,
        %(date)s::timestamp at time zone agency_timezone - interval '1 HOURS' AS left,
        %(date)s::timestamp at time zone agency_timezone + interval '29 HOURS' AS right
    FROM gtfs_agency
)
SELECT
    EXTRACT(EPOCH FROM timestamp) AS timestamp,
    trip_id,
    trip_start_date date,
    stop_sequence seq,
    ROUND(length * careful_locate(the_geom, ST_SetSRID(ST_MakePoint(longitude, latitude), 4326),
        (dist_along_route / length)::numeric, 0.2)::numeric, 2) AS distance
FROM {0} p
    LEFT JOIN gtfs_trips USING (trip_id)
    -- TODO: change to LEFT JOIN when fix implemented for orphan stops
    INNER JOIN gtfs_stop_times st USING (feed_index, trip_id, stop_id)
    LEFT JOIN gtfs_shape_geoms s USING (feed_index, shape_id)
    LEFT JOIN service USING (feed_index)
WHERE
    vehicle_id = %(vehicle)s
    AND (
        trip_start_date = date %(date)s
        OR timestamp BETWEEN service.left AND service.right
    )
ORDER BY
    trip_id,
    timestamp
"""

SELECT_VEHICLE = """SELECT DISTINCT vehicle_id
    FROM {0} WHERE trip_start_date = %s"""

SELECT_STOPTIMES = """SELECT
    stop_id AS id,
    wall_time(date %(date)s, arrival_time, agency_timezone) AS datetime,
    route_id,
    direction_id,
    stop_sequence AS seq,
    shape_dist_traveled distance
FROM gtfs_trips
    LEFT JOIN gtfs_agency USING (feed_index)
    LEFT JOIN gtfs_stop_times USING (feed_index, trip_id)
    LEFT JOIN gtfs_stops USING (feed_index, stop_id)
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
    shape_dist_traveled distance
FROM gtfs_trips
    LEFT JOIN gtfs_agency USING (feed_index)
    LEFT JOIN gtfs_stop_times USING (feed_index, trip_id)
    LEFT JOIN gtfs_stops USING (feed_index, stop_id)
WHERE trip_id = %(trip)s
ORDER BY stop_sequence ASC;
"""

INSERT = """INSERT INTO {}
    (vehicle_id, trip_id, route_id, direction_id, stop_id, call_time, source, deviation)
    VALUES (%(vehicle)s, %(trip)s, %(route_id)s, %(direction_id)s, %(stop_id)s, %(call_time)s, %(source)s, %(deviation)s)"""


def common(lis: list):
    return Counter(lis).most_common(1)[0][0]


def mask(lis: list, key: Callable) -> list:
    '''
    Create a mask on `lis` using the `key` function.
    `key` will be evaluated on pairs of items in `lis`.
    Returned list will only include items where `key` evaluates to True.
    '''
    result = [lis[0]]
    for item in lis[1:]:
        if key(item, result[-1]):
            result.append(item)
    return result


def desc2fn(description: tuple) -> tuple:
    '''Extract tuple of field names from psycopg2 cursor.description.'''
    return tuple(d.name for d in description)


def compare_seq(x, y):
    try:
        return x.seq >= y.seq
    except TypeError:
        # Be lenient when there's bad data: return True when None.
        return x.seq is None or y.seq is None


def compare_dist(a, b):
    try:
        return a.distance >= b.distance
    except TypeError:
        # Don't be lenient when there's bad data: return False.
        return False


def samerun(a, b):
    '''Check if two positions belong to the same run'''
    return all((
        # Trip is the same.
        getattr(a, 'trip_id', None) == b.trip_id,
        # Sequence is the same or higher.
        getattr(a, 'seq', 0) <= b.seq,
    ))


def filter_positions(cursor, date, positions_table=None, vehicle=None):
    '''
    Compile list of positions for a vehicle, using a list of positions
    and filtering based on positions that reflect change in pattern or next_stop.
    '''
    runs = []
    query = VEHICLE_QUERY.format(positions_table or 'positions')

    # load up cursor with every position for vehicle
    cursor.execute(query, {'vehicle': vehicle, 'date': date})
    if cursor.rowcount == 0:
        logging.warning('No rows found for %s on %s', vehicle, date)
        return []

    prev = namedtuple('prev', [])()
    position = cursor.fetchone()
    while position is not None:
        # If trip IDs differ
        if not samerun(prev, position):
            # start a new run
            runs.append([])

        # Check if distance is the same or greater, otherwise discard.
        if getattr(prev, 'distance', 0) <= position.distance:
            # append the position
            runs[-1].append(position)

        prev = position
        position = cursor.fetchone()

    # filter out any runs that start the next day
    # mask runs to eliminate out-of-order stop sequences
    runs = [mask(run, key=lambda a, b: compare_seq(a, b) and compare_dist(a, b)) for run in runs
            if len(run) > 2
            and run[0].date.isoformat() == date
            and len(set(r.seq for r in run)) > 1
            ]

    return runs


def get_stoptimes(cursor, tripid, date):
    logging.debug('Fetching stoptimes for %s', tripid)
    fields = {'trip': tripid, 'date': date}
    cursor.execute(SELECT_STOPTIMES, fields)

    if cursor.rowcount == 0:
        logging.warning("Couldn't find any stoptimes in date range, running simple query: %s", tripid)
        logging.warning(cursor.query.decode('utf8'))
        cursor.execute(SELECT_STOPTIMES_PLAIN, fields)

    return cursor.fetchall()


def extrapolate(run, stoptimes, method=None):
    x = [a.distance for a in run]
    y = [a.timestamp for a in run]
    coefficients = np.polyfit(x, y, 1)
    result = np.poly1d(coefficients)([x.distance for x in stoptimes])
    return [call(s, t, method) for s, t in zip(stoptimes, result)]


def call(stoptime, seconds, method=None):
    '''
    Returns a dict with route, direction, stop, call time and source.
    Call time is in UTC.
    '''
    calltime = datetime.utcfromtimestamp(seconds).replace(tzinfo=pytz.UTC)
    return {
        'route_id': stoptime.route_id,
        'direction_id': stoptime.direction_id,
        'stop_id': stoptime.id,
        'call_time': calltime,
        'deviation': calltime - stoptime.datetime,
        'source': method or 'I'
    }


def generate_calls(run: list, stops: list) -> list:
    '''
    list of calls to be written
    Args:
        run: list generated from enumerate(positions)
        stoptimes: list of scheduled stoptimes for this trip
    '''
    obs_distances = [p.distance for p in run]
    obs_times = [p.timestamp for p in run]
    stop_positions = [x.distance for x in stops]
    e = 4

    # Get the range of stop positions that can be interpolated based on data.
    # The rest will be extrapolated
    si = bisect_left(stop_positions, obs_distances[0])
    ei = bisect(stop_positions, obs_distances[-1])

    if len(stops[si:ei]) == 0:
        return []

    # Interpolate main chunk of positions.
    interpolated = np.interp(stop_positions[si:ei], obs_distances, obs_times)
    calls = [call(stop, secs) for stop, secs in zip(stops[si:ei], interpolated)]

    # Extrapolate back for stops that occurred before observed positions.
    # Second part of conditional establishes that we have enough to judge
    # Goal is to only extrapolate based on unique distances
    masked = mask(run, lambda x, y: x.distance > y.distance + MIN_EXTRAP_DIST)

    if si > 0 and len(masked) > si + e:
        try:
            backward = extrapolate(masked[:e], stops[:si], 'S')
            calls = backward + calls
        except Exception as error:
            logging.warning('%s. Ignoring back extrapolation', error)
            logging.warning('    positions %s, sequence: %s, stops: %s', stop_positions[:si], [x.seq for x in stops[:si]], stop_positions[:si],)

    # Extrapolate forward to the stops after the observed positions.
    if ei < len(stops) and len(masked) > e + ei:
        try:
            forward = extrapolate(masked[-e:], stops[ei:], 'E')
            calls.extend(forward)
        except Exception as error:
            logging.warning('%s. Ignoring forward extrapolation', error)
            logging.warning('positions %s, sequence: %s', stop_positions[ei:], [x.seq for x in stops[ei:]])

    try:
        assert not decreasing([x['call_time'] for x in calls])
    except AssertionError:
        logging.error('decreasing calls. trip %s vehicle %s', run[0].trip_id, run[0].vehicle_id)
        return []

    try:
        assert increasing([x['call_time'] for x in calls])
    except AssertionError:
        logging.info('non-increasing calls. trip %s vehicle %s', run[0].trip_id, run[0].vehicle_id)

    return calls


def increasing(L):
    return all(x <= y for x, y in zip(L, L[1:]))


def decreasing(L):
    return all(x > y for x, y in zip(L, L[1:]))


def track_vehicle(vehicle_id, calls_table, date, connectionstring, positions_table=None):
    positions_table = positions_table or 'positions'
    with psycopg2.connect(connectionstring) as conn:
        logging.info('STARTING %s', vehicle_id)
        with conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
            runs = filter_positions(cursor, date, positions_table, vehicle_id)
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
                trip_id = common([x.trip_id for x in run])

                # Get the scheduled list of stops for this trip.
                stoptimes = get_stoptimes(cursor, trip_id, date)

                if any(x.distance is None for x in stoptimes):
                    logging.warning('Missing stoptimes for %s', trip_id)
                    continue

                # Generate (infer) calls.
                calls = generate_calls(run, stoptimes)

                # write calls to sink
                cursor.executemany(
                    INSERT.format(calls_table),
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
    parser.add_argument('--calls-table', type=str, default='calls')
    parser.add_argument('--positions-table', type=str, default='positions')
    parser.add_argument('--vehicle', type=str)
    parser.add_argument('--debug', action='store_true')

    args = parser.parse_args()

    psycopg2.extensions.register_type(DEC2FLOAT)

    if args.vehicle:
        vehicles = [args.vehicle]
    else:
        with psycopg2.connect(args.connectionstring) as conn:
            with conn.cursor() as cursor:
                cursor.execute(SELECT_VEHICLE.format(args.positions_table), (args.date,))
                vehicles = [x[0] for x in cursor.fetchall()]

        logging.info('Found %s vehicles', len(vehicles))

    itervehicles = zip(vehicles,
                       cycle([args.calls_table]),
                       cycle([args.date]),
                       cycle([args.connectionstring]),
                       cycle([args.positions_table]),
                       )

    if args.debug:
        logging.info("debug mode")
        for i in itervehicles:
            track_vehicle(*i)
    else:
        with Pool(os.cpu_count()) as pool:
            pool.starmap(track_vehicle, itervehicles)

    logging.info("completed %s", args.date)


if __name__ == '__main__':
    main()
