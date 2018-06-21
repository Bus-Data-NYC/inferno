#!/user/bin/env python3.5

# Copyright 2017-18 TransitCenter http://transitcenter.org

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#  http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import division
import sys
import os
import getpass
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
logformatter = logging.Formatter(fmt='%(levelname)s (%(lineno)3d) %(asctime)s %(message)s')
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
MIN_EXTRAP_DIST = 1

# The number of positions to use when extrapolating.
EXTRAP_LENGTH = 5

# Maximum number of stops to extrapolate forward or backward
EXTRAP_COUNT = 2

# Doing one complicated thing in this query.
# Some bus routes are loops with tails (e.g. B74):
#    +--+
#    |  |---- (start and end)
#    +——+
# ST_LineLocatePoint can't handle this, so we use the mostly-untrustworthy
# "positions"."dist_along_route" column to limit the part of the shape_geom
# we examine to a fraction of the LineString.
VEHICLE_QUERY = """
SELECT
    EXTRACT(EPOCH FROM timestamp) AS time,
    trip_id,
    trip_start_date date,
    stop_sequence seq,
    (CASE WHEN
        dist_along_route is NULL and dist_from_stop is NULL
    THEN ST_LineLocatePoint(
        ST_Transform(r.the_geom, %(epsg)s),
        ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), %(epsg)s)
        ) * CASE %(epsg)s WHEN 4326 THEN r.length ELSE ST_Length(ST_Transform(r.the_geom, %(epsg)s)) END
    ELSE safe_locate(
        ST_Transform(r.the_geom, %(epsg)s),
        ST_Transform(ST_SetSRID(ST_MakePoint(longitude, latitude), 4326), %(epsg)s),
        -- greatest lower-bound is 500m from end of route, lowest is 0, default is 500m before estimated position
        LEAST(length - 500, GREATEST(0, dist_along_route - dist_from_stop - 500)),
        -- greatest upper-bound is length, lowest is 100m from start, default is 100m past stop
        LEAST(length, GREATEST(dist_along_route, 0) + 100),
        (CASE %(epsg)s WHEN 4326 THEN r.length ELSE ST_Length(ST_Transform(r.the_geom, %(epsg)s)) END)::numeric
    ) END
    )::numeric(10, 2) AS distance
FROM {0} p
    LEFT JOIN gtfs.trips USING (trip_id)
    -- TODO: change to LEFT JOIN when fix implemented for orphan stops
    INNER JOIN gtfs.stop_times st USING (feed_index, trip_id, stop_id)
    LEFT JOIN gtfs.shape_geoms r USING (feed_index, shape_id)
WHERE
    vehicle_id = %(vehicle)s
    AND trip_start_date = %(date)s::date
ORDER BY "timestamp"
"""

SELECT_VEHICLE = """SELECT DISTINCT vehicle_id
    FROM {0} WHERE trip_start_date = %s"""

SELECT_CALLED_VEHICLES = """SELECT vehicle_id FROM calls
    WHERE source = 'I' AND call_time::date = %s
    GROUP BY vehicle_id"""

SELECT_STOPTIMES = """SELECT
    feed_index,
    stop_id,
    wall_timez(DATE %(date)s, arrival_time, agency_timezone) AS datetime,
    DATE %(date)s as date,
    route_id,
    direction_id,
    stop_sequence AS seq,
    shape_dist_traveled distance
FROM gtfs.trips
    LEFT JOIN gtfs.agency USING (feed_index)
    LEFT JOIN gtfs.stop_times USING (feed_index, trip_id)
    LEFT JOIN gtfs.stops USING (feed_index, stop_id)
WHERE trip_id = %(trip)s
    AND feed_index = (
        SELECT MAX(feed_index)
        FROM gtfs.trips
            LEFT JOIN gtfs.calendar USING (feed_index, service_id)
        WHERE trip_id = %(trip)s
            AND date %(date)s BETWEEN start_date and end_date
    )
ORDER BY stop_sequence ASC
"""

SELECT_STOPTIMES_PLAIN = """SELECT DISTINCT
    feed_index,
    stop_id,
    wall_timez(date %(date)s, arrival_time, agency_timezone) AS datetime,
    date %(date)s as date,
    route_id,
    direction_id,
    shape_dist_traveled distance
FROM gtfs.trips
    LEFT JOIN gtfs.agency USING (feed_index)
    LEFT JOIN gtfs.stop_times USING (feed_index, trip_id)
    LEFT JOIN gtfs.stops USING (feed_index, stop_id)
WHERE trip_id = %(trip)s
ORDER BY stop_sequence ASC;
"""

INSERT = """INSERT INTO {}
    (vehicle_id, trip_id, direction_id, stop_id, run_index,
        call_time, source, deviation, feed_index, date)
    VALUES (%(vehicle)s, %(trip)s, %(direction_id)s, %(stop_id)s, currval('run_index'),
        %(call_time)s, %(source)s, %(deviation)s, %(feed_index)s, %(date)s)
    ON CONFLICT DO NOTHING"""


def common(lis: list):
    return Counter(lis).most_common(1)[0][0]


def mask(lis: list, key: Callable, keep_last=None) -> list:
    '''
    Create a mask on `lis` using the `key` function.
    `key` will be evaluated on pairs of items in `lis`.
    Returned list will only include items where `key` evaluates to True.
    Arguments:
        keep_last (boolean): In a sequence of items where key() is False,
                             keep the last one.
    '''
    result = [lis[0]]
    for item in lis[1:]:
        if key(item, result[-1]):
            result.append(item)
        elif keep_last is True:
            result[-1] = item
    return result


def desc2fn(description: tuple) -> tuple:
    '''Extract tuple of field names from psycopg2 cursor.description.'''
    return tuple(d.name for d in description)


def compare_dist(a, b):
    try:
        return a.distance >= b.distance
    except TypeError:
        # Don't be lenient when there's bad data: return False.
        return False


def toutc(timestamp):
    return datetime.utcfromtimestamp(timestamp).replace(tzinfo=pytz.UTC)


def get_positions(cursor, positions_table, query_args):
    '''
    Compile list of positions for a vehicle, using a list of positions
    and filtering based on positions that reflect change in pattern or next_stop.
    '''
    runs = []
    query = VEHICLE_QUERY.format(positions_table or 'positions')

    # load up cursor with every position for vehicle
    cursor.execute(query, query_args)
    if cursor.rowcount == 0:
        logging.warning('No rows found for %s on %s', query_args['vehicle'], query_args['date'])
        return []

    # dummy position for comparison with first row
    prev = namedtuple('prev', ('distance', 'trip_id'))(0, None)

    for position in cursor:
        # If we're on a new trip, start a new run
        if not position.trip_id == prev.trip_id:
            runs.append([])

        # If the distance has not declined, append the position
        if compare_dist(position, prev) or position.trip_id != prev.trip_id:
            runs[-1].append(position)
            prev = position

        position = cursor.fetchone()

    return runs


def filter_positions(runs):
    '''Filter runs to elimate shorties.'''
    return [run for run in runs if len(run) > 2 and len(set(r.seq for r in run)) > 1]


def get_stoptimes(cursor, tripid, date):
    logging.debug('Fetching stoptimes for %s', tripid)
    fields = {'trip': tripid, 'date': date}

    cursor.execute(SELECT_STOPTIMES, fields)

    if cursor.rowcount == 0:
        logging.warning("Couldn't find any stoptimes in date range, running simple query: %s", tripid)
        logging.debug(cursor.query.decode('utf8'))
        cursor.execute(SELECT_STOPTIMES_PLAIN, fields)

    return cursor.fetchall()


def extrapolate(run, stoptimes, method=None):
    '''
        Extrapolating is hard. Depending on the input data points, extrapolated
        data could produce impossible results, e.g. an extrapoled time being less
        than a known time. This is true even for linear extrapolations.
        This function may run multiple extrapolations, counterintuitively using less
        data until a reasonable result is obtained. In the extreme, a linear extrapolation
        from two observations will always provide a plausible (if rough) estimate.
    '''
    xs = [x.distance for x in run]
    ys = [x.time for x in run]
    data = [x.distance for x in stoptimes]
    result = []

    # Use builtin comparison functions.
    # Operations are symmetric when extrapolating forward vs. backward.
    if method == 'E':
        # Extrapolate forward (to End).
        compare = ys[-1].__lt__

    elif method == 'S':
        # Extrapolate backward (to Start).
        compare = ys[0].__gt__

    else:
        raise ValueError("Invalid direction")

    # Try to ensure that the extrapolated values are consistent with
    # the previous values by using shorter versions of the run when necessary
    while len(ys) > 1:
        slope, intercept = np.polyfit(xs, ys, 1)
        result = [slope * x + intercept for x in data]

        if slope > 0 and all(compare(y) for y in result):
            # Got a legal extrapolation, return calls.
            # Slope should always be > 0, if it isn't there's a serious data issue.
            break

        else:
            result = []
            # Slice from the beginning (if forward) or end (if backward)
            # of the run.
            logging.debug('Invalid extrap. method: %s. slope: %s. comparison: %s',
                          method,
                          round(slope, 2),
                          [compare(y) for y in result])
            logging.debug('new extrap length: %s', len(xs) - 1)
            xs.pop(0 if method == 'E' else -1)
            ys.pop(0 if method == 'E' else -1)

    return [call(s, t, method) for s, t in zip(stoptimes, result)]


def call(stoptime, seconds, method=None):
    '''
    Returns a dict with route, direction, stop, call time and source.
    Call time is in UTC.
    '''
    result = dict(stoptime._asdict(), call_time=toutc(seconds), source=method or 'I')
    result['deviation'] = result['call_time'] - stoptime.datetime
    return result


def generate_calls(run: list, stops: list, mintime=None, maxtime=None) -> list:
    '''
    list of calls to be written
    Args:
        run: list generated from enumerate(positions)
        stoptimes: list of scheduled stoptimes for this trip
        mintime: don't extrapolate back before this time
        maxtime: don't extrapolate forward past this time
    '''
    obs_distances = [p.distance for p in run]
    obs_times = [p.time for p in run]
    stop_positions = [x.distance for x in stops]

    # Get the range of stop positions that can be interpolated based on data.
    # The rest will be extrapolated
    si = bisect_left(stop_positions, obs_distances[0])
    ei = bisect(stop_positions, obs_distances[-1])

    logging.debug('min, max\t%s\t%s', mintime, maxtime)
    logging.debug('this run\t%s\t%s', toutc(obs_times[0]), toutc(obs_times[-1]))

    if not stops[si:ei]:
        logging.debug('No calls because no stops between si (%s) and ei (%s)',
                      obs_distances[0], obs_distances[-1])
        logging.debug('Stop distance range: %s - %s', min(stop_positions), max(stop_positions))
        return []

    # Interpolate main chunk of positions.
    interpolated = np.interp(stop_positions[si:ei], obs_distances, obs_times)
    calls = [call(stop, secs) for stop, secs in zip(stops[si:ei], interpolated)]

    # Goal is to only extrapolate based on unique distances,
    # When extrapolating forward, keep the oldest figure for a particular distance;
    # when extrapolating back, keep the newest.
    back_mask = mask(run, lambda x, y: x.distance > y.distance + MIN_EXTRAP_DIST)[:EXTRAP_LENGTH]
    forward_mask = mask(run, lambda x, y: x.distance > y.distance + MIN_EXTRAP_DIST, keep_last=True)[-EXTRAP_LENGTH:]

    # Extrapolate back for stops that occurred before observed positions.
    if si > 0 and len(back_mask) > 1:
        logging.debug('extrapolating backward. si = %s', si)
        try:
            backward = extrapolate(back_mask, stops[si - EXTRAP_COUNT: si], 'S')
            if mintime:
                backward = [x for x in backward if x['call_time'] > mintime]
            calls = backward + calls

        except Exception as error:
            logging.warning('%s -- Ignoring back extrapolation: %s ', run[0].trip_id, error)

    # Extrapolate forward to the stops after the observed positions.
    if ei < len(stops) and len(forward_mask) > 1:
        logging.debug('extrapolating forward. ei = %s', ei)
        try:
            forward = extrapolate(forward_mask, stops[ei: ei + EXTRAP_COUNT], 'E')
            if maxtime:
                forward = [x for x in forward if x['call_time'] < maxtime]
            calls.extend(forward)

        except Exception as error:
            logging.warning('%s -- Ignoring forward extrapolation: %s', run[0].trip_id, error)

    try:
        assert increasing([x['call_time'] for x in calls])
    except AssertionError:
        logging.info('%s -- non-increasing calls', run[0].trip_id)
        logging.debug("calc'ed call times: %s", [x['call_time'].timestamp() for x in calls])
        logging.debug('observed positions: %s', obs_distances)
        logging.debug('observed times: %s', obs_times)
        logging.debug('stop positions: %s', stop_positions)

    return calls


def increasing(L):
    return all(x <= y for x, y in zip(L, L[1:]))


def track_vehicle(vehicle_id, query_args: dict, conn_kwargs: dict, calls_table, positions_table=None):
    positions_table = positions_table or 'positions'
    query_args['vehicle'] = vehicle_id

    with psycopg2.connect(**conn_kwargs) as conn:
        logging.info('STARTING %s', vehicle_id)
        with conn.cursor(cursor_factory=NamedTupleCursor) as cursor:
            rawruns = get_positions(cursor, positions_table, query_args)
            # filter out short runs and ones with few stops
            runs = filter_positions(rawruns)

            if len(rawruns) > len(runs):
                logging.debug('skipping %d short runs, query: %s', len(rawruns)-len(runs), query_args)

            # Compute temporal bounds of each run.
            starts = [None] + [toutc(run[0].time) for run in runs[:-1]]
            ends = [toutc(run[-1].time) for run in runs[1:]] + [None]

            # Counter is just for logging.
            lenc = 0

            # each run will become a trip
            for run, start, end in zip(runs, starts, ends):
                if not run:
                    continue
                elif len(run) <= 2:
                    logging.debug('short run (%d positions), v_id=%s, %s',
                                  len(run), query_args['vehicle'], run[0].time)
                    continue

                # Assume most common trip is the correct one.
                trip_id = common([x.trip_id for x in run])

                # Get the scheduled list of stops for this trip.
                stoptimes = get_stoptimes(cursor, trip_id, query_args['date'])

                if any(x.distance is None for x in stoptimes):
                    logging.warning('Missing stoptimes for %s', trip_id)
                    continue

                # Generate (infer) calls.
                calls = generate_calls(run, stoptimes, mintime=start, maxtime=end)

                # update run_index sequence
                cursor.execute("SELECT nextval('run_index')")

                # write calls to sink
                cursor.executemany(
                    INSERT.format(calls_table),
                    [dict(trip=trip_id, vehicle=vehicle_id, **c) for c in calls]
                )

                lenc += len(calls)
                conn.commit()
                logging.debug('%s', cursor.statusmessage)

            logging.info('COMMIT vehicle= %s, calls= %s', vehicle_id, lenc)


def connection_params():
    pg = {
        'PGUSER': 'user',
        'PGHOST': 'host',
        'PGPORT': 'port',
        'PGPASSWORD': 'password',
        'PGPASSFILE': 'passfile',
    }
    params = {'dbname': os.environ.get('PGDATABASE', getpass.getuser())}
    params.update({v: os.environ[k] for k, v in pg.items() if k in os.environ})
    return params


def main():  # pragma: no cover
    # connectionstring: str, table, date, vehicle=None
    parser = argparse.ArgumentParser()
    parser.add_argument('date', type=str)
    parser.add_argument('--calls-table', type=str, default='calls')
    parser.add_argument('--positions-table', type=str, default='positions')
    parser.add_argument('--vehicle', type=str)
    parser.add_argument('--epsg', type=int, default=4326,
                        help='projection in which to calculate distances')
    parser.add_argument('--debug', action='store_true')
    parser.add_argument('--quiet', action='store_true')
    parser.add_argument('--incomplete', action='store_true', help='Restart an incomplete date')

    args = parser.parse_args()

    psycopg2.extensions.register_type(DEC2FLOAT)

    conn_kwargs = connection_params()

    if args.vehicle:
        vehicles = [args.vehicle]
    else:
        with psycopg2.connect(**conn_kwargs) as conn:
            with conn.cursor() as cursor:
                logging.info('Finding vehicles for %s', args.date)
                cursor.execute(SELECT_VEHICLE.format(args.positions_table), (args.date,))
                vehicles = [x[0] for x in cursor.fetchall()]

                if args.incomplete:
                    logging.info('Removing already-called vehicles')
                    cursor.execute(SELECT_CALLED_VEHICLES, (args.date,))
                    called = set([x[0] for x in cursor.fetchall()])
                    vehicles = set(vehicles).difference(called)
                    logging.info('Removed %s', len(called))

        logging.info('Found %s vehicles', len(vehicles))

    itervehicles = zip(vehicles,
                       cycle([{'date': args.date, 'epsg': args.epsg}]),
                       cycle([conn_kwargs]),
                       cycle([args.calls_table]),
                       cycle([args.positions_table]),
                       )

    if args.quiet:
        logger.setLevel(logging.WARNING)

    if args.debug:
        logging.info("debug mode")
        logger.setLevel(logging.DEBUG)
        for i in itervehicles:
            track_vehicle(*i)
    else:
        with Pool(os.cpu_count()) as pool:
            pool.starmap(track_vehicle, itervehicles)

    logging.info("completed %s", args.date)


if __name__ == '__main__':
    main()
