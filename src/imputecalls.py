#!/user/bin/env python3
from __future__ import division
import sys
import os
from multiprocessing import Pool
import logging
from datetime import timedelta
from collections import Counter
from itertools import chain, tee, repeat
import MySQLdb
import MySQLdb.cursors
from get_config import get_config

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
    CONVERT_TZ(p.timestamp_utc, 'UTC', 'EST') AS arrival,
    st.arrival_time scheduled_time,
    rt.trip_index trip,
    next_stop_id next_stop,
    dist_from_stop,
    pattern_id pattern,
    dist_along_route,
    service_date,
    vehicle_id,
    stop_sequence
FROM positions p
    INNER JOIN ref_trips rt ON (rt.trip_id = p.trip_id)
    INNER JOIN ref_trip_patterns tp ON (rt.trip_index = tp.trip_index)
    INNER JOIN ref_stop_times st ON (
        rt.trip_index = st.trip_index
        AND p.next_stop_id = st.stop_id
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
    stop_id id, arrival_time AS time, rds_index, stop_sequence
    FROM ref_stop_times WHERE trip_index = %s
    ORDER BY stop_sequence ASC"""

INSERT = """INSERT IGNORE INTO calls
    (vehicle_id, trip_index, rds_index, stop_sequence, call_time, source, deviation)
    VALUES ({}, {}, %s, %s, %s, %s, 555)"""


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


def extrapolate(call1, call2, stoptime1, stoptime2):
    # Return the extrapolated duration between
    # stoptime1 and stoptime2 based on observed duration between call1 and call2
    assert call2[2] > call1[2]
    assert stoptime2['time'] > stoptime1['time']
    call_dur = (call2[2] - call1[2]).total_seconds()
    call_sched_dur = max((stoptime2['time'] - stoptime1['time']).total_seconds(), 1.)
    sched_dur = stoptime2['time'] - stoptime1['time']
    return timedelta(seconds=sched_dur.total_seconds() * (call_dur / call_sched_dur))


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
    ratio = (next_call[2] - prev_call[2]).total_seconds() / max(1., (next_st['time'] - prev_st['time']).total_seconds())

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
    calls = []
    # Ensure stop sequences increment by 1
    i = 1
    stop_sequencer = {}
    try:
        for s in stoptimes:
            s['stop_sequence_original'] = s['stop_sequence']
            stop_sequencer[s['stop_sequence_original']] = s['stop_sequence'] = i
            i += 1

        for x in run:
            x['stop_sequence_original'] = x['stop_sequence']
            try:
                x['stop_sequence'] = stop_sequencer[x['stop_sequence_original']]

            except KeyError:
                stop_sequencer[x['stop_sequence']] = x['stop_sequence']
    
    except Exception as err:
        logging.error('stop sequencing failed: %s', repr(err))
        raise err
        return []

    # pairwise iteration: scheduled stoptime and next scheduled stoptime
    for stoptime, next_stoptime in pairwise(stoptimes):
        call = find_call(run, stoptime, next_stoptime)
        if call is not None:
            calls.append(call)

    # Bail if we can't impute anything good
    if len(calls) < len(stoptimes) / 2:
        return []

    recorded_stops = [c[1] for c in calls]

    # Optionally add in at start/end stops, easier when we know the next/previous call
    try:
        if stoptimes[-1]['stop_sequence_original'] not in recorded_stops:
            delta = extrapolate(calls[-2], calls[-1], stoptimes[-2], stoptimes[-1])
            calls.append([
                stoptimes[-1]['rds_index'],
                stoptimes[-1]['stop_sequence_original'],
                calls[-1][2] + delta,
                'E']
            )
            # Insert a dummy position call right after the imputed first stop
            run.append({
                'departure': calls[-1][2] + delta + timedelta(seconds=1),
                'scheduled_time': stoptimes[-1]['time'] + timedelta(seconds=1),
                'stop_sequence': stoptimes[-1]['stop_sequence'] + 1,
                'i_am_a_dummy': True,
            })
            run[-1]['arrival'] = run[-1]['departure']
            recorded_stops.append(stoptimes[-1]['stop_sequence_original'])

        if stoptimes[0]['stop_sequence_original'] not in recorded_stops:
            delta = extrapolate(calls[1], calls[2], stoptimes[0], stoptimes[1])
            calls.insert(0, [
                stoptimes[0]['rds_index'],
                stoptimes[0]['stop_sequence_original'],
                calls[0][2] - delta,
                'S']
            )
            # Insert a dummy position call right before the imputed first stop
            run.insert(0, {
                'next_stop': stoptimes[0]['id'],
                'departure': calls[0][2] - delta - timedelta(seconds=1),
                'scheduled_time': stoptimes[0]['time'],
                'stop_sequence': stoptimes[0]['stop_sequence'],
                'i_am_a_dummy': True
            })
            run[0]['arrival'] = run[0]['departure']
            recorded_stops.append(stoptimes[0]['stop_sequence_original'])

    except Exception as err:
        logging.error(err)
        logging.error('failed extrapolation: %s', str(run[0]))
        return []

    # Now do imputations
    try:
        for stoptime in stoptimes:
            if stoptime['stop_sequence_original'] in recorded_stops:
                continue

            new_calls = impute_calls(stoptime['stop_sequence_original'], calls, stoptimes)
            if len(new_calls):
                calls.extend(new_calls)
                recorded_stops.extend([c[1] for c in new_calls])

    except Exception as err:
        logging.error('imputation failure %s %s', repr(err), err)
        return []

    calls.sort(key=lambda x: x[1])
    return calls


def process_vehicle(vehicle_id, date, config):
    conn = MySQLdb.connect(**config)
    print('STARTING', vehicle_id, file=sys.stderr)
    with conn.cursor() as cursor:
        # returns list in memory
        runs = filter_positions(cursor, vehicle_id, date)

        # each run will become a trip
        for run in runs:
            if len(run) == 0:
                continue
            elif len(run) <= 3:
                logging.info('missing positions for run, v_id=%s, %s', vehicle_id, run[0]['arrival'])
                continue

        # get the scheduled list of trips for this run
        trip_index = common([x['trip'] for x in run])
        cursor.execute(SELECT_TRIP_INDEX, (trip_index,))

        calls = generate_calls(run, cursor.fetchall())

            # write calls to sink
            insert = INSERT.format(vehicle_id, trip_index)
            cursor.executemany(insert, calls)
            conn.commit()

    sink.close()

def main(db_name, date, vehicle=None, config=None):
    # connect to MySQL
    config = config or get_config()
    config['unix_socket'] = '/tmp/mysql.sock'
    config['db'] = db_name
    config['cursorclass'] = MySQLdb.cursors.DictCursor

    if vehicle:
        vehicles = [vehicle]
    else:
        conn = MySQLdb.connect(**config)
        vehicles = fetch_vehicles(conn.cursor(), date)
        conn.close()

    count = len(vehicles)
    itervehicles = zip(vehicles, repeat(date, count), repeat(config, count))

    with Pool(os.cpu_count() - 1) as pool:
        pool.starmap(process_vehicle, itervehicles)

    print("SUCCESS: Committed %s" % date, file=sys.stderr)


if __name__ == '__main__':
    main(*sys.argv[1:])
