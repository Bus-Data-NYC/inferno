#!/user/bin/env python
import sys
import os.path
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


def get_positions(cursor, vehicle_id, start, end):
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

    filtered_positions = []
    prev = {}
    position = cursor.fetchone()

    while position is not None:
        if (position['pattern'] != prev.get('pattern') or
                position['next_stop'] != prev.get('next_stop') or
                position['timestamp'] > prev.get('timestamp', 0) + MAX_TIME_BETWEEN_STOPS):

            if len(filtered_positions) and filtered_positions[-1] != prev:
                filtered_positions.append(prev)

            filtered_positions.append(position)

        elif (position['dist_from_stop'] and position['dist_from_stop'] < STOP_THRESHOLD):
            filtered_positions.append(position)

        prev = position
        position = cursor.fetchone()

    return filtered_positions


def assign_trips(runs):
    '''Ensure that each run has the same trip index (use the more'''
    # TODO
    return runs


def fetch_stop_times(cursor):
    '''
    Get first departure, arrival offset, departure offset, call_type.
    from ref_stop_times sorted by trip index and stop sequence'''
    pass


def fetch_trips(cursor):
    '''trip index, i from ref_stop_times'''
    pass


def fetch_vehicles(cursor, start, end):
    cursor.execute(
        "SELECT DISTINCT vehicle_id FROM positions WHERE timestamp_utc BETWEEN %s AND %s",
        (start, end)
    )
    return [row['vehicle_id'] for row in cursor.fetchall()]


def main(db_name, start_date, end_date, outfile):
    # connect to MySQL
    config = get_config()
    source = MySQLdb.connect(db=db_name, cursorclass=MySQLdb.cursors.DictCursor, **config)

    cursor = source.cursor()

    sink = MySQLdb.connect(db=db_name, **config)

    # Get distinct vehicles from MySQL
    vehicles = fetch_vehicles(cursor, start_date, end_date)

    # Run query for every vehicle
    for vehicle_id in vehicles:
        positions = get_positions(cursor, vehicle_id, start_date, end_date)

        print(vehicle_id, len(positions))

        # rather than loopting through positions, loop through stops on the trip, and use the positions to get info on that.
        # Using positions, generate calls
        # if (position['pattern'] != prev_position.get('pattern') or
        #     position['stop_sequence'] < prev_position.get('stop_sequence') or
        #     position['timestamp'] > prev_position.get('timestamp') + MAX_TIME_BETWEEN_STOPS
        #     ):
        #     pass
        # Must refer to


if __name__ == '__main__':
    main(*sys.argv[1:])
