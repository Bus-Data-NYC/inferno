#!/user/bin/env python
import sys
from math import ceil
from vincenty import vincenty
import MySQLdb
import MySQLdb.cursors
from MySQLdb.constants import FIELD_TYPE
from get_config import get_config

'''
Calculate distance between two lng, lat points in feet.
Insert result in ref_stop_times table
'''
TRIPS_QUERY = '''
    SELECT DISTINCT trip_index trip
    FROM ref_stop_times st
    LEFT JOIN ref_distances d ON (
        d.stop_id = st.stop_id
        AND d.rds_index = st.rds_index
        AND d.stop_sequence = st.stop_sequence
    )
    WHERE d.distance IS NULL
    '''

QUERY = '''
    SELECT stop_id stop,
        trip_index trip,
        stop_lat lat,
        stop_lon lon,
        stop_sequence,
        rds_index,
        d.distance
    FROM ref_stop_times st
    LEFT JOIN ref_stops s USING (stop_id)
    LEFT JOIN ref_distances d USING (stop_id, rds_index, stop_sequence)
    WHERE trip_index = %s
    ORDER BY stop_sequence ASC
    '''

UPDATE = '''
    INSERT ref_distances (stop_id, stop_sequence, rds_index, distance)
    VALUES (%s, %s, %s, %s)
    '''


def listtrips(cursor):
    cursor.execute(TRIPS_QUERY)
    return cursor.fetchall()


def main(dbname):
    config = get_config()
    config['conv'] = {
        FIELD_TYPE.DECIMAL: float,
        FIELD_TYPE.NEWDECIMAL: float,
    }
    source = MySQLdb.connect(db=dbname, cursorclass=MySQLdb.cursors.DictCursor, **config)
    cursor = source.cursor()

    sink = MySQLdb.connect(db=dbname, **config)

    print('finding undistanced trips...')
    trips = listtrips(cursor)

    print('found {} trips.'.format(len(trips)))

    lat, lon = 0, 0
    for trip in trips:
        cursor.execute(QUERY, (trip['trip'],))
        row = cursor.fetchone()
        updates = []

        while row is not None:
            if row['distance'] is None:
                if row['stop_sequence'] == 1:
                    dist = 0.
                else:
                    # Points are in (lat, lng) order.
                    dist = vincenty((row['lat'], row['lon']), (lat, lon), miles=True)
                    dist = ceil(dist * 52800.) / 10

                updates.append((row['stop'], row['stop_sequence'], row['rds_index'], dist))

            lat, lon = row['lat'], row['lon']
            row = cursor.fetchone()

        if len(updates):
            print('updating', len(updates), 'rows')
            sink.cursor().executemany(UPDATE, updates)

    sink.commit()


if __name__ == '__main__':
    main(sys.argv[1])
