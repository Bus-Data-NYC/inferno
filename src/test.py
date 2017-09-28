#!/user/bin/env python3.5
from os import path
import logging
from datetime import datetime
from collections import namedtuple
import unittest
import psycopg2
from psycopg2.extras import NamedTupleCursor
from pytz import utc
import inferno

# Clam up, logging!
logger = logging.getLogger()
logger.setLevel(logging.INFO)


def monotonically_increasing(L):
    return all(x < y for x, y in zip(L, L[1:]))


class TestInferno(unittest.TestCase):

    dirname = path.dirname(__file__)
    connstr = 'dbname=inferno'
    vehicle_id = '8500'
    service_date = '2017-05-20'

    Position = namedtuple('position', ['seq', 'distance', 'trip_id'])

    @classmethod
    def setUpClass(cls):
        psycopg2.extensions.register_type(inferno.DEC2FLOAT)
        cls._connection = psycopg2.connect(cls.connstr, cursor_factory=NamedTupleCursor)
        with cls._connection.cursor() as c:
            print('TRUNCATE TABLE calls')
            c.execute('TRUNCATE TABLE calls')

        cls.sequence_data = [cls.Position(x[0], x[1], 'x') for x in [
            (25, 6515.72), (26, 6763.42), (27, 6856.14), (28, 6848.21),
            (29, 6848.21), (31, 6848.21), (30, 6848.21)]]

    @classmethod
    def tearDownClass(cls):
        cls._connection.close()

    def test_calls(self):
        '''call generator'''
        with self._connection.cursor(cursor_factory=NamedTupleCursor) as cursor:
            runs = inferno.get_positions(cursor, self.service_date, 'positions', self.vehicle_id)

            for run in inferno.filter_positions(runs):
                trip = inferno.common([x.trip_id for x in run])

                stoptimes = inferno.get_stoptimes(cursor, trip, self.service_date)
                self.assertGreater(len(stoptimes), 0)
                self.assertEqual(len(stoptimes), len(set(x.id for x in stoptimes)), 'No duplicate stoptimes')

                calls = inferno.generate_calls(run, stoptimes)
                self.assertGreater(len(calls), 0, 'non-zero calls')

                self.assertTrue(monotonically_increasing([x['call_time']
                                                          for x in calls]), 'Monotonically increasing call times')
                self.assertEqual(len(calls), len(set(c['call_time'] for c in calls)), 'No duplicate calls')

                self.assertEqual(len(calls), len(stoptimes), 'Same number of calls as stop times in (%s)' % trip)

    def test_vehicle_query(self):
        args = {'vehicle': self.vehicle_id, 'date': self.service_date}

        with self._connection.cursor() as curs:
            curs.execute(inferno.VEHICLE_QUERY.format('positions'), args)
            result = curs.fetchall()

        self.assertTrue(all(result[0]))

    def test_common(self):
        a = ['a', 'a', 'b']
        self.assertEqual('a', inferno.common(a))
        b = ['a', 'a', 'b', 'c', 'd', 'D']
        self.assertEqual('a', inferno.common(b))

    def test_mask(self):
        a = [1, 2, False, 3]
        def key(a, b):
            return bool(a and b)
        self.assertSequenceEqual([1, 2, 3], inferno.mask(a, key))

        obj = [self.Position(x, x, '.') for x in (1, 2, 3)]
        self.assertSequenceEqual(obj, inferno.mask(obj, inferno.compare_seq))

        wrong = [self.Position(x, x, '.') for x in (1, 2, 0, 3)]
        self.assertSequenceEqual(obj, inferno.mask(wrong, inferno.compare_seq))

        lis = [1, 2, 3, 2, 2, 2, 4, 5]
        result = inferno.mask(lis, lambda x, y: x >= y)
        self.assertEqual(result, [1, 2, 3, 4, 5])

    def test_compare_dist(self):
        result = inferno.mask(self.sequence_data, inferno.compare_dist)
        assert inferno.increasing([x.distance for x in result])

    def compare_seq(self):
        a, b = {'seq': 1}, {'seq': 2}

        self.assertFalse(inferno.compare_seq(a, b))
        self.assertTrue(inferno.compare_seq(b, a))
        self.assertTrue(inferno.compare_seq(a, a))

        result = inferno.mask(self.sequence_data, inferno.compare_seq)
        assert inferno.increasing([x['seq'] for x in result])

    def test_increasing(self):
        a = [1, 2, 3]
        b = [1, 2, 2, 3]
        c = [1, 2, 3, 2]
        d = [1]

        self.assertTrue(inferno.increasing(a))
        self.assertTrue(inferno.increasing(b))
        self.assertFalse(inferno.increasing(c))
        self.assertTrue(inferno.increasing(d))

    def test_desc2fn(self):
        nt = namedtuple('a', ['name'])
        a = [nt('foo'), nt('bar')]
        self.assertSequenceEqual(('foo', 'bar'), inferno.desc2fn(a))

    def test_samerun(self):
        a = self.Position(1, 1, 'x')
        b = self.Position(2, 2, 'x')
        c = self.Position(2, 3, 'z')
        d = self.Position(0, 4, 'x')

        self.assertTrue(inferno.samerun(a, b))
        self.assertFalse(inferno.samerun(a, c))
        self.assertFalse(inferno.samerun(a, d))

    def _stoptimes(self, trip, date):
        with self._connection.cursor() as cursor:
            stoptimes = inferno.get_stoptimes(cursor, trip, date)

        self.assertTrue(not any([x.seq is None for x in stoptimes]), 'No None sequences')
        self.assertTrue(not any([x.datetime is None for x in stoptimes]), 'No None datetimes')
        self.assertTrue(not any([x.distance is None for x in stoptimes]), 'No None distances')
        self.assertTrue(monotonically_increasing([x.distance for x in stoptimes]), 'Monotonically increasing dist')
        self.assertTrue(monotonically_increasing([x.datetime for x in stoptimes]), 'Monotonically increasing time')
        self.assertTrue(monotonically_increasing([x.seq for x in stoptimes]), 'Monotonically increasing sequence')

    def test_get_stoptimes(self):
        self._stoptimes('UP_B7-Weekday-SDon-119500_B74_605', self.service_date)
        self._stoptimes('QV_B7-Saturday-038500_MISC_120', self.service_date)

    def _run_tst(self, runs):
        for run in runs:
            # Only one trip id per run
            try:
                self.assertEqual(len(set([r.trip_id for r in run])), 1, 'only one trip id per run')
            except AssertionError:
                raise AssertionError(set([r.trip_id for r in run]))

            # increasing distance
            try:
                self.assertTrue(inferno.increasing([r.distance for r in run]), 'increasing distance')
            except AssertionError:
                errs = [(i, datetime.fromtimestamp(r.time).strftime('%c'), r.distance)
                        for i, r in enumerate(run, 1)]
                raise AssertionError(errs)

    def test_get_positions(self):
        with self._connection.cursor() as cursor:
            # Check that imaginary vehicle returns nothing
            runs = inferno.get_positions(cursor, self.service_date, 'positions', 'magic schoolbus')
            self.assertEqual(len(runs), 0)

            runs = inferno.get_positions(cursor, self.service_date, 'positions', self.vehicle_id)
            self.assertIsInstance(runs, list)

            runs2 = inferno.get_positions(cursor, self.service_date, 'positions', '7149')

        try:
            self._run_tst(inferno.filter_positions(runs))
        except AssertionError:
            raise AssertionError('Run test failed for', self.service_date, self.vehicle_id)
        try:
            self._run_tst(inferno.filter_positions(runs))
        except AssertionError:
            raise AssertionError('Run test failed for', self.service_date, '7149')

    def test_track_vehicle(self):
        inferno.track_vehicle(self.vehicle_id, 'calls', self.service_date, self.connstr)

    def test_call(self):
        StopTime = namedtuple('stoptime', ('feed_index', 'datetime', 'id', 'route_id', 'direction_id'))

        stoptime = StopTime(1, datetime(2017, 5, 30, 23, 46, 15, tzinfo=utc), 'abc', 'lorem ipsum', 0)
        seconds = 1496188035
        dt = datetime(2017, 5, 30, 23, 47, 15, tzinfo=utc)
        fixture = {
            'route_id': stoptime.route_id,
            'direction_id': stoptime.direction_id,
            'stop_id': stoptime.id,
            'call_time': dt,
            'feed_index': 1,
            'deviation': dt - stoptime.datetime,
            'source': 'I'
        }
        c1 = inferno.call(stoptime, seconds)
        self.assertEqual(c1, fixture)

        fixture['source'] = 'X'
        c2 = inferno.call(stoptime, seconds, 'X')
        self.assertEqual(c2, fixture)

        return c1

    def test_insert(self):
        call = self.test_call()
        call.update({'vehicle': 123, 'trip': 'xyz'})
        with self._connection.cursor() as cursor:
            cursor.execute(inferno.INSERT.format('calls'), call)
        self._connection.commit()

        with self._connection.cursor() as cursor:
            cursor.execute('truncate table calls')
        self._connection.commit()

    def test_wall_time(self):
        '''Test the wall_time postgressql function'''
        with self._connection.cursor() as cursor:
            cursor.execute("""SELECT
                wall_time('2017-03-10'::date, %(time)s::interval, 'America/New_York'::text) a,
                wall_time('2017-03-11'::date, %(time)s::interval, 'America/New_York'::text) b,
                wall_time('2017-03-12'::date, %(time)s::interval, 'America/New_York'::text) c
            """, {'time': '27:00:00'}
            )
            r = cursor.fetchone()

        self.assertEqual(r.a.astimezone(utc), datetime(2017, 3, 11, 3 + 5, 0, tzinfo=utc))
        self.assertEqual(r.b.astimezone(utc), datetime(2017, 3, 12, 4 + 4, 0, tzinfo=utc))
        self.assertEqual(r.c.astimezone(utc), datetime(2017, 3, 13, 3 + 4, 0, tzinfo=utc))

    def test_queries(self):
        data = {'trip': 'QV_B7-Weekday-SDon-145500_MISC_320', 'date': '2017-05-20'}

        with self._connection.cursor() as cursor:
            cursor.execute(inferno.SELECT_STOPTIMES, data)
            if cursor.rowcount == 0:
                print(cursor.query.decode('utf8'))
                raise AssertionError('No result for query')

            for row in cursor.fetchall():
                assert isinstance(row[0], int)
                assert isinstance(row[2], datetime)
                assert isinstance(row[6], float)

            cursor.execute(inferno.SELECT_STOPTIMES_PLAIN, data)
            if cursor.rowcount == 0:
                print(cursor.query.decode('utf8'))
                raise AssertionError('No result for query')

            for row in cursor.fetchall():
                assert isinstance(row[2], datetime)
                assert isinstance(row[6], float)

if __name__ == '__main__':
    unittest.main()
