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

from os import environ, path
import logging
from datetime import date, datetime
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
    connargs = {'dbname': 'inferno'}
    vehicle_id = '8500'
    service_date = '2017-05-20'
    epsg = 3628
    Position = namedtuple('position', ['seq', 'distance', 'trip_id', 'time'])
    StopTime = namedtuple('stoptime', ['feed_index', 'stop_id', 'datetime', 'route_id',
                                       'direction_id', 'seq', 'distance', 'date'])

    @classmethod
    def setUpClass(cls):
        psycopg2.extensions.register_type(inferno.DEC2FLOAT)
        cls._connection = psycopg2.connect(cursor_factory=NamedTupleCursor, **cls.connargs)
        with cls._connection.cursor() as c:
            print('TRUNCATE TABLE calls')
            c.execute('TRUNCATE TABLE calls')

            c.execute("SELECT nextval('run_index')")

        cls.query_args = {'vehicle': cls.vehicle_id, 'date': cls.service_date, 'epsg': cls.epsg}

        cls.sequence_data = [cls.Position(x[0], x[1], 'x', x[2]) for x in [
            (25, 6515.72, 100), (26, 6763.42, 110), (27, 6856.14, 120), (28, 6848.21, 130),
            (29, 6848.21, 140), (31, 6848.21, 150), (30, 6848.21, 160)]]

    @classmethod
    def tearDownClass(cls):
        cls._connection.close()

    def test_calls(self):
        '''call generator'''
        with self._connection.cursor(cursor_factory=NamedTupleCursor) as cursor:
            runs = inferno.get_positions(cursor, 'rt_vehicle_positions', self.query_args)

            for run in inferno.filter_positions(runs):
                trip = inferno.common([x.trip_id for x in run])

                stoptimes = inferno.get_stoptimes(cursor, trip, self.service_date)
                self.assertGreater(len(stoptimes), 0)
                self.assertEqual(len(stoptimes), len(set(x.stop_id for x in stoptimes)), 'No duplicate stoptimes')

                try:
                    calls = inferno.generate_calls(run, stoptimes)
                    self.assertGreater(len(calls), 0, 'non-zero calls')
                except AssertionError:
                    print([r.seq for r in run])
                    raise

                self.assertTrue(monotonically_increasing([x['call_time']
                                                          for x in calls]), 'Monotonically increasing call times')
                self.assertEqual(len(calls), len(set(c['call_time'] for c in calls)), 'No duplicate calls')

                self.assertLessEqual(len(calls), len(stoptimes),
                                     'Roughly same number of calls as stop times in (%s)' % trip)

    def test_vehicle_query(self):

        with self._connection.cursor() as curs:
            curs.execute(inferno.VEHICLE_QUERY.format('rt_vehicle_positions'), self.query_args)
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

        lis = [1, 2, 3, 2, 2, 2, 4, 5]
        result = inferno.mask(lis, lambda x, y: x >= y)
        self.assertEqual(result, [1, 2, 3, 4, 5])

    def test_compare_dist(self):
        result = inferno.mask(self.sequence_data, inferno.compare_dist)
        assert inferno.increasing([x.distance for x in result])

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
            args = {'date': self.service_date, 'vehicle': 'magic schoolbus', 'epsg': self.epsg}
            runs = inferno.get_positions(cursor, 'rt_vehicle_positions', args)
            self.assertEqual(len(runs), 0)

            args['vehicle'] = self.vehicle_id
            runs = inferno.get_positions(cursor, 'rt_vehicle_positions', args)
            self.assertIsInstance(runs, list)

        try:
            self._run_tst(runs)
        except AssertionError:
            print(runs)
            raise AssertionError('Run test failed for', self.service_date, self.vehicle_id)
        try:
            self._run_tst(runs)
        except AssertionError:
            raise AssertionError('Run test failed for', self.service_date, '7149')

    def test_track_vehicle(self):
        inferno.track_vehicle(self.vehicle_id,
                              query_args={'date': self.service_date, 'epsg': self.epsg},
                              calls_table='calls',
                              conn_kwargs=self.connargs,
                              positions_table='rt_vehicle_positions'
                              )

    def test_call(self):
        dt1 = datetime(2017, 5, 30, 23, 46, 15, tzinfo=utc)
        stoptime = self.StopTime(1, 'id', dt1, 'route', 1, 2, 1.1, '2017-05-01')

        seconds = 1496188035
        dt2 = datetime(2017, 5, 30, 23, 47, 15, tzinfo=utc)

        fixture = {
            'direction_id': stoptime.direction_id,
            'stop_id': stoptime.stop_id,
            'call_time': dt2,
            'feed_index': stoptime.feed_index,
            'deviation': dt2 - stoptime.datetime,
            'seq': 2,
            'distance': 1.1,
            'date': '2017-05-01',
            'source': 'I',
        }
        c1 = inferno.call(stoptime, seconds)
        for k, v in fixture.items():
            self.assertEqual(v, c1[k])

        c2 = inferno.call(stoptime, seconds, 'X')
        self.assertEqual('X', c2['source'])

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
                wall_timez('2017-03-10'::date, %(time)s::interval, 'America/New_York'::text) a,
                wall_timez('2017-03-11'::date, %(time)s::interval, 'America/New_York'::text) b,
                wall_timez('2017-03-12'::date, %(time)s::interval, 'America/New_York'::text) c
            """, {'time': '27:00:00'}
            )
            r = cursor.fetchone()

        self.assertEqual(r.a.astimezone(utc), datetime(2017, 3, 11, 3 + 5, 0, tzinfo=utc))
        self.assertEqual(r.b.astimezone(utc), datetime(2017, 3, 12, 4 + 4, 0, tzinfo=utc))
        self.assertEqual(r.c.astimezone(utc), datetime(2017, 3, 13, 3 + 4, 0, tzinfo=utc))

    def test_queries(self):
        data = {'trip': 'QV_B7-Weekday-SDon-145500_MISC_320', 'date': '2017-05-20'}

        with self._connection.cursor(cursor_factory=NamedTupleCursor) as cursor:
            cursor.execute(inferno.SELECT_STOPTIMES, data)
            if cursor.rowcount == 0:
                print(cursor.query.decode('utf8'))
                raise AssertionError('No result for query')

            for row in cursor.fetchall():
                assert isinstance(row.feed_index, int)
                assert isinstance(row.datetime, datetime)
                assert isinstance(row.date, date)
                assert isinstance(row.seq, int)
                assert isinstance(row.distance, float)

            cursor.execute(inferno.SELECT_STOPTIMES_PLAIN, data)
            if cursor.rowcount == 0:
                print(cursor.query.decode('utf8'))
                raise AssertionError('No result for query')

            for row in cursor.fetchall():
                assert isinstance(row.datetime, datetime)
                assert isinstance(row.date, date)
                assert isinstance(row.distance, float)

    def test_extrapolate(self):
        with self.assertRaises(ValueError):
            inferno.extrapolate([], [], 'X')

        with self.assertRaises(IndexError):
            inferno.extrapolate([], [], 'E')

        # Test extrapolation on bowl-shaped data
        # Based on real data that caused problem in old extrapolator.
        ydata = (100, 125, 200, 400)
        dt2 = datetime(2017, 5, 30, 23, 47, 15, tzinfo=utc)

        stoptimes = [self.StopTime(None, None, dt2, None, None, None, a, None) for a in range(5, 7)]
        run = [self.Position(None, a, None, b) for a, b in enumerate(ydata, start=1)]

        calls = inferno.extrapolate(run, stoptimes, 'E')
        assert len(calls) == 2
        a = calls[0]['call_time'].timestamp()
        b = calls[1]['call_time'].timestamp()
        self.assertGreater(a, ydata[-1])
        self.assertGreater(b, ydata[-1])
        self.assertGreater(b, a)
        self.assertEqual(a, 450)
        self.assertEqual(b, 547.5)

        # Too-short input gets empty response
        self.assertSequenceEqual(inferno.extrapolate([run[0]], stoptimes, 'E'), [])

        # backward input gets empty response
        bad = [100, 90]
        badrun = [self.Position(None, a, None, b) for a, b in enumerate(bad, start=1)]
        self.assertSequenceEqual(inferno.extrapolate(badrun, stoptimes, 'E'), [])

    def test_params(self):
        params = inferno.connection_params()
        for p in ('PGUSER', 'PGHOST', 'PGPORT', 'PGPASSWORD', 'PGPASSFILE'):
            self.assertEqual(environ.get(p), params.get(p.replace('PG', '').lower()))

if __name__ == '__main__':
    unittest.main()
