#!/user/bin/env python3.5
from os import path
from collections import namedtuple
import unittest
import psycopg2
import inferno


def increasing(L):
    return all(x <= y for x, y in zip(L, L[1:]))


def monotonically_increasing(L):
    return all(x <= y for x, y in zip(L, L[1:]))


class TestInferno(unittest.TestCase):

    dirname = path.dirname(__file__)
    connstr = 'dbname=inferno'
    vehicle_id = '8500'
    service_date = '2017-05-20'

    @classmethod
    def setUpClass(cls):
        psycopg2.extensions.register_type(inferno.DEC2FLOAT)
        cls._connection = psycopg2.connect(cls.connstr)

    @classmethod
    def tearDownClass(cls):
        cls._connection.close()

    def test_calls(self):
        with self._connection.cursor() as cursor:
            runs = inferno.filter_positions(cursor, self.service_date, self.vehicle_id)
            run = runs[0]
            trip = inferno.common([x['trip_id'] for x in run])
            stoptimes = inferno.get_stoptimes(cursor, trip)

        calls = inferno.generate_calls(run, stoptimes)

        # No duplicates
        assert len(calls) == len(set(c['call_time'] for c in calls))

        # Monotonically increasing
        self.assertTrue(monotonically_increasing([x['call_time'] for x in calls]))

    def test_vehicle_query(self):
        args = {'vehicle': self.vehicle_id, 'date': self.service_date}

        with self._connection.cursor() as curs:
            curs.execute(inferno.VEHICLE_QUERY, args)
            result = curs.fetchall()

        self.assertEqual(902, len(result))
        self.assertTrue(all(result[0]))

    def test_common(self):
        a = ['a', 'a', 'b']
        self.assertEqual('a', inferno.common(a))
        b = ['a', 'a', 'b', 'c', 'd', 'D']
        self.assertEqual('a', inferno.common(b))

    def test_mask(self):
        a = [1, 2, False, 3]
        def key(a, b):
            return a and b
        self.assertSequenceEqual([1, 2], inferno.mask2(a, key))

    def test_desc2fn(self):
        nt = namedtuple('a', ['name'])
        a = [nt('foo'), nt('bar')]
        self.assertSequenceEqual(('foo', 'bar'), inferno.desc2fn(a))

    def test_filter_positions(self):
        with self._connection.cursor() as cursor:
            runs = inferno.filter_positions(cursor, self.service_date, self.vehicle_id)

        self.assertIsInstance(runs, list)

        for run in runs:
            # Same vehicle in every run
            assert set([r['vehicle_id'] for r in run]) == set([self.vehicle_id])
            # Only one trip id per run
            assert len(set([r['trip_id'] for r in run])) == 1
            # increasing distance
            self.assertTrue(increasing([r['distance'] for r in run]))

    def test_track_vehicle(self):
        inferno.track_vehicle(self.vehicle_id, 'calls', self.service_date, self.connstr)

if __name__ == '__main__':
    unittest.main()
