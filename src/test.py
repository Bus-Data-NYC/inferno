#!/user/bin/env python3
import unittest
import psycopg2
import imputecalls
import pytz
import data.positions


class TestImpute(unittest.TestCase):

    connstr = 'dbname=nycbus'

    def test_calls(self):
        calls = imputecalls.generate_calls(data.positions.run, data.positions.stoptimes)

        # No duplicates
        assert len(calls) == len(set(c['call_time'] for c in calls))

        # Monotonically increasing
        prev = calls[0]['call_time']

        for call in calls[1:]:
            assert call['call_time'] > prev
            prev = call['call_time']

        imputecalls.main(self.connstr, table='calls', date='2017-05-20', vehicle='8500')

        trip = data.positions.run[0]['trip_id']

        calltimes = [x['call_time'].time() for x in calls]

        with psycopg2.connect(self.connstr) as conn:
            with conn.cursor() as cursor:
                cursor.execute('SELECT call_time FROM calls WHERE trip_id = %s', (trip,))

                for row in cursor.fetchall():
                    self.assertIn(row[0].time(), calltimes)


if __name__ == '__main__':
    unittest.main()
