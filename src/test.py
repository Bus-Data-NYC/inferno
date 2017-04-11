#!/user/bin/env python3
from datetime import datetime, timedelta
import unittest
import imputecalls


class TestImpute(unittest.TestCase):

    def test_extrapolate_start(self):
        # call format:
        # [rds_index, stop_sequence, datetime, source]
        call_1 = [11188, 2, datetime(2016, 1, 1, 11, 1, 18), 'C']
        call_2 = [11189, 3, datetime(2016, 1, 1, 11, 1, 51), 'C']

        stoptime_1 = {
            'id': 551247,
            'time': timedelta(hours=11),
            'rds_index': 11187,
            'stop_sequence': 1
        }
        stoptime_2 = {
            'id': 551248,
            'time': timedelta(hours=11, minutes=1),
            'rds_index': 11188,
            'stop_sequence': 2
        }

        # extrapolate back
        delta = imputecalls.extrapolate(call_1, call_2, stoptime_1, stoptime_2)

        assert call_1[2] - delta < call_1[2]

    def test_extrapolate_end(self):
        call_1 = [11188, 2, datetime(2016, 1, 1, 11, 1, 18), 'C']
        call_2 = [11189, 3, datetime(2016, 1, 1, 11, 1, 51), 'C']

        stoptime_1 = {
            'id': 551247,
            'time': timedelta(hours=11),
            'rds_index': 11187,
            'stop_sequence': 1
        }
        stoptime_2 = {
            'id': 551248,
            'time': timedelta(hours=11, minutes=1),
            'rds_index': 11188,
            'stop_sequence': 2
        }

        # extrapolate back
        delta = imputecalls.extrapolate(call_1, call_2, stoptime_1, stoptime_2)

        assert call_1[2] + delta > call_1[2]

if __name__ == '__main__':
    unittest.main()
