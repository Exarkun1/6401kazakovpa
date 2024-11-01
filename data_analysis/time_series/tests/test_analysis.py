"""
Модуль тестирования работы класса обработки временных рядов.
"""
import unittest
import datetime
import numpy as np
from pandas import DataFrame
from time_series import TimeSeriesAnalyser

class TestStockAnalyser(unittest.TestCase):
    def setUp(self):
        data = DataFrame(
            data={"Open": [1, 2, 3, 4, 5, 6, 7, 8, 9]},
            index=[
                datetime.datetime(2024, 9, 1),
                datetime.datetime(2024, 9, 2),
                datetime.datetime(2024, 9, 3),
                datetime.datetime(2024, 9, 4),
                datetime.datetime(2024, 9, 5),
                datetime.datetime(2024, 9, 6),
                datetime.datetime(2024, 9, 7),
                datetime.datetime(2024, 9, 8),
                datetime.datetime(2024, 9, 9),
            ]
            )
        self.stock_analyser = TimeSeriesAnalyser(data)

    def test_find_blobal_extremes(self):
        data = self.stock_analyser.find_extremes(glb=True)
        self.assertEqual(data["Extreme"].array[0], 1)
        self.assertEqual(data["Extreme"].array[1], 9)

    def test_find_local_extremes(self):
        data = self.stock_analyser.find_extremes()
        self.assertEqual(len(data), 0)

    def test_differentiate(self):
        data = self.stock_analyser.differentiate()
        self.assertEqual(np.sum(data["Diff"].array), 8)

    def test_calc_movavg_int(self):
        data = self.stock_analyser.calc_movavg(window=4)
        self.assertEqual(data["Moving avg"].array[4], 3.5)

    def test_calc_movarg_timedelta(self):
        data = self.stock_analyser.calc_movavg(window=datetime.timedelta(days=3))
        self.assertEqual(data["Moving avg"].array[4], 3.5)

    def test_calc_autocor(self):
        data = self.stock_analyser.calc_autocor()
        self.assertTrue(1 - data["Autocor"].array[4] < 1e12)

if __name__ == "__main__":
    unittest.main()