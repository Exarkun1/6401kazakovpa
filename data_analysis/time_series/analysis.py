"""
Модуль класса обработки временных рядов.
"""
import pandas as pd
import numpy as np
import datetime
from pandas import DataFrame

from time_series.data_frames import create_dataframe

class TimeSeriesAnalyser:
    """
    Класс обработки временных рядов.
    """
    def __init__(self, 
                 data: DataFrame,
                 interval: datetime.timedelta=None):
        """
        Args:
            data: временной ряд в форме таблицы.
            interval: минимальный интервал ряда.
        """
        self._data = data
        self._n = data.shape[0]
        self._interval_name = data.index.name
        self._interval = None

        if interval == None:
            self._interval = data.index[1] - data.index[0]
            for i in range(2, self._n):
                interval = data.index[i] - data.index[i-1]
                if self._interval > interval:
                    self._interval = interval
        else:
            self._interval = interval

    def get_array(self, 
                  col: str) -> np.ndarray:
        """
        Метод получения массива значений временного ряда.

        Args:
            col: имя столбца временного ряда.
        Returns:
            Массив значений столбца временного ряда.
        """
        return self._data[col].array
    
    @property
    def index(self) -> np.ndarray:
        """
        Свойство получения массива переодов временного ряда.

        Returns:
            Массив периодов временного ряда.
        """
        return self._data.index.array
    
    @property
    def interval(self) -> datetime.timedelta:
        """
        Свойство для получения минимального интервала временного ряда.

        Returns:
            Минимальный интервал временного ряда.
        """
        return self._interval

    def find_min(self, 
                 col: str="Open") -> DataFrame:
        """
        Метод поиска минимального значения временного ряда.

        Args:
            col: имя столбца врменного ряда.

        Returns:
            Таблица с временем/датой и минимальным значением.
        """
        values = self.get_array(col)
        index_min = np.argmin(values)

        return create_dataframe(
            data=[values[index_min]],
            data_name="Min",
            index=[self.index[index_min]],
            index_name=self._interval_name
        )
    
    def find_max(self, 
                 col: str="Open") -> DataFrame:
        """
        Метод поиска максимального значения временного ряда.

        Args:
            col: имя столбца временного ряда.

        Returns:
            Таблица с временем/датой и максимальным значением.
        """
        values = self.get_array(col)
        index_max = np.argmax(values)

        return create_dataframe(
            data=[values[index_max]],
            data_name="Max",
            index=[self.index[index_max]],
            index_name=self._interval_name
        )

    def find_extremes(self, 
                      col: str="Open") -> DataFrame:
        """
        Метод поиска экстремумов временного ряда.

        Args:
            col: имя столбца временного ряда.

        Returns:
            Таблица с временем/датой и значениями экстремумов.
        """
        min_data = self.find_min(col).rename(columns={"Min": "Extreme"})
        max_data = self.find_max(col).rename(columns={"Max": "Extreme"})

        return pd.concat([min_data, max_data], axis=0)
    
    def differentiate(self, 
                      col: str="Open") -> DataFrame:
        """
        Метод вычисления дифференциала временного ряда.

        Args:
            col: имя столбца временного ряда.

        Returns:
            Таблица дифференциала временного ряда.
        """
        values = self.get_array(col)
        intervals = (self.index[1:self._n] - self.index[0:self._n-1]) / self.interval
        diffs = (values[1:self._n] - values[0:self._n-1])

        return create_dataframe(
            data=diffs/intervals,
            data_name="Diff",
            index=self.index[0:self._n-1],
            index_name=self._interval_name
        )
        

    def calc_movavg(self, 
                    window: int | datetime.timedelta, 
                    col: str="Open") -> DataFrame:
        """
        Метод вычисления скользящего среднего временного ряда.

        Args:
            window: окно, по которому вычисляется скользящее среднее.
            col: имя столбца временного ряда.

        Returns:
            Таблица скользящего среднего временного ряда.
        """
        if isinstance(window, int):
            return self._calc_movarg_int(window, col)
        if isinstance(window, datetime.timedelta):
            return self._calc_movarg_timedelta(window, col)
    
    def _calc_movarg_int(self, 
                         window: int, 
                         col: str="Open") -> DataFrame:
        """
        Метод вычисления скользящего среднего временного ряда 
        по целочисленному окну.

        Args:
            window: окно, по которому вычисляется скользящее среднее.
            col: имя столбца временного ряда.

        Returns:
            Таблица скользящего среднего временного ряда.
        """
        if window < 1:
            raise Exception("Parameter window is less than 1.")
        values = self.get_array(col)
        movavgs = np.empty([self._n], dtype=float)

        for i in range(0, self._n):
            start_pos = i - window + 1
            if start_pos < 0:
                start_pos = 0
            movavgs[i] = np.average(values[start_pos:i+1])

        return create_dataframe(
            data=movavgs,
            data_name="Moving avg",
            index=self.index,
            index_name=self._interval_name
        )

    def _calc_movarg_timedelta(self, 
                               window: datetime.timedelta, 
                               col: str="Open") -> DataFrame:
        """
        Метод вычисления скользящего среднего временного ряда 
        по интервальному окну.

        Args:
            window: окно, по которому вычисляется скользящее среднее.
            col: имя столбца временного ряда.

        Returns:
            Таблица скользящего среднего временного ряда.
        """
        values = self.get_array(col)
        movavgs = np.empty([self._n], dtype=float)

        for i in range(0, self._n):
            sum_prices = 0
            n = 0
            for j in range(i, -1, -1):
                interval = self.index[i] - self.index[j]
                if interval <= window:
                    sum_prices += values[j]
                    n += 1
                else:
                    break
            movavgs[i] = sum_prices / n

        return create_dataframe(
            data=movavgs,
            data_name="Moving avg",
            index=self.index,
            index_name=self._interval_name
        )

    def calc_autocor(self, 
                     col: str="Open") -> DataFrame:
        """
        Метод для вычисления автокорреляции временного ряда.

        Args:
            col: имя столбцавременного ряда.

        Returns:
            Таблица автокорреляции временного ряда.
        """
        values = self.get_array(col)
        x = values[1:self._n]
        y = values[0:self._n-1]
        avg_x = np.average(x)
        var_x = np.var(x)
        avg_y = np.average(y)
        var_y = np.var(y)
        autocor = (x - avg_x)/var_x * (y - avg_y)/var_y
        
        return create_dataframe(
            data=autocor,
            data_name="Autocor",
            index=self.index[1:self._n],
            index_name=self._interval_name
        )