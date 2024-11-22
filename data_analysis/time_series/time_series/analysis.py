"""
Модуль класса обработки временных рядов.
"""
import pandas as pd
import numpy as np
import datetime
import logging
from pandas import DataFrame

from time_series.time_series.data_frames import create_dataframe, join_dataframes

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
        self.__data = data
        self.__n = data.shape[0]
        self.__interval_name = data.index.name
        self.__interval = None
        self.__logger = logging.getLogger(__name__)

        if interval is None:
            self.__interval = data.index[1] - data.index[0]
            for i in range(2, self.__n):
                interval = data.index[i] - data.index[i-1]
                if self.__interval > interval:
                    self.__interval = interval
        else:
            self.__interval = interval

    def get_array(self, 
                  col: str) -> np.ndarray:
        """
        Метод получения массива значений временного ряда.

        Args:
            col: имя столбца временного ряда.
        Returns:
            Массив значений столбца временного ряда.
        """
        try:
            return self.__data[col].array
        except:
            error = KeyError("Попытка получения не существующего столбца.")
            self.__logger.error(error)
            raise error
    
    @property
    def index(self) -> np.ndarray:
        """
        Свойство получения массива переодов временного ряда.

        Returns:
            Массив периодов временного ряда.
        """
        return self.__data.index.array
    
    @property
    def interval(self) -> datetime.timedelta:
        """
        Свойство для получения минимального интервала временного ряда.

        Returns:
            Минимальный интервал временного ряда.
        """
        return self.__interval

    def find_min(self, 
                 col: str="Open") -> DataFrame:
        """
        Метод поиска глобального минимального значения временного ряда.

        Args:
            col: имя столбца врменного ряда.

        Returns:
            Таблица с минимальным значением временного ряда.
        """
        values = self.get_array(col)
        index_min = np.argmin(values)

        return create_dataframe(
            data=[values[index_min]],
            data_name="Min",
            index=[self.index[index_min]],
            index_name=self.__interval_name
        )
    
    def find_max(self, 
                 col: str="Open") -> DataFrame:
        """
        Метод поиска глобального максимального значения временного ряда.

        Args:
            col: имя столбца временного ряда.

        Returns:
            Таблица с максимальным значением временного ряда.
        """
        values = self.get_array(col)
        index_max = np.argmax(values)

        return create_dataframe(
            data=[values[index_max]],
            data_name="Max",
            index=[self.index[index_max]],
            index_name=self.__interval_name
        )
    
    @join_dataframes
    def find_extremes(self,
                      glb=False,
                      col: str="Open") -> DataFrame:
        """
        Метод поиска экстремумов временного ряда.

        Args:
            glb: тип экстремумов (глобальные, если True,
            локальные, если False).
            col: имя столбца временного ряда.

        Returns:
            Таблица с экстремумами временного ряда.
        """
        if glb:
            return self._find_glb_extremes(col)
        else:
            return self._find_loc_extremes(col)

    def _find_glb_extremes(self, 
                           col: str="Open") -> DataFrame:
        """
        Метод поиска глобальных экстремумов временного ряда.

        Args:
            col: имя столбца временного ряда.

        Returns:
            Таблица с глобальными экстремумами временного ряда.
        """
        min_data = self.find_min(col).rename(columns={"Min": "Extreme"})
        min_data["Type"] = "Min"
        max_data = self.find_max(col).rename(columns={"Max": "Extreme"})
        max_data["Type"] = "Max"
        return pd.concat([min_data, max_data], axis=0)
    
    def _find_loc_extremes(self,
                           col: str="Open") -> DataFrame:
        """
        Метод поиска локальных экстремумов временного ряда.

        Args:
            col: имя столбца временного ряда.

        Returns:
            Таблица с локальными экстремумами временного ряда.
        """
        values = self.get_array(col)
        indexes = []
        types = []
        
        for i in range(1, self.__n-1):
            if values[i] <= values[i-1] and values[i] <= values[i+1]:
                indexes.append(i)
                types.append("Min")
            elif values[i] >= values[i-1] and values[i] >= values[i+1]:
                indexes.append(i)
                types.append("Max")
        
        data = create_dataframe(
            data=values[indexes],
            data_name="Extreme",
            index=self.index[indexes],
            index_name=self.__interval_name
        )
        data["Type"] = types
        return data

    @join_dataframes        
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
        intervals = (self.index[1:self.__n] - self.index[0:self.__n-1]) / self.interval
        diffs = (values[1:self.__n] - values[0:self.__n-1])

        result = create_dataframe(
            data=diffs/intervals,
            data_name="Diff",
            index=self.index[0:self.__n-1],
            index_name=self.__interval_name
        )
        self.__logger.debug("Вычислен дифференциал временного ряда.")
        return result
        
    @join_dataframes
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
        result = None
        if isinstance(window, int):
            result = self._calc_movarg_int(window, col)
        if isinstance(window, datetime.timedelta):
            result = self._calc_movarg_timedelta(window, col)
        self.__logger.debug("Вычислено скользящее среднее временного ряда.")
        return result
    
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
            error = ValueError("Попытка передачи отрицательного окна.")
            self.__logger.error(error)
            raise error
        values = self.get_array(col)
        movavgs = np.empty([self.__n], dtype=float)

        for i in range(0, self.__n):
            start_pos = i - window + 1
            if start_pos < 0:
                start_pos = 0
            movavgs[i] = np.average(values[start_pos:i+1])

        return create_dataframe(
            data=movavgs,
            data_name="Moving avg",
            index=self.index,
            index_name=self.__interval_name
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
        movavgs = np.empty([self.__n], dtype=float)

        for i in range(0, self.__n):
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
            index_name=self.__interval_name
        )

    @join_dataframes
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
        autocor = np.empty([self.__n-1])
        for i in range(0, self.__n-1):
            x = values[i:self.__n]
            y = values[0:self.__n-i]
            avg_x = np.average(x)
            avg_y = np.average(y)
            avg_xy = np.average(x*y)
            std_x = np.std(x)
            std_y = np.std(y)
            autocor[i] = (avg_xy - avg_x*avg_y) / (std_x*std_y)

        result = create_dataframe(
            data=autocor,
            data_name="Autocor",
            index=self.index[0:self.__n-1],
            index_name=self.__interval_name
        )
        self.__logger.debug("Вычислена атокорреляция временного ряда.")
        return result
    
if __name__ == "__main__":
    pass