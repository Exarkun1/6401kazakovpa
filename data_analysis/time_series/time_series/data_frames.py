"""
Модуль вспомогательных функций обработки таблиц.
"""
import typing as tp
import functools
import numpy as np
from pandas import DataFrame, ExcelWriter

def get_rows(data : DataFrame) -> tp.Iterable[dict[str, object]]:
    """
    Итератор по строкам таблицы.

    Args:
        data: таблица, по строкам которой проводится итерация.

    Returns:
        Итератор, возвращающий словари, содержащие строки таблицы.
    """
    for i in range(0, len(data)):
        row = {data.index.name: data.index[i]}
        for index, value in data.iloc[i].items():
            row[index] = value
        yield row

def get_col_arrays(data : DataFrame, 
                   col : str) -> tuple[np.ndarray, np.ndarray]:
    """
    Функция получения массивов индекса и столбца.

    Args:
        data: таблица, из которой необходимо получить столбец.
        col: имя столбца.

    Returns:
        Кортеж, содержащий массив индексов и массив столбца.
    """
    return (data.index.array, data[col].array)

def create_dataframe(data: np.ndarray | list,
                     data_name: str,
                     index: np.ndarray | list,
                     index_name: str) -> DataFrame:
    """
    Функция создания таблицы из одного столбца.

    Args:
        data: массив значений стобца.
        data_name: имя столбца.
        index: массив значений строк.
        index_name: имя строк.

    Returns:
        Таблица из одного столбца. 
    """
    return DataFrame(
        data={data_name : data}, 
        index=index
        ).rename_axis(index_name)

def join_all(*datas: list[DataFrame]) -> DataFrame:
    """
    Функция соединения нескольких таблиц.

    Args:
        datas: список таблиц для соединения.

    Returns:
        Таблица из соединения таблиц.
    """
    res = datas[0]
    for i in range(1,len(datas)):
        res = res.join(datas[i])
    return res

def write_excel(data : DataFrame,
                path: str):
    """
    Функция записи таблицы в excel документ.

    Args:
        data: таблица, которую необходимо записать.
        path: путь к excel документу.
    """
    with ExcelWriter(path, engine='xlsxwriter') as writer:
        data.to_excel(writer, sheet_name='test', startrow=0, startcol=0)

def join_dataframes(
        func : tp.Callable[..., DataFrame]) -> tp.Callable[..., DataFrame]:
    """
    Декоратор, добавляющий результат функции в список на соединение,
    для добавления необходимо передать функции аргумент join_cols
    (список столбцов таблицы).

    Args:
        func: исходная функция.
    
    Returns:
        Функция с возможностью добавления результата в список 
        на соединение.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwards):
        try:
            join_cols = kwards.pop("join_cols")
            data = func(*args, **kwards)
            JoinedDataframe().add(data[join_cols])
            return func(*args, **kwards)
        except KeyError:
            return func(*args, **kwards)
    return wrapper

class JoinedDataframe:
    """
    Объект, хранящий список таблиц на соединнение.
    """
    _instance = None

    def __new__(cls):
        if cls._instance == None:
            cls._instance = super(__class__, cls).__new__(cls)
            cls._instance._dataframes = []
        return cls._instance
    
    def add(self,
            data: DataFrame):
        """
        Метод добавления в список на соединение.

        Args:
            data: таблица для добавления.
        """
        self._dataframes.append(data)

    def get_join(self) -> DataFrame:
        """
        Метод возврата соединнной таблицы.

        Returns:
            Соединённая таблица.
        """
        return join_all(*self._dataframes)
    
    def write(self, 
              path: str):
        """
        Метод записи соединённой таблицы в excel документ.

        Args:
            path: путь к excel документу.
        """
        write_excel(self.get_join(), path)

    def clear(self):
        """
        Метод очистки списка таблиц на соединение.
        """
        self._dataframes.clear()

if __name__ == "__main__":
    pass