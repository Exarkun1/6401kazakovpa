"""
Модуль вспомогательных функций обработки таблиц.
"""
import typing as tp
import numpy as np
from pandas import DataFrame, ExcelWriter

def get_rows(data : DataFrame) -> tp.Iterable[dict[str, object]]:
    """
    Итератор по строкам таблицы.

    Args:
        data: таблица.

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
        data: таблица.
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

def get_col(data: DataFrame,
            col: str) -> DataFrame:
    return create_dataframe(
        data=data[col].array,
        data_name=col,
        index=data.index.array,
        index_name=data.index.name
    )

def join_all(*datas: list[DataFrame]) -> DataFrame:
    res = datas[0]
    for i in range(1,len(datas)):
        res = res.join(datas[i])
    return res

def write_excel(data : DataFrame,
                path: str):
    """
    Функция записи таблицы в excel документ.

    Args:
        data: таблица
        path: путь к excel документу.
    """
    with ExcelWriter(path, engine='xlsxwriter') as writer:
        data.to_excel(writer, sheet_name='test', startrow=0, startcol=0)

if __name__ == "__main__":
    pass