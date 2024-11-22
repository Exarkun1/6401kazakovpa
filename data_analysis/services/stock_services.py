import yfinance as yf
import threading
import logging
import time
import pandas as pd
from pandas import DataFrame, Series
from datetime import datetime, timedelta

from time_series import TimeSeriesAnalyser
from services.stock_datas import get_period_data, get_last_data, convert_str_to_sec

class StopToken:
    """
    Класс токена останова задачи.
    """
    def __init__(self):
        self.__is_stopped = False

    @property
    def is_stopped(self) -> bool:
        """
        Свойство проверки остановки задачи.
        
        Returns:
            True, если задача должна быть остановлена.
        """
        return self.__is_stopped
    
    def stop(self):
        """
        Метод установки статуса остановки задачи.
        """
        self.__is_stopped = True

class StockObserver:
    """
    Сервис отслеживания изменений цен на акции в реальном времени.
    """
    def __init__(self,
                 org: str|yf.Ticker,
                 period: timedelta,
                 interval: str):
        """
        Конструктор сервиса.
        
        Args:
            org: имя организации.
            period: период, с которого начинаются измерения.
            interval: интервал обращения за новыми данными.
        """
        self.__org = org
        self.__period = period
        self.__interval = interval
        self.__stop_token = None
        self.__logger = logging.getLogger(f"{__name__}(id={id(self)})")
        self.__thread = None

    def start(self,
              col: str,
              window: int|timedelta,
              path: str):
        """
        Метод запуска сервиса.
        
        Args:
            col: имя столбца временного ряда.
            window: окно для скользящего среднего.
            path: путь к файлу для сохранения данных.
        """
        if self.__thread is not None:
            error = RuntimeError("Попытка повторного запуска сервиса.")
            self.__logger.error(error)
            raise error
        
        self.__stop_token = StopToken()
        self.__thread = threading.Thread(target=self.__run,
                                         args=(col,
                                               window,
                                               path,
                                               self.__stop_token))
        try:
            self.__thread.start()
            self.__logger.info("Сервис был запущен.")
        except:
            error = RuntimeError("Не удалось запустить сервис.")
            self.__logger.error(error)
            raise error

    def stop(self):
        """
        Метод остановки сервиса.
        """
        if self.__thread is None:
            self.__logger.warning("Попытка завершения незапущенного сервиса.")
            return
        
        self.__stop_token.stop()
        self.__thread.join()
        self.__thread = None
        self.__logger.info("Cервис был остановлен.")

    def __run(self,
              col: str,
              window: int|timedelta,
              path: str,
              token: StopToken):
        """
        Метод потока отслеживания изменения данных.

        Args:
            col: имя столбца временного ряда.
            window: окно для скользящего среднего.
            path: путь к файлу для сохранения данных.
            token: токен останова.
        """
        interval = timedelta(seconds=convert_str_to_sec(self.__interval))
        write_str = None

        try:
            data = get_period_data(self.__org,
                                   datetime.now()-self.__period,
                                   datetime.now(),
                                   self.__interval)
        except:
            error = RuntimeError("Не удалось загрузить начальные данные.")
            self.stop()
            self.__logger.error(error)
            raise error
        
        while(not token.is_stopped):
            try:
                last_data = get_last_data(self.__org, 
                                          self.__period, 
                                          self.__interval)
                if write_str is None:
                    write_str = self.__calc_write_str(data, interval, col, window)
            
            except:
                error = RuntimeError("Не удалось загрузить последние данные.")
                self.stop()
                self.__logger.error(error)
                raise error
            
            if last_data.name != data.index[-1]:
                data = pd.concat([data, last_data])
                write_str = self.__calc_write_str(data, interval, col, window)

            try:
                with(open(path, 'a') as  file):
                    file.write(write_str + "\n")
                self.__logger.info(f"Сервис записал последние цены на акций {self.__org}.")
            except:
                error = RuntimeError("Не удалось записать данные в файл.")
                self.stop()
                self.__logger.error(error)
                raise error
            time.sleep(convert_str_to_sec(self.__interval))

    def __calc_write_str(self,
                         data: DataFrame,
                         interval: timedelta,
                         col: str,
                         window: int|timedelta) -> str:
        """
        Метод вычисления строки для записи.

        Args:
            data: таблица с данными за весь период.
            interval: интервал обращения за новыми данными.
            col: имя столбца временного ряда.
            window: окно для скользящего среднего.

        Returns:
            Результирующая строка.
        """
        analyser = TimeSeriesAnalyser(data, interval)
        movavg_data = analyser.calc_movavg(window=window, col=col)
        last_movavg = movavg_data.iloc[-1]
        analyser = TimeSeriesAnalyser(movavg_data, interval)
        diff_data = analyser.differentiate(col="Moving avg")
        last_diff = diff_data.iloc[-1]
        autocor_data = analyser.calc_autocor(col="Moving avg")
        last_autocor = autocor_data.iloc[-1]
        write_str = self.__get_write_str(data.iloc[-1], col, 
                                         last_movavg, last_diff, last_autocor)
        return write_str

    def __get_write_str(self,
                        last_data: Series,
                        col: str,
                        last_movavg: Series,
                        last_diff: Series,
                        last_autocor: Series) -> str:
        """
        Метод получения строки для записи по последним изменениям данных.

        Args:
            last_data: последнее изменение цен.
            col: имя столбца временного ряда.
            last_movavg: последнее изменение скользящего среднего.
            last_diff: последнее изменение дифференциала.
            last_autocor: последнее изменение автокорреляции.

        Returns:
            Результирующая строка.
        """
        return (f"{self.__org} => "
                f"Date: {last_data.name}, {col}: {last_data[col]}," 
                f" Moving avg: {last_movavg["Moving avg"]},"
                f" Diff: {last_diff["Diff"]}, "
                f" Autocor: {last_autocor["Autocor"]}")

if __name__ == "__main__":
    pass
    