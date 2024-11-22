import yfinance as yf
from pandas import DataFrame, Series
from datetime import datetime, timedelta

def get_period_data(org: str|yf.Ticker,
             start: datetime|str=None,
             end: datetime|str=None,
             interval: str="1d") -> DataFrame:
    """
    Метод получения цен на акции за определнный период.

    Args:
        org: имя организации.
        start: начало периода.
        end: конец периода.
        interval: интервал отсчётов в периоде.

    Returns:
        Таблица с данными изменения цен за период.
    """
    if end != None and start == None:
        raise KeyError()
    if start == None:
        start = datetime.now()-timedelta(days=365)
    if end == None:
        end = datetime.now()
    return yf.download(org, start=start, end=end, interval=interval)

def get_last_data(org: str|yf.Ticker, 
                  period: timedelta=None,
                  interval: str="1m") -> Series:
    """
    Метод получения последнего изменения цен на акции.

    Args:
        org: имя организации.
        period: период, за который рассматриваются изменения.
        interval: интервал изменений.

    Returns:
        Ряд последнего изменения цены акции.
    """
    if period == None:
        period = timedelta(days=1)
    data = yf.download(org, start=(datetime.now()-period), end=datetime.now(), interval=interval)
    return data.iloc[-1]

def convert_str_to_sec(interval: str) -> int:
    """
    Метод конвертации строки интервала времени в число секунд.

    Args:
        interval: интервал времени.

    Returns:
        Число секунд.
    """
    if interval[-1] == "s":
        return int(interval[:-1])
    if interval[-1] == "m":
        return 60 * int(interval[:-1])
    if interval[-1] == "h":
        return 3600 * int(interval[:-1])
    if interval[-1] == "d":
        return 3600 * 24 * int(interval[:-1])
    if interval[-1] == "w":
        return 3600 * 24 * 7 * int(interval[:-1])
    if interval[-2:] == "mo":
        return 3600 * 24 * 30 * int(interval[:-2])
    if interval[-1] == "y":
        return 3600 * 24 * 365 * int(interval[:-1])

if __name__ == "__main__":
    pass