import logging.config
import json
import time
import logging
from datetime import datetime, timedelta

from services import StockObserver

if __name__ == "__main__":
    with(open("configs/logger_config.json", "r") as file):
        json_data = file.read()
        config_obj = json.loads(json_data)
        logging.config.dictConfig(config_obj)
    msft_observer = StockObserver("MSFT", timedelta(days=5), "1m")
    ibm_observer = StockObserver("IBM", timedelta(days=5), "1m")

    msft_observer.start("Open", 20, "stock_update.txt")
    ibm_observer.start("Open", 20, "stock_update.txt")
    time.sleep(180)
    msft_observer.stop()
    ibm_observer.stop()