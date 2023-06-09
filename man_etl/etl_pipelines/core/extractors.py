import os
from contextlib import contextmanager

import pandas as pd
import logging
from man_etl.etl_pipelines.core.base import Extractor
from arcticdb.version_store.library import Library
from typing import List, Dict
from dataclasses import dataclass
from datetime import datetime, timedelta
import yfinance as yf

from pyarrow._flight import FlightClient

from man_etl.etl_pipelines.util.definitions import TRANSFORM
from man_etl.etl_pipelines.util.utils import SERVER_MAPPINGS
from pyarrow import flight

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Logger")


@dataclass
class CSVExtractor(Extractor):
    csv_path: str

    def extract(self) -> pd.DataFrame:
        csv_list = os.listdir(self.csv_path)
        csv_hash = {}
        for csv in csv_list:
            stock = csv.split(".csv")[0]
            csv_hash[stock] = pd.read_csv(f"{self.csv_path}/{csv}")
        return csv_hash


class YFExtractor(Extractor):
    symbols: List = ["AAPL", "GOOGL", "MSFT"]

    def extract(self):
        data = {}
        end = datetime.now().date()
        start = end - timedelta(days=365)
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")
        for symbol in self.symbols:
            data[symbol] = yf.download(
                symbol, start=start_str, end=end_str, interval="1d"
            )
        return data


@dataclass
class ArcticExtractor(Extractor):
    library: Library

    def extract(self, symbol: str) -> pd.DataFrame:
        return self.library.read(symbol).data

    def extract_many(self) -> Dict:
        data = {}
        for sym in self._get_raw_symbols():
            data[sym] = self.extract(sym)
        return data

    def extract_all(self):
        data = {}
        for sym in self.library.list_symbols():
            data[sym] = self.extract(sym)
        return data

    def _get_raw_symbols(self):
        return [sym for sym in self.library.list_symbols() if TRANSFORM not in sym]


class ArrowFlightExtractor(Extractor):
    def __init__(self, endpoint, filepath):
        self.endpoint = endpoint
        self.filepath = filepath

    @contextmanager
    def client_connection(self) -> FlightClient:
        try:
            yield flight.connect(self.endpoint)
        finally:
            logger.info("closed connection")

    def extract(self) -> pd.DataFrame:
        with self.client_connection() as client:
            self.vendor_flight = client.get_flight_info(
                flight.FlightDescriptor.for_path(self.filepath)
            )
            reader = client.do_get(self.vendor_flight.endpoints[0].ticket)
            data_table = reader.read_all()
            return data_table.to_pandas()

    @classmethod
    def parquet(cls, filepath: str):
        return cls(endpoint=SERVER_MAPPINGS["parquet"], filepath=filepath)

    @classmethod
    def arctic(cls, library, symbol):
        raise NotImplementedError
