import os
import pandas as pd
import logging
from man_etl.etl_pipelines.core.base import Extractor
from arcticdb.version_store.library import Library
from typing import List, Dict
from dataclasses import dataclass
from man_etl.etl_pipelines.util.definitions import TRANSFORM

from pyarrow._flight import FlightClient
from man_etl.etl_pipelines.util.utils import SERVER_MAPPINGS
from pyarrow import flight

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')


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
    def extract(self):
        pass

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
    
    def _get_raw_symbols(self):
        return [sym for sym in self.library.list_symbols() if TRANSFORM not in sym]

class ArrowFlightExtractor(Extractor):
    def __init__(self, endpoint, filepath):
        self.endpoint = endpoint
        self.filepath = filepath

    @property
    def client_connection(self) -> FlightClient:
        yield flight.connect(self.endpoint)

    def extract(self) -> pd.DataFrame:
        with self.client_connection as client:
            self.vendor_flight = client.get_flight_info(flight.FlightDescriptor.for_path(self.filepath))
            reader = client.do_get(self.vendor_flight.endpoints[0].ticket)
            data_table = reader.read_all()
            return data_table.to_pandas()

    @classmethod
    def parquet(cls, filepath: str):
        return cls(endpoint=SERVER_MAPPINGS["parquet"], filepath=filepath)

    @classmethod
    def arctic(cls, library, symbol):
        raise NotImplementedError
