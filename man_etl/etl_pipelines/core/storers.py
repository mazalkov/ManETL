import pandas as pd
import logging
from typing import Dict

from arcticdb.version_store.library import Library
from pyarrow import flight, Table
from pyarrow._flight import FlightClient

from man_etl.etl_pipelines.core.base import Storer
from dataclasses import dataclass
from man_etl.etl_pipelines.util.utils import SERVER_MAPPINGS

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')


@dataclass
class ArcticStorer(Storer):
    destination: Library
    to_store: Dict
    
    def store(self):
        for sym in self.to_store:
            data = self.to_store[sym]
            self.destination.write(sym, data)

class ArrowFlightStorer(Storer):
    def __init__(self, library_name, endpoint, to_store):
        self.library_name = library_name
        self.endpoint = endpoint
        self.to_store = to_store

    @property
    def client_connection(self) -> FlightClient:
        yield flight.connect(self.endpoint)

    def store(self):
        with self.client_connection as client:
            for symbol, data in self.to_store:
                data_table = Table.from_pandas(data)
                upload_descriptor = flight.FlightDescriptor.for_path(f"{self.library_name}/{symbol}.parquet")
                writer, _ = client.do_put(upload_descriptor, data_table.schema)
                writer.write_table(data_table)
                writer.close()
    @classmethod
    def parquet(cls, to_store: Dict[str, pd.DataFrame], library_name: str):
        return cls(endpoint=SERVER_MAPPINGS["parquet"], to_store=to_store, library_name=library_name)
