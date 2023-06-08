import pandas as pd
import click
from pyarrow._flight import FlightClient

from man_etl.etl_pipelines.core.base import Extractor, Transformer
from man_etl.etl_pipelines.csv_to_arctic import ArcticStorer
from man_etl.etl_pipelines.util.utils import SERVER_MAPPINGS
from pyarrow import flight
from dataclasses import dataclass

VENDOR_FILEPATH = "ons/cpi.parquet"


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




class CPITransformer(Transformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_data = df
        return transformed_data


@click.command()
@click.option("--library", prompt="Enter library name", default="test")
@click.option("--csv-path", prompt="Enter path for csv files", default="../data/")
def arrow_flight_loader():
    extractor = ArrowFlightExtractor.parquet(filepath=VENDOR_FILEPATH)
    data = extractor.extract()
    transformed_data = CPITransformer().transform(data)
    storer = ArcticStorer(destination="ons", to_store=transformed_data)
    storer.store()
