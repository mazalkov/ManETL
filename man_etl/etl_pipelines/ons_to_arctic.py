import pandas as pd
import click

from man_etl.etl_pipelines.core.base import Extractor, Transformer
from man_etl.etl_pipelines.csv_to_arctic import ArcticStorer
from man_etl.etl_pipelines.util.utils import SERVER_MAPPINGS
from pyarrow import flight
from dataclasses import dataclass
VENDOR_FILEPATH = "ons/cpi.parquet"

@dataclass
class ArrowFlightExtractor(Extractor):
    endpoint = SERVER_MAPPINGS["parquet"]

    def __init__(self, filepath):
        self.client = flight.connect(self.endpoint)
        self.vendor_flight = self.client.get_flight_info(flight.FlightDescriptor.for_path(filepath))


    def extract(self) -> pd.DataFrame:
        reader = self.client.do_get(self.vendor_flight.endpoints[0].ticket)
        data_table = reader.read_all()
        return data_table.to_pandas()

class CPITransformer(Transformer):
    def transform(self, df:pd.DataFrame) -> pd.DataFrame:
        transformed_data = df
        return transformed_data

@click.command()
@click.option("--library", prompt="Enter library name", default="test")
@click.option("--csv-path", prompt="Enter path for csv files", default="../data/")
def arrow_flight_loader():
    extractor = ArrowFlightExtractor(VENDOR_FILEPATH)
    data = extractor.extract()
    transformed_data = CPITransformer().transform(data)
    storer = ArcticStorer(destination="ons", to_store=transformed_data)
    storer.store()


