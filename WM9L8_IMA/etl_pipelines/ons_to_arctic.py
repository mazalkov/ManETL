import pandas as pd

from WM9L8_IMA.etl_pipelines.core.base import Extractor
from WM9L8_IMA.etl_pipelines.util.utils import SERVER_MAPPINGS
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

def arrow_flight_loader():
    pass



