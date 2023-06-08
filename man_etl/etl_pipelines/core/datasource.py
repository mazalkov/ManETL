import os
import pandas as pd
import logging
from man_etl.etl_pipelines.core.base import Extractor
from dataclasses import dataclass

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
