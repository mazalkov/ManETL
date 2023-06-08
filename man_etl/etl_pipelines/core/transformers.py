import os
import pandas as pd
import logging
from man_etl.etl_pipelines.core.base import Transformer
from typing import Dict
from dataclasses import dataclass


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')

@dataclass
class DataFrameTransformer(Transformer):
    data: Dict

    def transform(self) -> pd.DataFrame:
        pass

    def transfrom_many(self) -> Dict:
        pass

class CPITransformer(Transformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_data = df
        return transformed_data
