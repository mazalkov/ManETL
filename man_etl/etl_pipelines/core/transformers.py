import os
import pandas as pd
import logging
from man_etl.etl_pipelines.core.base import Transformer
from man_etl.etl_pipelines.util.definitions import TRANSFORM
from man_etl.etl_pipelines.util.transforms import calcs
from typing import Dict, List
from dataclasses import dataclass


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Logger")


@dataclass
class DataFrameTransformer(Transformer):
    data: Dict
    transformers: Dict

    def transform(self) -> Dict:
        transformed_data = {}
        for symbol in self.data:
            data_obj = self.data[symbol]
            for tname, transform in self.transformers.items():
                data_obj = self._apply_transform(data_obj, tname, transform)
            transformed_data[f"{symbol}_{TRANSFORM}"] = data_obj
        return transformed_data

    @staticmethod
    def _apply_transform(data_unit: pd.DataFrame, colname: str, fn) -> pd.DataFrame:
        data_unit[colname] = data_unit.apply(fn, axis=1)
        return data_unit


class CPITransformer(Transformer):
    def transform(self, df: pd.DataFrame) -> pd.DataFrame:
        transformed_data = df
        return transformed_data
