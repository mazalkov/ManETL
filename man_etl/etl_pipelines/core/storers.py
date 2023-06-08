import pandas as pd
import logging
from typing import Dict

from arcticdb.version_store.library import Library

from man_etl.etl_pipelines.core.base import Storer
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')


@dataclass
class ArcticStorer(Storer):
    to_store: Dict 
    destination: Library

    def store(self, sym: str, data:pd.DataFrame):
        logger.info(f"Writing data for symbol {sym}")
        self.destination.write(sym, data)

    def store_many(self):
        for sym in self.to_store:
            data = self.to_store[sym]
            self.store(sym, data)