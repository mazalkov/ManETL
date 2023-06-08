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
    destination: Library

    def store(self):
        for sym in self.to_store:
            data = self.to_store[sym]
            self.store(sym, data)