import os
import pandas as pd
import logging
from typing import Dict
from arcticdb import Arctic
from arcticdb.version_store.library import Library
from logging import getLogger
from typing import List
from man_etl.etl_pipelines.core.base import Storer, Extractor
from dataclasses import dataclass

S3_PATH = "s3://s3.eu-west-2.amazonaws.com:manstocks?region=eu-west-2&access=AKIAVHAD6ZB4RYHDPBWA&secret=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"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')

class ArcticInitializer:
    def __init__(self, libname: str):
        self.libname: str = libname

    def __call__(self) -> Library:
        self.create_library()
        return self.get_library()

    def get_db(self) -> Arctic:
        return Arctic(S3_PATH)

    def create_library(self):
        ac = self.get_db()
        if self.libname not in ac.list_libraries():
            logger.info(f"Creating library {self.libname}")
            ac.create_library(self.libname)
        else:
            logger.info(f"The library {self.libname} already exists")

    def get_library(self) -> Library:
        if self.libname in self.get_db().list_libraries():
            return self.get_db()[self.libname]
        else:
            self.create_library()
            return self.get_db()[self.libname]
        
@dataclass
class ArcticStorer(Storer):
    to_store: Dict 
    destination: Library

    def store(self, sym: str, data:pd.DataFrame):
        logger.info(f"Writing data for symboe {sym}")
        self.destination.write(sym, data)

    def store_many(self):
        for sym in self.to_store:
            data = self.to_store[sym]
            self.store(sym, data)

        
@dataclass
class ArcticExtractor(Extractor):
    symbols: List
    library: Library

    def extract(self, symbol: str) -> pd.DataFrame:
        return self.library.read(symbol).data

    def extract_many(self) -> Dict:
        data = {}
        for sym in self.symbols:
            data[sym] = self.extract(sym)
        return data
