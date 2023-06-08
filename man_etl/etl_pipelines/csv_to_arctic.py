import os
import pandas as pd
import logging
from typing import List, Dict
from arcticdb import Arctic
from arcticdb.version_store.library import Library
from logging import getLogger
import click
from man_etl.etl_pipelines.core.base import Extractor, Storer
from dataclasses import dataclass


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')



CSV_PATH = "../data/"
DB_PATH = "lmdb:////mnt/c/Users/sharl/repos/DTS/BigData/WM9L8-IMA/astore"
S3_PATH = "s3://s3.eu-west-2.amazonaws.com:manstocks?region=eu-west-2&access=AKIAVHAD6ZB4RYHDPBWA&secret=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"


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

@click.command()
@click.option("--library", prompt="Enter library name", default="test")
@click.option("--csv-path", prompt="Enter path for csv files", default="../data/")
def csv_to_arctic(library, csv_path):
    data = CSVExtractor(csv_path).extract()
    arctic_lib = ArcticInitializer(library)()
    ArcticStorer(to_store=data, destination=arctic_lib).store_many()


if __name__ == "__main__":
    csv_to_arctic()
