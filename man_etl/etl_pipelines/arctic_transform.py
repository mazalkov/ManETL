import logging
from typing import List, Dict
from logging import getLogger
import click
from man_etl.etl_pipelines.core.arctic import ArcticInitializer, ArcticStorer
from man_etl.etl_pipelines.core.datasource import CSVExtractor, YFExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')

CSV_PATH = "../data/"
DB_PATH = "lmdb:////mnt/c/Users/sharl/repos/DTS/BigData/WM9L8-IMA/astore"

@click.command()
@click.option("--library", prompt="Enter library name", default="test")
@click.option("--csv-path", prompt="Enter path for csv files", default="../data/")
def csv_to_arctic(library, csv_path):
    data = CSVExtractor(csv_path).extract()
    arctic_lib = ArcticInitializer(library)()
    logger.info("Storing data in S3")
    ArcticStorer(to_store=data, destination=arctic_lib).store_many()

if __name__ == "__main__":
    csv_to_arctic()