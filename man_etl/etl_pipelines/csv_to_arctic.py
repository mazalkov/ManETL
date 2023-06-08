import logging
from typing import List, Dict
from logging import getLogger
import click
from man_etl.etl_pipelines.core.arctic import ArcticInitializer, ArcticStorer
from man_etl.etl_pipelines.core.extractors import CSVExtractor, YFExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')


CSV_PATH = "../data/"
DB_PATH = "lmdb:////mnt/c/Users/sharl/repos/DTS/BigData/WM9L8-IMA/astore"
S3_PATH = "s3://s3.eu-west-2.amazonaws.com:manstocks?region=eu-west-2&access=AKIAVHAD6ZB4RYHDPBWA&secret=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"




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
