import logging
from typing import List, Dict
from logging import getLogger
import click
from man_etl.etl_pipelines.core.arctic import ArcticInitializer
from man_etl.etl_pipelines.core.storers import ArcticStorer
from man_etl.etl_pipelines.core.extractors import YFExtractor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Logger")

S3_PATH = "s3://s3.eu-west-2.amazonaws.com:manstocks?region=eu-west-2&access=AKIAVHAD6ZB4RYHDPBWA&secret=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"


@click.command()
@click.option("--library", prompt="Enter library name", default="test")
def yf_to_arctic(library):
    data = YFExtractor().extract()
    arctic_lib = ArcticInitializer(library)()
    logger.info("Storing data in S3")
    ArcticStorer(to_store=data, destination=arctic_lib).store()


if __name__ == "__main__":
    yf_to_arctic()
