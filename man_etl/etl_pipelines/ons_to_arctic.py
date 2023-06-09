import click

from man_etl.etl_pipelines.core.arctic import ArcticInitializer
from man_etl.etl_pipelines.core.storers import ArcticStorer
from man_etl.etl_pipelines.core.extractors import ArrowFlightExtractor
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Logger")

VENDOR_FILEPATH = "vendor_ons/cpi.parquet"


@click.command()
@click.option("--library", prompt="Enter library name", default="ons")
def ons_to_arctic(library):
    extractor_data = ArrowFlightExtractor.parquet(filepath=VENDOR_FILEPATH).extract()
    payload = {"cpi": extractor_data}
    arctic_lib = ArcticInitializer(libname=library)()
    logger.info("Storing data in S3")
    ArcticStorer(to_store=payload, destination=arctic_lib).store()
