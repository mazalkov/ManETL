import click
from man_etl.etl_pipelines.core.storers import ArrowFlightStorer
from man_etl.etl_pipelines.core.extractors import ArcticExtractor
from man_etl.etl_pipelines.core.arctic import ArcticInitializer
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')

@click.command()
@click.option("--library", prompt="Enter library name", default="etl_demo")
def arctic_to_arrowflight(library):
    logger.info("Initializing Arctic library")
    lib = ArcticInitializer(libname=library)()
    logger.info("Extracting data from S3")
    extractor_data = ArcticExtractor(library=lib).extract_all()
    logger.info("Storing data to ArrowFlight")
    storer = ArrowFlightStorer.parquet(to_store=extractor_data, library_name=library)
    storer.store()