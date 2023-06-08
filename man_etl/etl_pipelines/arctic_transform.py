import logging
from typing import List, Dict
from logging import getLogger
import click
from man_etl.etl_pipelines.core.arctic import ArcticInitializer
from man_etl.etl_pipelines.core.extractors import ArcticExtractor
from man_etl.etl_pipelines.core.transformers import DataFrameTransformer
from man_etl.etl_pipelines.core.storers import ArcticStorer
from man_etl.etl_pipelines.util.transforms import calcs

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger('Logger')

@click.command()
@click.option("--library", prompt="Enter library name", default="test")
def arctic_transform(library):
    logger.info(f'Initializing library {library}')
    arctic_lib = ArcticInitializer(libname=library)()
    logger.info(f'Extracting data from library: {library}')
    extracted_data = ArcticExtractor(library=arctic_lib).extract_many()
    logger.info(f'Transforming data.......')
    transformed_data = DataFrameTransformer(data=extracted_data, transformers=calcs).transform()
    logger.info(f'Storing transformed data in S3')
    ArcticStorer(to_store=transformed_data, destination=arctic_lib).store_many()


if __name__ == "__main__":
    arctic_transform()