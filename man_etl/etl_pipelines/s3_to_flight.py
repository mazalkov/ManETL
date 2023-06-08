import click
from man_etl.etl_pipelines.core.storers import ArrowFlightStorer
from man_etl.etl_pipelines.core.extractors import ArcticExtractor

@click.command()
@click.option("--library", prompt="Enter library name", default="etl_demo")
def s3_to_flight(library):
    extractor_data = ArcticExtractor(library=library).extract_many()
    storer = ArrowFlightStorer.parquet(to_store=extractor_data, library_name=library)
    storer.store()