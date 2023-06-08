import click

from man_etl.etl_pipelines.core.storers import ArcticStorer
from man_etl.etl_pipelines.core.extractors import ArrowFlightExtractor
from man_etl.etl_pipelines.core.transformers import CPITransformer

VENDOR_FILEPATH = "ons/cpi.parquet"

@click.command()
@click.option("--library", prompt="Enter library name", default="ons")
def arrow_flight_loader(library):
    extractor = ArrowFlightExtractor.parquet(filepath=VENDOR_FILEPATH)
    data = extractor.extract()
    transformed_data = CPITransformer().transform(data)
    data_unit = {"ons-cpi": transformed_data}
    storer = ArcticStorer(destination=library, to_store=data_unit).store()
