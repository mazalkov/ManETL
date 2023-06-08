import click

#from man_etl.etl_pipelines.core.arctic import ArcticInitializer
#from man_etl.etl_pipelines.core.pipeline import Pipeline
from man_etl.etl_pipelines.core.storers import ArrowFlightStorer
from man_etl.etl_pipelines.core.extractors import ArrowFlightExtractor
#from man_etl.etl_pipelines.core.transformers import CPITransformer

VENDOR_FILEPATH = "ons/cpi.parquet"

@click.command()
@click.option("--library", prompt="Enter library name", default="ons")
def ons_to_arctic(library):
    extractor_data = ArrowFlightExtractor.parquet(filepath=VENDOR_FILEPATH).extract()
    payload = {"cpi": extractor_data}
    storer = ArrowFlightStorer.parquet(to_store=payload, library_name=library)
    storer.store()
    # library = ArcticInitializer(libname=library)()
    # storer = ArcticStorer(destination=library, to_store=extractor_data)
    # storer.store()

