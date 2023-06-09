import logging
from dataclasses import dataclass

from man_etl.etl_pipelines.core.base import Extractor, Transformer, Storer

logger = logging.getLogger(__name__)


@dataclass
class Pipeline:
    extractor: Extractor
    transformer: Transformer
    storer: Storer

    def run_pipeline(self):
        logger.info("runing extraction")
        extracted_data = self.extractor.extract()
        logger.info("running transform")
        self.transformer.initialize(extracted_data)
        transformed_data = self.transformer.transform()
        logger.info("running storer")
        self.storer.initialize(transformed_data)
        self.storer.store()
