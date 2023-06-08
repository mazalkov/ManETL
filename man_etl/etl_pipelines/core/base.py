import abc
from dataclasses import dataclass
from typing import List, Union, Any, Dict

import pandas as pd


@dataclass
class Extractor(abc.ABC):
    @abc.abstractmethod
    def extract(self) -> pd.DataFrame:
        raise NotImplementedError("please implement an extraction")


class Transformer(abc.ABC):
    @abc.abstractmethod
    def transform(self, *args, **kwargs) -> Any:
        raise NotImplementedError("please implement an extraction")


@dataclass
class Storer(abc.ABC):
    destination: Any
    to_store: Union[pd.DataFrame, Dict[str, pd.DataFrame]]

    @abc.abstractmethod
    def store(self):
        raise NotImplementedError("please implement a storer")

    @abc.abstractmethod
    def store_many(self):
        raise NotImplementedError("please implement a storer")
