import abc
from dataclasses import dataclass

import pandas as pd


@dataclass
class Extractor(abc.ABC):

    @abc.abstractmethod
    def extract(self) -> pd.DataFrame:
        raise NotImplementedError("please implement an extraction")


class Transformer(abc.ABC):

    @abc.abstractmethod
    def transform(self) -> pd.DataFrame:
        raise NotImplementedError("please implement an extraction")


class Storer(abc.ABC):

    @abc.abstractmethod
    def store(self) -> pd.DataFrame:
        raise NotImplementedError("please implement a storer")
