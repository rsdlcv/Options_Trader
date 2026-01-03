from abc import ABC, abstractmethod

from pydantic import BaseModel


class OutputSource(BaseModel, ABC):
    pass


class RESTOutputSource(OutputSource):
    @abstractmethod
    def buy(self, asset_identifier: str, price: float, quantity: int):
        raise Exception(f"Method buy() must be implemented for {self.__class__.__name__}")

    @abstractmethod
    def sell(self, asset_identifier: str, price: float, quantity: int):
        raise Exception(f"Method sell() must be implemented for {self.__class__.__name__}")


class BalanzRESTOutputSource(RESTOutputSource):
    def sell(self, asset_identifier: str, price: float, quantity: int):
        print('SELL')

    def buy(self, asset_identifier: str, price: float, quantity: int):
        print('BUY')