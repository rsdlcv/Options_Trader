from abc import ABC, abstractmethod
from typing import List, Any, Dict

import pandas as pd
from pydantic import BaseModel, model_validator

from models.asset import Asset
from models.indicators.indicator import Indicator
from models.portfolio import Portfolio


class StrategyConfiguration(BaseModel):
    indicators: List[Any]
    portfolio: Portfolio

    def __str__(self):
        properties = []
        properties += [str(indicator) for indicator in self.indicators]
        properties += [str(self.portfolio)]

        return '#'.join(properties)

    def __hash__(self):
        return self.__str__().__hash__()


class Strategy(BaseModel, ABC):
    config: StrategyConfiguration

    def __hash__(self):
        return self.__str__().__hash__()

    @model_validator(mode='after')
    def validate(self):
        indicators_id = [i.__class__.__name__ for i in self.config.indicators]

        if len(indicators_id) != len(set(indicators_id)):
            raise Exception(f"Repeated indicators ids in strategy {self.__class__.__name__}")

        existing_alias = {}

        for indicator in self.config.indicators:
            for asset in indicator.assets:
                if asset.strategy_alias == "":
                    continue
                if asset.strategy_alias in existing_alias.keys() and existing_alias[asset.alias] != asset.ticker:
                    raise Exception(f"Alias {asset.strategy_alias} duplicated in {self.__class__.__name__}")
                existing_alias[asset.strategy_alias] = asset.ticker


    @staticmethod
    def get_assets_from_strategies(strategies: List[Any]) -> List[Asset]:
        assets = []

        for strategy in strategies:
            for indicator in strategy.config.indicators:
                assets += indicator.assets

        return assets

    @staticmethod
    def get_indicators_from_strategies(strategies: List[Any]) -> List[Indicator]:
        indicators = []

        for strategy in strategies:
            indicators += strategy.config.indicators

        return indicators

    @abstractmethod
    def evaluate(
            self,
            real_time_prices: pd.DataFrame,
            timeframed_indicators: pd.DataFrame,
            timeframed_assets: pd.DataFrame,
            timeframed_assets_definition_map: Dict[str, str]  # Dict timeframe -> list of tickers
    ):
        raise Exception(f"evaluate method must be implemented in class {self.__class__.__name__}")
