from typing import List, Dict

import pandas as pd

from models.indicators.indicator import Indicator
from models.strategies.strategy import Strategy, StrategyConfiguration


class BestStrategyEver(Strategy):
    def evaluate(
            self,
            real_time_assets: pd.DataFrame,
            timeframed_assets: pd.DataFrame,
            timeframed_indicators: pd.DataFrame,
            timeframed_assets_definition_map: Dict[str, str]

    ):
        if len(list(timeframed_assets_definition_map.values())[0]) != 2:
            raise Exception(f"No more than 2 assets allowed for {self.__class__.__name__}")

        print(timeframed_indicators.shape, timeframed_indicators.columns)
        print(timeframed_assets.shape, timeframed_assets.columns)
        print(timeframed_assets_definition_map)

        print('\n\n')
