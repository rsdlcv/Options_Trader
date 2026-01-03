from typing import List

import pandas as pd

from models.asset import Asset
from models.indicators.indicator import Indicator, IndicatorConfiguration


class VeryPowerfulIndicatorConfiguration(IndicatorConfiguration):
    sma_length: int


class VeryPowerfulIndicator(Indicator):

    def __init__(self, assets: List[Asset], config: List[VeryPowerfulIndicatorConfiguration]):
        super().__init__(assets=assets, config=config)

    def compute(self, df: pd.DataFrame, config: VeryPowerfulIndicatorConfiguration) -> pd.DataFrame:
        return (
            df[['Close']]
            .rolling(config.sma_length).mean()
            .iloc[-1:]
        ).rename(columns={'Close': f'SMA_{config.sma_length}'})
        # Habria que testear que este identificador no se repita
