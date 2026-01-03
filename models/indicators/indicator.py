from abc import ABC, abstractmethod
from typing import List

import pandas as pd
from pydantic import BaseModel, model_validator

from models.asset import Asset


class IndicatorConfiguration(BaseModel, ABC):
    timeframe: str  # In seconds
    min_length: int

    def __str__(self):
        properties = [f'{k}={v}' for k, v in self.model_dump().items()]
        properties = '#'.join(properties)

        return properties


class Indicator(BaseModel, ABC):
    config: List[IndicatorConfiguration]
    assets: List[Asset]

    def __hash__(self):
        return self.__str__().__hash__()

    @model_validator(mode='after')
    def validate(self):
        configs = [str(config) for config in self.config]
        assets_ids = [str(asset) for asset in self.assets]
        timeframes = [config.timeframe for config in self.config]

        # hay que asumir que los timeframes sean divisibles entre si por el time.sleep(min())

        for timeframe in timeframes:
            if int(timeframe) < 5:
                raise Exception(f"Timeframe must be greater or equal than 5 in indicator {self.__class__.__name__}")

        if len(assets_ids) != len(set(assets_ids)):
            raise Exception(f"Repeated assets in indicator {self.__class__.__name__}")
        if len(configs) != len(set(configs)):
            raise Exception(f"Repeated configs in indicator {self.__class__.__name__}")

    @abstractmethod
    def compute(self, df: pd.DataFrame, config: IndicatorConfiguration) -> pd.DataFrame:
        raise Exception(f"Method compute() must be implemented for {self.__class__.__name__}")
