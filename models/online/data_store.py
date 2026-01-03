import multiprocessing
import os
import threading
import time
from datetime import datetime, timedelta
from typing import List, Dict, Any, Tuple

import pandas as pd
import schedule

from models.asset import Asset
from models.indicators.indicator import Indicator, IndicatorConfiguration


class DataStore:

    def __init__(
            self,
            indicators: List[Indicator],
            input_store: Dict[str, pd.DataFrame],  # Asset -> Dataframe
            data_store: Dict[str, pd.DataFrame]  # TF_ASSET OR TF_INDICATOR_CONFIG -> Dataframe
            # STORE IDS: Two types
            # ASSET ID -> <timeframe>_ASSET#<source.__class__.__name__>#<asset_name> -> Ej: 5_ASSET#BalanzWebsocketInputSource#GFGC10608J
            # INDICATOR ID -> <timeframe>_INDICATOR#<source.__class__.__name__>#<asset_name>#<config_id_to_str> -> Ej: 5_INDICATOR#BalanzWebsocketInputSource#GFGC10608J#SampleIndicator#timeframe=5#min_length=10#sma_length=10
    ):

        self.input_store = input_store
        self.data_store = data_store

        self.timeframed_assets_definitions, self.timeframed_indicators_definitions = self.define_timeframed_objects_to_compute(
            indicators)
        self.WORKERS_POOL_SIZE = os.cpu_count() - 2

        threading.Thread(target=self.start_scheduler).start()

    def start_scheduler(self):

        with multiprocessing.Pool(processes=self.WORKERS_POOL_SIZE) as pool:
            timeframe_in_seconds = [int(t) for t in self.timeframed_assets_definitions.keys()]
            for timeframe in timeframe_in_seconds:
                schedule.every(timeframe).seconds.do(
                    self.new_timeframed_execution,
                    timeframe=str(timeframe),
                    pool=pool,
                    data_store=self.data_store,
                    input_store=self.input_store
                )

            while True:
                schedule.run_pending()
                time.sleep(min(timeframe_in_seconds))

    def new_timeframed_execution(
            self, timeframe: str,
            pool: multiprocessing.Pool,
            data_store: Dict[str, Any],
            input_store: Dict[str, Any]
    ):

        assets_params = [
            (str(asset), timeframe, data_store, input_store)
            for asset in self.timeframed_assets_definitions[timeframe].keys()
        ]
        pool.starmap(self.compute_timeframed_asset, assets_params)

        indicator_params = [
            (
                indicator_config_id,
                timeframe,
                data_store,
                self.timeframed_indicators_definitions[timeframe][indicator_config_id]['asset'],
                self.timeframed_indicators_definitions[timeframe][indicator_config_id]['indicator'],
                self.timeframed_indicators_definitions[timeframe][indicator_config_id]['indicator_config']
            )
            for indicator_config_id in self.timeframed_indicators_definitions[timeframe].keys()
        ]
        pool.starmap(self.compute_timeframed_indicator, indicator_params)

    def compute_timeframed_asset(self, asset_id: str, timeframe: str, data_store: Dict[str, Any],
                                 input_store: Dict[str, Any]):
        def new_tf(df: pd.DataFrame) -> pd.DataFrame:
            df = df[df.index > (datetime.now() - timedelta(seconds=int(timeframe)))]
            if len(df) == 0:
                return pd.DataFrame([])

            prices = df['last_price']
            vol = df['volume']
            return pd.DataFrame({
                'Time': datetime.now(),
                'Open': prices.iloc[0],
                'High': prices.max(),
                'Low': prices.min(),
                'Close': prices.iloc[-1],
                'Volume': vol.sum(),
            }, index=[0])

        tf_asset_key = f'{timeframe}_{asset_id}'

        if tf_asset_key not in data_store.keys():
            data_store[tf_asset_key] = pd.DataFrame()

        if asset_id not in input_store.keys():
            return None

        new_df = new_tf(input_store[asset_id])
        if len(new_df) == 0 and len(data_store[tf_asset_key]):
            new_df = data_store[tf_asset_key].iloc[-1:]

        data_store[tf_asset_key] = pd.concat([
            data_store[tf_asset_key],
            new_df
        ], ignore_index=True)

    def compute_timeframed_indicator(self, indicator_config_id: str, timeframe: str, data_store: Dict[str, Any],
                                     asset: Asset, indicator: Indicator, indicator_config: IndicatorConfiguration):

        asset_id = f'ASSET#{str(asset)}'
        tf_asset_key = f'{timeframe}_{asset_id}'
        tf_indicator_config_k = f'{timeframe}_{indicator_config_id}'

        if tf_indicator_config_k not in data_store.keys():
            data_store[tf_indicator_config_k] = pd.DataFrame()

        if tf_asset_key in data_store.keys() and len(data_store[tf_asset_key]) >= indicator_config.min_length:
            data_store[tf_indicator_config_k] = pd.concat([
                data_store[tf_indicator_config_k],
                indicator.compute(
                    data_store[tf_asset_key].iloc[-indicator_config.min_length:],
                    indicator_config
                )
            ], ignore_index=True)

    def define_timeframed_objects_to_compute(self, indicators: List[Indicator]) -> Tuple[
        Dict[str, Any], Dict[str, Any]]:
        timeframed_assets, timeframed_indicators = {}, {}

        combinations = [
            (indicator, indicator_config, asset)
            for indicator in indicators
            for indicator_config in indicator.config
            for asset in indicator.assets
        ]

        for indicator, indicator_config, asset in combinations:
            asset_id = "#".join(['ASSET', str(asset)])
            indicator_config_id = "#".join(
                ['INDICATOR', str(asset), indicator.__class__.__name__, str(indicator_config)])

            if indicator_config.timeframe not in timeframed_indicators.keys():
                timeframed_assets[indicator_config.timeframe] = {}
                timeframed_indicators[indicator_config.timeframe] = {}

            if asset_id not in timeframed_assets[indicator_config.timeframe].keys():
                timeframed_assets[indicator_config.timeframe][asset_id] = {
                    'asset': asset
                }

            if indicator_config_id not in timeframed_indicators[indicator_config.timeframe].keys():
                timeframed_indicators[indicator_config.timeframe][indicator_config_id] = {
                    'asset': asset,
                    'indicator': indicator,
                    'indicator_config': indicator_config
                }

        return timeframed_assets, timeframed_indicators
