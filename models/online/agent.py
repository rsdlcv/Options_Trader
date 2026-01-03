import multiprocessing
from typing import List, Any, Dict

import pandas as pd

from models.online.data_store import DataStore
from models.online.input_store import InputStore
from models.strategies.strategy import Strategy


class Agent:

    def __init__(
            self,
            strategies: List[Strategy],
            input_store: InputStore,
            data_store: DataStore
    ) -> None:
        self.strategies = strategies
        self.input_store = input_store
        self.data_store = data_store

        # Hash between InputStore queue and strategies
        self.input_strategies_map = self.get_input_strategies_map(strategies)

    def start(self):
        # The listener process will be listening to the input queue and will trigger the strategies
        listen_to_input_queue_process = multiprocessing.Process(
            target=Agent.listen_to_input_queue,
            args=(self,)
        )
        listen_to_input_queue_process.start()

    def listen_to_input_queue(self) -> Any:
        while True:
            # Ticker that triggers strategy evaluation
            activaction_ticker = self.input_store.input_notification_queue.get()

            for strategy in self.input_strategies_map[activaction_ticker]:
                self.process_strategy_by_ticker(strategy, activaction_ticker)

    def process_strategy_by_ticker(self, strategy: Strategy, activation_ticker: str) -> None:
        asset_id = f'ASSET#{activation_ticker}'
        real_time_assets = self.input_store.store[asset_id].iloc[-1]

        real_time_assets = pd.DataFrame([])
        timeframed_indicators = pd.DataFrame([])
        timeframed_assets = pd.DataFrame([])
        timeframed_assets_definition_map = {} # timeframe -> List[ticker]
        for i in strategy.config.indicators:
            combinations = [
                (i_config, asset)
                for i_config in i.config
                for asset in i.assets
            ]
            for i_config, asset in combinations:
                asset_id = f'ASSET#{str(asset)}'
                timeframed_asset_id = f'{i_config.timeframe}_{asset_id}'
                indicator_id = f'{i_config.timeframe}_INDICATOR#{activation_ticker}#{i.__class__.__name__}#{str(i_config)}'

                if (indicator_id not in self.data_store.data_store.keys()
                        or timeframed_asset_id not in self.data_store.data_store.keys()
                        or asset_id not in self.input_store.store.keys()
                ):
                    print(f'Skipping {strategy.__class__.__name__} evaluation for ticker {activation_ticker}')
                    return None

                real_time_asset_prefix = asset.strategy_alias if asset.strategy_alias != "" else asset.ticker
                timeframed_prefix = f'{i_config.timeframe}#{real_time_asset_prefix}'

                if i_config.timeframe not in timeframed_assets_definition_map.keys():
                    timeframed_assets_definition_map[i_config.timeframe] = []
                if real_time_asset_prefix not in timeframed_assets_definition_map[i_config.timeframe]:
                    timeframed_assets_definition_map[i_config.timeframe].append(real_time_asset_prefix)

                if len(real_time_assets) == 0 or not real_time_assets.columns.str.startswith(real_time_asset_prefix).any():

                    real_time_assets = pd.concat([
                        real_time_assets,
                        self.input_store.store[asset_id].iloc[-5:].add_prefix(f'{real_time_asset_prefix}#')
                    ], axis=1)

                if len(timeframed_assets) == 0 or not timeframed_assets.columns.str.startswith(
                        timeframed_prefix).any():
                    timeframed_assets = pd.concat([
                        timeframed_assets,
                        self.data_store.data_store[timeframed_asset_id].add_prefix(f'{timeframed_prefix}#')
                    ], axis=1)

                timeframed_indicators = pd.concat([
                    timeframed_indicators,
                    self.data_store.data_store[indicator_id].add_prefix(f'{timeframed_prefix}#')
                ], axis=1)

        # If differences -> not up to date
        # if not (real_time_assets.index == self.input_store.store[asset_id].iloc[-1].index).all():
        #     print('Returning')
        #     print(real_time_assets.index)
        #     print(self.input_store.store[asset_id].iloc[-1].index)
        #     print('\n\n\n')
        #     return None

        strategy.evaluate(real_time_assets, timeframed_indicators, timeframed_assets, timeframed_assets_definition_map)

    def get_input_strategies_map(self, strategies: List[Strategy]) -> Dict[str, Any]:
        input_strategies_map = {}

        combinations = [
            (strategy, indicator, asset)
            for strategy in strategies
            for indicator in strategy.config.indicators
            for asset in indicator.assets
        ]

        for strategy, indicator, asset in combinations:

            asset_str = str(asset)
            if asset_str not in input_strategies_map.keys():
                input_strategies_map[asset_str] = []
            input_strategies_map[asset_str].append(strategy)

        return input_strategies_map
