import multiprocessing
import os

from models.online.agent import Agent
from models.asset import Asset
from models.indicators.sample_indicator import VeryPowerfulIndicator, VeryPowerfulIndicatorConfiguration
from models.input_source import BalanzWebsocketInputSource
from models.online.data_store import DataStore
from models.online.input_store import InputStore
from models.output_source import BalanzRESTOutputSource
from models.portfolio import Portfolio
from models.strategies.sample_strategy import BestStrategyEver
from models.strategies.strategy import StrategyConfiguration, Strategy


def main():
    manager = multiprocessing.Manager()
    strategies = [
        BestStrategyEver(
            config=StrategyConfiguration(
                indicators=[
                    VeryPowerfulIndicator(
                        assets=[
                            Asset(ticker='GGAL', identifier='GGAL-0002-C-CT-ARS', alias="sub",
                                  source=BalanzWebsocketInputSource()),
                            Asset(ticker='GFGC41617G', identifier='GFGC41617G', source=BalanzWebsocketInputSource())
                        ],
                        config=[VeryPowerfulIndicatorConfiguration(
                            timeframe=str(5),
                            min_length=10,
                            sma_length=10
                        )]
                    )
                ],
                portfolio=Portfolio(
                    liquid=1000000,
                    output=BalanzRESTOutputSource()
                )
            )
        )
    ]
    # Queue to notify when an asset is updated
    input_notification_queue = multiprocessing.Queue()
    # Obtains assets and notifies to queue when asset is updated
    input_store = InputStore(
        assets=Strategy.get_assets_from_strategies(strategies),
        input_notification_queue=input_notification_queue,
        store=manager.dict()
    )

    # Calculates indicators and assets grouped by timeframe
    data_store = DataStore(
        indicators=Strategy.get_indicators_from_strategies(strategies),
        input_store=input_store.store,
        data_store=manager.dict()
    )

    agent = Agent(strategies=strategies, input_store=input_store, data_store=data_store)
    agent.start()


if __name__ == '__main__':
    os.environ['TOKEN'] = "B9E7FD4C-BD14-4EE0-810E-7B07D12A1BB1"
    multiprocessing.set_start_method("fork")
    main()
