import multiprocessing
from multiprocessing import Queue
from typing import Any, List, Dict

import pandas as pd

from models.asset import Asset


class InputStore:

    def __init__(
            self,
            assets: List[Asset],
            input_notification_queue: Queue,
            store: Dict[str, pd.DataFrame]
            # STORE IDS: ASSET#<source.__class__.__name__>#<asset_name> -> Ej: ASSET#BalanzWebsocketInputSource#GGAL
    ):
        # This queue will notify when a source_asset is updated
        self.input_notification_queue = input_notification_queue
        self.store = store

        # Map that defines which sources and assets must be used
        self.assets_sources_map = self.define_assets_and_sources_requirements(assets)
        # This queue will gather the messages from the websocket and save the data to the internal store
        self.internal_queue = multiprocessing.Queue()

        if len(self.assets_sources_map.keys()) > 1:
            raise Exception("More than one input sources defined")

        # Get data from sources using first core
        for source_name in self.assets_sources_map.keys():
            source = self.assets_sources_map[source_name]['source']
            assets = self.assets_sources_map[source_name]['assets']

            assets = [asset.identifier for asset in assets]

            # Each websocket will send a message to the queue
            websocket_process = multiprocessing.Process(
                target=source.start,
                args=(self.internal_queue, assets)
            )
            websocket_process.start()

        # The listener process will save the df and notify to external queue
        listener_process = multiprocessing.Process(
            target=InputStore.listen_to_websockets,
            args=(self,)
        )
        listener_process.start()

    def listen_to_websockets(self) -> Any:
        while True:
            source_ticker, df = self.internal_queue.get()
            if source_ticker is None or df is None:
                print('Input Store listener turned off')
                break

            self.add_to_store(source_ticker, df)

            # Notifying other resources with a message like <source_name>#<asset_name>
            self.input_notification_queue.put(source_ticker)

    def add_to_store(self, store_id: str, df: pd.DataFrame):
        store_id = "ASSET#" + store_id
        if store_id not in self.store.keys():
            self.store[store_id] = df
        else:
            self.store[store_id] = pd.concat([
                self.store[store_id],
                df
            ])

    def define_assets_and_sources_requirements(self, assets: List[Asset]):
        """
        For every source and asset create a dict with the following format:

        {
            <source_1>: {
                'assets': [<list_of_assets_to_be_extracted_from_source>]
            }
            <source_2>:
        }

        """

        assets_sources_map = {}

        for asset in assets:
            source_name = asset.source.__class__.__name__

            if source_name not in assets_sources_map.keys():
                assets_sources_map[source_name] = {
                    'assets': set(),
                    'source': asset.source
                }

            assets_sources_map[source_name]['assets'].add(asset)

        return assets_sources_map
