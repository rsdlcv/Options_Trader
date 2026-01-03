import json
import os
from abc import ABC, abstractmethod
from datetime import datetime
from multiprocessing import Queue
from typing import List, Any, Dict, Tuple

import pandas as pd
from pydantic import BaseModel
from websockets.sync.client import connect


class InputSource(BaseModel, ABC):
    pass


class WebsocketInputSource(InputSource, ABC):

    @abstractmethod
    def start(self, queue: Queue, tickers: List[str]) -> None:
        raise Exception(f"Method start() must be implemented for {self.__class__.__name__}")


class BalanzWebsocketInputSource(WebsocketInputSource):
    url: str = "wss://clientes.balanz.com/websocket"

    @staticmethod
    def preprocess_message(message: str) -> Tuple[str, pd.DataFrame]:

        message = json.loads(message)

        df = pd.DataFrame([{
            'time': datetime.now(),

            'last_price': message.get('u', None),
            'volume': message.get('v', None),

            'box_buy_price_1': message.get('pc', None),
            'box_buy_quantity_1': message.get('cc', None),
            'box_buy_price_2': message.get('pc1', None),
            'box_buy_quantity_2': message.get('cc1', None),
            'box_buy_price_3': message.get('pc2', None),
            'box_buy_quantity_3': message.get('cc2', None),
            'box_buy_price_4': message.get('pc3', None),
            'box_buy_quantity_4': message.get('cc3', None),
            'box_buy_price_5': message.get('pc4', None),
            'box_buy_quantity_5': message.get('cc4', None),
            'box_buy_price_6': message.get('pc5', None),
            'box_buy_quantity_6': message.get('cc5', None),
            'box_buy_price_7': message.get('pc6', None),
            'box_buy_quantity_7': message.get('cc6', None),

            'box_sell_price_1': message.get('pv', None),
            'box_sell_quantity_1': message.get('cv', None),
            'box_sell_price_2': message.get('pv1', None),
            'box_sell_quantity_2': message.get('cv1', None),
            'box_sell_price_3': message.get('pv2', None),
            'box_sell_quantity_3': message.get('cv2', None),
            'box_sell_price_4': message.get('pv3', None),
            'box_sell_quantity_4': message.get('cv3', None),
            'box_sell_price_5': message.get('pv4', None),
            'box_sell_quantity_5': message.get('cv4', None),
            'box_sell_price_6': message.get('pv5', None),
            'box_sell_quantity_6': message.get('cv5', None),
            'box_sell_price_7': message.get('pv6', None),
            'box_sell_quantity_7': message.get('cv6', None)
        }])

        df.set_index('time', inplace=True)

        return f"{BalanzWebsocketInputSource.__name__}#{message['ticker']}", df

    def start(self, queue: Queue, identifiers: List[str]) -> None:
        with connect(self.url) as websocket:
            print(f'Starting connection {self.url}')

            for identifier in identifiers:
                print(f'Sending ticker request {self.__class__.__name__} - {identifier}')
                websocket.send(
                    '{{"securities": "{ticker}","token": "{token}"}}'.format(ticker=identifier, token=os.environ["TOKEN"])
                )

            while True:
                message = websocket.recv()
                queue.put(BalanzWebsocketInputSource.preprocess_message(message))


"""

import websockets
# drive the client connection
async def main():
    # open a connection to the server
    async with websockets.connect("wss://clientes.balanz.com/websocket") as websocket:
        # report progress
        print('Connected to server')
        # send a message to server
        print(await websocket.send('{"securities": "GFGC40608J", "token": "<>"}'))
        print(await websocket.send('{"securities": "GFGC44608J", "token": "<>"}'))
        
        while True:
            # read message from server
            message = await websocket.recv()
            # report result
            print(f"Received: {message}")
    # report progress
    print('Disconnected')
# start the event loop
asyncio.run(main())


"""
