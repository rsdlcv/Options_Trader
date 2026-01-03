from copy import copy
from typing import List

from pydantic import BaseModel

from models.asset import Asset
from models.output_source import OutputSource


class Operation(BaseModel):
    identifier: str
    quantity: int
    price: float
    operation_type: str  # LONG-SHORT-ETC


class Portfolio(BaseModel):
    liquid: float
    output: OutputSource
    open_operations: List[Operation] = []

    def add_operation(self, asset: Asset, price: float, quantity: int, operation_type: str) -> None:
        self.open_operations.append(Operation(
            identifier=asset.identifier,
            quantity=quantity,
            price=price,
            operation_type=operation_type
        ))

    def remove_operation(self, operation: Operation) -> None:
        self.open_operations.remove(operation)

    def edit_operation(self, old_operation: Operation, new_operation: Operation) -> None:
        self.open_operations[self.open_operations.index(old_operation)] = new_operation

    def remove_asset_quantity(self, asset: Asset, quantity: int) -> None:
        current_q = quantity
        ops = [
            operation
            for operation in self.open_operations
            if asset.identifier == operation.identifier
        ]
        ops.sort(key=lambda o: o.price)  # First = cheap
        for operation in ops:
            if operation.quantity == current_q:
                self.remove_operation(operation)
                break
            elif operation.quantity > current_q:
                new_operation = copy(operation)
                new_operation.quantity -= current_q
                self.edit_operation(operation, new_operation)
                break
            else:
                self.remove_operation(operation)
                current_q -= operation.quantity

    def get_operations_by_type_and_asset(self, asset: Asset, filter_type: str) -> List[Operation]:
        return [
            operation
            for operation in self.open_operations
            if operation.operation_type == filter_type and asset.identifier == operation.identifier
        ]

    def get_quantity_by_asset(self, asset: Asset) -> int:
        return sum([
            operation.quantity
            for operation in self.open_operations
            if operation.identifier == asset.identifier
        ])

    def buy(self, asset: Asset, price: float, quantity: int, operation_type: str):
        total_amount = price * quantity
        if self.liquid >= total_amount:
            self.liquid -= total_amount
            self.output.buy(asset.identifier, price, quantity)
            self.add_operation(asset, price, quantity, operation_type)
        else:
            print(
                f'Error when buying {operation_type} {quantity} {asset.identifier} assets at {price} because liquidity = {self.liquid}')

    def sell(self, asset: Asset, price: float, quantity: int):
        asset_q = self.get_quantity_by_asset(asset)
        if asset_q >= quantity:
            self.output.sell(asset.identifier, price, quantity)
            self.liquid += price * quantity
            self.remove_asset_quantity(asset, quantity)
