from pydantic import BaseModel

from models.input_source import InputSource


class Asset(BaseModel):
    ticker: str  # Internal name
    identifier: str  # Source identifier for the ticker
    source: InputSource
    strategy_alias: str = ""  # Alias for strategy # Ej:subyacente

    def __str__(self):
        return "#".join([self.source.__class__.__name__, self.ticker])

    def __hash__(self):
        return self.__str__().__hash__()
