from typing import NamedTuple


class Trade(NamedTuple):
    ts_recv: int
    exchange: str
    symbol: str
    price: float
    qty: float
    is_taker_sell: bool
    id: str
    ts_execution: int
    ts_event: int

    @classmethod
    def from_string(cls, data: str) -> "Trade":
        parts = data.split("|")
        return cls(
            ts_recv=int(parts[0]),
            exchange=parts[1],
            symbol=parts[2],
            price=float(parts[3]),
            qty=float(parts[4]),
            is_taker_sell=parts[5] == "1",
            id=parts[6],
            ts_execution=int(parts[7]),
            ts_event=int(parts[8]),
        )

    def to_string(self) -> str:
        is_taker_sell = 1 if self.is_taker_sell else 0
        return f"{self.ts_recv}|{self.exchange}|{self.symbol}|{self.price}|{self.qty}|{is_taker_sell}|{self.id}|{self.ts_execution}|{self.ts_event}"
