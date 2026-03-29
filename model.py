from typing import NamedTuple, Final

TRADE_DATA_EXAMPLE: Final[str] = "1773850108424554527|binance-spot|btc-usdt|65000.6372|0.065|1|"


class Trade(NamedTuple):
    receive_time_ns: int
    exchange: str
    symbol: str
    price: float
    qty: float
    is_taker_sell: bool
    id: str

    @classmethod
    def from_string(cls, data: str) -> "Trade":
        parts = data.split("|")
        return cls(
            receive_time_ns=int(parts[0]),
            exchange=parts[1],
            symbol=parts[2],
            price=float(parts[3]),
            qty=float(parts[4]),
            is_taker_sell=parts[5] == "1",
            id=parts[6],
        )

    def to_string(self) -> str:
        return f"{self.receive_time_ns}|{self.exchange}|{self.symbol}|{self.price}|{self.qty}|{self.is_taker_sell}|{self.id}"


if __name__ == "__main__":
    t = Trade.from_string(TRADE_DATA_EXAMPLE)
    print(t)
    print(t.to_string())
