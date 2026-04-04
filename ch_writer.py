import time

import clickhouse_connect
from clickhouse_connect.driver import AsyncClient

from model import Trade


class ClickhouseWriter:
    def __init__(self, batch_size: int = 10_000, batch_ttl: float = 10.0) -> None:
        self._client: AsyncClient | None = None
        self._batch_size = batch_size
        self._batch_ttl = batch_ttl
        self._trades: list[Trade] = []
        self._last_flush_time = time.monotonic()

    async def connect(self) -> None:
        self._client = await clickhouse_connect.get_async_client(host="clickhouse")

    def _should_flush(self) -> bool:
        return len(self._trades) >= self._batch_size or (
            len(self._trades) > 0
            and time.monotonic() - self._last_flush_time > self._batch_ttl
        )

    async def flush(self) -> None:
        await self._client.insert(
            table="market_data.trades",
            data=self._trades,
            column_names=[
                "ts_recv",
                "exchange",
                "symbol",
                "price",
                "qty",
                "is_taker_sell",
                "id",
                "ts_execution",
                "ts_event",
            ],
        )
        self._trades.clear()
        self._last_flush_time = time.monotonic()

    async def write_trade(self, trade: Trade) -> None:
        self._trades.append(trade)
        if self._should_flush():
            await self.flush()
