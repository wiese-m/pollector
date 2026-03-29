import clickhouse_connect
from clickhouse_connect.datatypes.base import ClickHouseType, InsertContext
from clickhouse_connect.driver import AsyncClient
from model import Trade


class ClickhouseWriter:
    def __init__(self, batch_size: int = 10_000) -> None:
        self._client: AsyncClient | None = None
        # self._trades_ctx: InsertContext | None = None
        self._batch_size = batch_size
        self._trades: list[Trade] = []

    async def connect(self) -> None:
        self._client = await clickhouse_connect.get_async_client(host="clickhouse")
        # self._trades_ctx = InsertContext(
        #     table="market_data.trades",
        # )

    async def write_trade(self, trade: Trade) -> None:
        assert isinstance(self._client, AsyncClient)
        # assert isinstance(self._trades_ctx, InsertContext)
        self._trades.append(trade)
        if len(self._trades) >= self._batch_size:
            # self._trades_ctx.data = self._trades
            await self._client.insert(
                table="market_data.trades",
                data=self._trades,
                column_names=[
                    "timestamp",
                    "exchange",
                    "symbol",
                    "price",
                    "qty",
                    "is_taker_sell",
                    "id",
                ],
            )
            # await self._client.data_insert(self._trades_ctx)
            self._trades.clear()
