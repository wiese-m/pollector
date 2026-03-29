import asyncio
import logging
from typing import Any
from model import Trade
from ch_writer import ClickhouseWriter

log = logging.getLogger("pollector")


class ServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue[str], writer: ClickhouseWriter) -> None:
        self.queue = queue
        self.drop_count = 0
        self.msg_count = 0
        self.writer = writer

    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        try:
            self.queue.put_nowait(data.decode())
        except asyncio.QueueFull:
            log.warning(f"Dropped message from: {addr}")
            self.drop_count += 1

    async def write(self) -> None:
        while True:
            data = await self.queue.get()
            trade = Trade.from_string(data)
            await self.writer.write_trade(trade)
            self.msg_count += 1

    async def monitor(self) -> None:
        while True:
            await asyncio.sleep(10)
            log.info(f"Queue size: {self.queue.qsize()}")
            log.info(f"Trades written: {self.msg_count}")


async def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    await asyncio.sleep(10)
    log.info("Starting UDP server")

    writer = ClickhouseWriter()
    await writer.connect()
    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=100_000)

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(queue, writer), local_addr=("0.0.0.0", 9999)
    )

    try:
        await asyncio.gather(protocol.write(), protocol.monitor())
    finally:
        transport.close()


asyncio.run(main())
