import asyncio
import logging
from typing import Any

from ch_writer import ClickhouseWriter
from model import Trade

log = logging.getLogger("pollector")


class ServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue[bytes], writer: ClickhouseWriter) -> None:
        self.queue = queue
        self.drop_count = 0
        self.msg_count = 0
        self.writer = writer

    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        try:
            self.queue.put_nowait(data)
            self.msg_count += 1
        except asyncio.QueueFull:
            log.warning(f"Dropped message from: {addr}")
            self.drop_count += 1

    async def write(self) -> None:
        while True:
            data = await self.queue.get()
            trade = Trade.from_string(data.decode())
            await self.writer.write_trade(trade)

    async def monitor(self, freq_sec: float = 10.0) -> None:
        while True:
            await asyncio.sleep(freq_sec)
            log.info(
                f"Queue size: {self.queue.qsize()} | Total messages: {self.msg_count} | Dropped messages: {self.drop_count}"
            )


async def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    log.info("Initializing ClickHouse writer...")
    writer = ClickhouseWriter()
    await writer.connect()
    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=100_000)
    loop = asyncio.get_running_loop()
    log.info("Starting UDP server...")
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(queue, writer), local_addr=("0.0.0.0", 9999)
    )
    try:
        await asyncio.gather(protocol.write(), protocol.monitor())
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
