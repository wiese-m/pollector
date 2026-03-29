import asyncio
import logging
from typing import Any
from model import Trade

log = logging.getLogger("pollector")


class ServerProtocol(asyncio.DatagramProtocol):
    def __init__(self, queue: asyncio.Queue[str]) -> None:
        self.queue = queue
        self.drop_count = 0
        self.msg_count = 0

    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        try:
            self.queue.put_nowait(data.decode())
        except asyncio.QueueFull:
            log.warning(f"Dropped message from: {addr}")
            self.drop_count += 1

    async def worker(self) -> None:
        while True:
            data = await self.queue.get()
            trade = Trade.from_string(data)
            self.msg_count += 1
            # log.info(trade)
            log.info(f"Total messages: {self.msg_count}")


async def main() -> None:
    logging.basicConfig(level=logging.DEBUG)
    log.info("Starting UDP server")

    queue: asyncio.Queue[str] = asyncio.Queue(maxsize=10)

    loop = asyncio.get_running_loop()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(queue), local_addr=("0.0.0.0", 9999)
    )

    try:
        await protocol.worker()
    finally:
        transport.close()


asyncio.run(main())
