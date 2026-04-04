import asyncio
import logging
import signal
from typing import Any

from ch_writer import ClickhouseWriter
from model import Trade

log = logging.getLogger("pollector")


class ServerProtocol(asyncio.DatagramProtocol):
    def __init__(
        self,
        queue: asyncio.Queue[bytes],
        writer: ClickhouseWriter,
        shutdown_event: asyncio.Event,
    ) -> None:
        self.shutdown_event = shutdown_event
        self.queue = queue
        self.drop_count = 0
        self.msg_count = 0
        self.writer = writer
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        log.info("Connection made")
        self.transport = transport

    def connection_lost(self, exc: Exception | None) -> None:
        if exc is None:
            log.info("Connection closed OK")
        else:
            log.error("Connection closed with error: %s", exc)

    def datagram_received(self, data: bytes, addr: tuple[str | Any, int]) -> None:
        try:
            self.queue.put_nowait(data)
            self.msg_count += 1
        except asyncio.QueueFull:
            log.warning(f"Dropped message from: {addr}")
            self.drop_count += 1

    async def write(self) -> None:
        while not self.shutdown_event.is_set():
            try:
                data = await asyncio.wait_for(self.queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue
            trade = Trade.from_string(data.decode())
            await self.writer.write_trade(trade)

    async def monitor(self, freq_sec: float = 10.0) -> None:
        while not self.shutdown_event.is_set():
            try:
                await asyncio.wait_for(self.shutdown_event.wait(), timeout=freq_sec)
            except asyncio.TimeoutError:
                pass
            log.info(
                f"Queue size: {self.queue.qsize()} | Total messages: {self.msg_count} | Dropped messages: {self.drop_count}"
            )


async def main() -> None:
    logging.basicConfig(
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        level=logging.INFO,
    )
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)
    log.info("Initializing ClickHouse writer...")
    writer = ClickhouseWriter()
    await writer.connect()
    queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=100_000)
    log.info("Starting UDP server...")
    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ServerProtocol(queue, writer, shutdown_event),
        local_addr=("0.0.0.0", 9999),
    )
    try:
        async with asyncio.TaskGroup() as tg:
            tg.create_task(protocol.write())
            tg.create_task(protocol.monitor(freq_sec=10.0))
    finally:
        await writer.flush()
        log.info("Flushed all buffered data on exit")
        transport.close()
        log.info("Transport closed")


if __name__ == "__main__":
    asyncio.run(main())
