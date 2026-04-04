import asyncio
import signal
from typing import Final

from examples.common import ClientProtocol

MOCK_TRADE: Final[bytes] = b"0|EXCHANGE|SYMBOL|10.0|0.5|0|222|0|0"


async def generate_trades(
    transport: asyncio.DatagramTransport, freq_sec: float, shutdown_event: asyncio.Event
) -> None:
    while not shutdown_event.is_set():
        transport.sendto(MOCK_TRADE)
        await asyncio.sleep(freq_sec)


async def main() -> None:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)
    transport, protocol = await loop.create_datagram_endpoint(
        protocol_factory=ClientProtocol,
        remote_addr=("127.0.0.1", 9999),
    )
    try:
        await generate_trades(transport, freq_sec=0.001, shutdown_event=shutdown_event)
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
