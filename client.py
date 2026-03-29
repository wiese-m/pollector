import asyncio

from model import TRADE_DATA_EXAMPLE


class ClientProtocol:
    def __init__(self, on_con_lost):
        self.on_con_lost = on_con_lost

    def connection_made(self, transport):
        print("Connection made")
        self.transport = transport

    def datagram_received(self, data, addr):
        print("Received:", data.decode())

        print("Close the socket")
        self.transport.close()

    def error_received(self, exc):
        print("Error received:", exc)

    def connection_lost(self, exc):
        print("Connection closed")
        self.on_con_lost.set_result(True)


async def worker(transport: asyncio.DatagramTransport) -> None:
    while True:
        transport.sendto(TRADE_DATA_EXAMPLE.encode())
        await asyncio.sleep(0)


async def main():
    loop = asyncio.get_running_loop()

    on_con_lost = loop.create_future()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ClientProtocol(on_con_lost), remote_addr=("127.0.0.1", 9999)
    )

    asyncio.create_task(worker(transport))
    try:
        await on_con_lost
    finally:
        transport.close()


asyncio.run(main())
