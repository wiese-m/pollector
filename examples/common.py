import asyncio


class ClientProtocol(asyncio.DatagramProtocol):
    def __init__(self) -> None:
        self.transport: asyncio.DatagramTransport | None = None

    def connection_made(self, transport: asyncio.DatagramTransport) -> None:
        print("Connected to UDP server")
        self.transport = transport

    def error_received(self, exc: Exception) -> None:
        print("Error received:", exc)

    def connection_lost(self, exc: Exception | None) -> None:
        if exc is None:
            print("Connection closed OK")
        else:
            print("Connection closed with exception:", exc)
