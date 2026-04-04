import asyncio
import json
import time

import requests
import websockets


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


BASE_WS_URL = "wss://fstream.binance.com/stream?streams="


def get_all_futures_symbols():
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    data = requests.get(url).json()
    symbols = [
        s["symbol"].lower()
        for s in data["symbols"]
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
    ]
    return symbols


def build_trade_ws_uri(symbols):
    streams = [f"{symbol}@trade" for symbol in symbols]
    return BASE_WS_URL + "/".join(streams)


async def binance_feed(transport: asyncio.DatagramTransport) -> None:
    symbols = get_all_futures_symbols()
    print(len(symbols))
    # symbols = symbols[:100]
    ws_uri = build_trade_ws_uri(symbols)
    async with websockets.connect(ws_uri) as ws:
        async for msg in ws:
            ts_recv = time.time_ns()
            msg_data = json.loads(msg)
            is_taker_sell = "1" if msg_data["data"]["m"] else "0"
            ts_execution = int(msg_data["data"]["T"]) * 1_000_000
            ts_event = int(msg_data["data"]["E"]) * 1_000_000
            trade = f"{ts_recv}|binance-perp|{msg_data['data']['s']}|{msg_data['data']['p']}|{msg_data['data']['q']}|{is_taker_sell}|{msg_data['data']['t']}|{ts_execution}|{ts_event}"
            transport.sendto(trade.encode())


async def main():
    loop = asyncio.get_running_loop()

    on_con_lost = loop.create_future()

    transport, protocol = await loop.create_datagram_endpoint(
        lambda: ClientProtocol(on_con_lost), remote_addr=("127.0.0.1", 9999)
    )

    asyncio.create_task(binance_feed(transport))
    try:
        await on_con_lost
    finally:
        transport.close()


asyncio.run(main())
