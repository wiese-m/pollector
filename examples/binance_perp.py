import asyncio
import json
import signal
import time

import aiohttp
import websockets

from examples.common import ClientProtocol


async def fetch_active_symbols() -> list[str]:
    url = "https://fapi.binance.com/fapi/v1/exchangeInfo"
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            response.raise_for_status()
            data = await response.json()
    symbols = [
        s["symbol"].lower()
        for s in data["symbols"]
        if s["contractType"] == "PERPETUAL" and s["status"] == "TRADING"
    ]
    return symbols


async def write_trades(
    symbols: list[str], transport: asyncio.DatagramTransport
) -> None:
    streams = "/".join(f"{symbol.lower()}@trade" for symbol in symbols)
    url = f"wss://fstream.binance.com/stream?streams={streams}"
    async with websockets.connect(url) as ws:
        print("Connected to WS -> consuming messages...")
        async for msg in ws:
            ts_recv = time.time_ns()
            msg_data = json.loads(msg)
            is_taker_sell = "1" if msg_data["data"]["m"] else "0"
            ts_execution = int(msg_data["data"]["T"]) * 1_000_000
            ts_event = int(msg_data["data"]["E"]) * 1_000_000
            trade = (
                f"{ts_recv}|"
                f"binance-perp|"
                f"{msg_data['data']['s']}|"
                f"{msg_data['data']['p']}|"
                f"{msg_data['data']['q']}|"
                f"{is_taker_sell}|"
                f"{msg_data['data']['t']}|"
                f"{ts_execution}|{ts_event}"
            )
            transport.sendto(trade.encode())


async def main() -> None:
    shutdown_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown_event.set)
    print("Fetching active symbols...")
    symbols = await fetch_active_symbols()
    print(f"Fetched {len(symbols)} active symbols")
    transport, protocol = await loop.create_datagram_endpoint(
        protocol_factory=ClientProtocol,
        remote_addr=("127.0.0.1", 9999),
    )
    tasks = {
        asyncio.create_task(shutdown_event.wait()),
        asyncio.create_task(write_trades(symbols, transport)),
    }
    try:
        await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
    finally:
        transport.close()


if __name__ == "__main__":
    asyncio.run(main())
