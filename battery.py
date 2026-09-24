import asyncio
import os

from bleak import BleakClient

from watch import load_env

load_env()
ADDR = os.environ["QORE_ADDR"]  # set it in .env (see .env.example)
WRITE = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
NOTIFY = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"

def make_packet(cmd: int, data: bytes = b"") -> bytes:
    p = bytearray(16)
    p[0] = cmd
    p[1:1 + len(data)] = data
    p[15] = sum(p[:15]) & 0xFF
    return bytes(p)

def on_notify(_, data: bytearray):
    print("<-", data.hex(" "))
    if data[0] == 0x03:
        print(f"   battery={data[1]}%  charging={bool(data[2])}")

async def main():
    async with BleakClient(ADDR) as c:
        await c.start_notify(NOTIFY, on_notify)
        pkt = make_packet(0x03)
        print("->", pkt.hex(" "))
        await c.write_gatt_char(WRITE, pkt, response=False)
        await asyncio.sleep(3)

asyncio.run(main())
