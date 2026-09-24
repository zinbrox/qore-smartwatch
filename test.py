import asyncio
from datetime import datetime
from bleak import BleakScanner, BleakClient


def on_notify(char, data: bytearray):
    ts = datetime.now().strftime("%H:%M:%S.%f")[:-3]
    print(f"{ts} <- {char.uuid}: {data.hex(' ')}")


async def main():
    print("Scanning for 8 seconds... (turn off Bluetooth on your phone first)\n")
    devices = await BleakScanner.discover(timeout=8, return_adv=True)

    # Strongest signal first — hold the band next to the laptop
    for addr, (d, adv) in sorted(devices.items(), key=lambda x: -x[1][1].rssi):
        mfr = {k: v.hex() for k, v in adv.manufacturer_data.items()}
        print(f"{addr}  rssi={adv.rssi:4}  name={adv.local_name or d.name}")
        if adv.service_uuids:
            print(f"    services={adv.service_uuids}")
        if mfr:
            print(f"    mfr={mfr}")

    addr = input("\nBand address: ").strip()

    print(f"\nConnecting to {addr}...")
    async with BleakClient(addr) as client:
        print("Connected\n")

        for service in client.services:
            print(f"Service {service.uuid}  ({service.description})")
            for char in service.characteristics:
                print(f"  Char {char.uuid}  {char.properties}  handle={char.handle}")

                if "read" in char.properties:
                    try:
                        value = await client.read_gatt_char(char)
                        print(f"      value: {value.hex(' ')}  | {value!r}")
                    except Exception as e:
                        print(f"      read failed: {e}")

                for desc in char.descriptors:
                    print(f"      Desc {desc.uuid}  handle={desc.handle}")

                if "notify" in char.properties or "indicate" in char.properties:
                    try:
                        await client.start_notify(char, on_notify)
                        print("      -> subscribed")
                    except Exception as e:
                        print(f"      subscribe failed: {e}")
            print()

        # Once you know a command from the HCI snoop log, send it here, e.g.:
        # await client.write_gatt_char("WRITE-CHAR-UUID", bytes.fromhex("aa bb cc"), response=False)

        print("Listening for notifications for 120 seconds... (Ctrl+C to stop)\n")
        await asyncio.sleep(120)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped")