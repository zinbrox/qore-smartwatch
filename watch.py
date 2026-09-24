#!/usr/bin/env python3
"""
qore.py - read data from a Pebble Qore band (Colmi / QRing protocol) over BLE.

Only needs `bleak`:  pip install bleak   (works on Python 3.9+)

Usage examples:
    python qore.py scan
    python qore.py info
    python qore.py caps                        # which features the band says it supports
    python qore.py hr-log --days 3
    python qore.py steps --days 3
    python qore.py live-hr
    python qore.py live-spo2
    python qore.py measure stress              # on-demand stress / hrv / check (health check)
    python qore.py totals                      # today's steps, kcal, distance, active minutes
    python qore.py alarms
    python qore.py probe                       # how many days of history the band holds
    python qore.py export --days 7 --out data
    python qore.py listen --seconds 60
    python qore.py raw 03                      # battery (checksum added automatically)
    python qore.py stress --days 2             # 30-minute stress scores
    python qore.py hrv --days 2                # 30-minute HRV (ms)
    python qore.py sleep                       # sleep sessions + stages
    python qore.py spo2                        # hourly SpO2 history
    python qore.py goals
    python qore.py auto-measure --enable stress,hrv
    python qore.py bigdata sleep               # raw dump via the v2 service

By default the band is found by its advertised name ("PBL Qore").
Pass --address <macOS UUID> to skip the scan, or set QORE_ADDR in your env or in a .env file
next to this script (see .env.example). QORE_NAME overrides the name prefix.

Close the Pebble app / turn off phone Bluetooth first: the band accepts one connection at a time.
"""
from __future__ import annotations

import argparse
import asyncio
import calendar
import csv
import json
import os
import sqlite3
import struct
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from bleak import BleakClient, BleakScanner

# --- BLE UUIDs ---------------------------------------------------------------
UART_WRITE = "6e400002-b5a3-f393-e0a9-e50e24dcca9e"
UART_NOTIFY = "6e400003-b5a3-f393-e0a9-e50e24dcca9e"
V2_WRITE = "de5bf72a-d711-4e47-af26-65e3012a5dc7"
V2_NOTIFY = "de5bf729-d711-4e47-af26-65e3012a5dc7"
DEV_HW = "00002a27-0000-1000-8000-00805f9b34fb"
DEV_FW = "00002a26-0000-1000-8000-00805f9b34fb"

def load_env(path: Path = Path(__file__).with_name(".env")):
    """Minimal .env reader (KEY=value lines); real environment variables win."""
    if not path.exists():
        return
    for line in path.read_text().splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, value = line.split("=", 1)
            os.environ.setdefault(key.strip(), value.strip().strip("'\""))


load_env()
DEFAULT_NAME = os.environ.get("QORE_NAME", "PBL Qore")

# --- Command IDs (first byte of a 16-byte packet) ----------------------------
CMD_SET_TIME = 0x01
CMD_BATTERY = 0x03
CMD_HR_LOG = 0x15
CMD_HR_SETTINGS = 0x16
CMD_RT_CONTINUE = 0x1E
CMD_GOALS = 0x21
CMD_TODAY_TOTALS = 0x48
CMD_SPO2_SETTINGS = 0x2C
CMD_STRESS_SETTINGS = 0x36
CMD_STRESS = 0x37
CMD_HRV_SETTINGS = 0x38
CMD_HRV = 0x39
CMD_TEMP_SETTINGS = 0x3A
CMD_STEPS = 0x43
CMD_RT_START = 0x69
CMD_RT_STOP = 0x6A

RT_HEART_RATE = 0x01
RT_SPO2 = 0x03

# On-demand measurements via CMD_RT_START: name -> (type, start payload)
# Replies are [0x69][type][err][value][x][y][beat interval ms u16 LE]; err 1 = not worn, 2 = no data.
MEASURE = {
    "hr": (RT_HEART_RATE, b"\x01\x01"),
    "spo2": (RT_SPO2, b"\x03\x25"),
    "check": (0x05, b"\x05\x01"),   # one-key health check; x/y look like a blood-pressure estimate
    "stress": (0x08, b"\x08\x01"),
    "hrv": (0x0A, b"\x0a\x01"),
}
MEASURE_UNITS = {"hr": "bpm", "spo2": "%", "check": "bpm", "stress": "", "hrv": "ms"}

# v2 "big data" requests: name -> (type, payload)
BIG_SLEEP = 0x27
BIG_SPO2 = 0x2A
BIG_TEMP = 0x25
BIG_ALARMS = 0x2C
BIGDATA_REQUESTS = {
    "sleep": (BIG_SLEEP, b"\xff"),
    "spo2": (BIG_SPO2, b"\xff"),
    "temp": (BIG_TEMP, b"\x02"),
}

# Automatic measurement toggles: name -> (cmd, prefix, index of the enabled flag in the reply)
AUTO_MEASURE = {
    "stress": (CMD_STRESS_SETTINGS, b"", 2),
    "hrv": (CMD_HRV_SETTINGS, b"", 2),
    "spo2": (CMD_SPO2_SETTINGS, b"", 2),
    "temp": (CMD_TEMP_SETTINGS, b"\x03", 3),
}

SLEEP_STAGES = {2: "light", 3: "deep", 4: "rem", 5: "awake"}


# --- Packet helpers ----------------------------------------------------------
def make_packet(cmd: int, data: bytes = b"") -> bytes:
    """16 bytes: [cmd][up to 14 bytes payload][checksum = sum(bytes 0..14) & 0xFF]"""
    if len(data) > 14:
        raise ValueError("payload must be <= 14 bytes")
    p = bytearray(16)
    p[0] = cmd
    p[1:1 + len(data)] = data
    p[15] = sum(p[:15]) & 0xFF
    return bytes(p)


def bcd(n: int) -> int:
    return ((n // 10) << 4) | (n % 10)


def from_bcd(b: int) -> int:
    return ((b >> 4) & 0x0F) * 10 + (b & 0x0F)


def hexs(b: bytes) -> str:
    return b.hex(" ")


def crc16_modbus(data: bytes) -> int:
    crc = 0xFFFF
    for b in data:
        crc ^= b
        for _ in range(8):
            crc = (crc >> 1) ^ 0xA001 if crc & 1 else crc >> 1
    return crc


def make_bigdata(kind: int, payload: bytes) -> bytes:
    """v2 frame: [0xBC][type][len u16 LE][crc16 u16 LE][payload]"""
    return bytes([0xBC, kind]) + struct.pack("<HH", len(payload), crc16_modbus(payload)) + payload


def midnight(day: date) -> datetime:
    return datetime(day.year, day.month, day.day)


# --- Parsers (pure, no BLE) --------------------------------------------------
class HeartRateLogParser:
    """
    Response to CMD_HR_LOG arrives as several packets:
      sub 0   : [2]=number of packets, [3]=interval in minutes
      sub 1   : [2:6]=timestamp (LE int32), [6:15]=first 9 readings
      sub 2.. : [2:15]=13 readings each, last one has sub == size-1
      sub 255 : no data for that day
    """

    def __init__(self):
        self.size = 0
        self.interval = 5
        self.base_ts: int | None = None
        self.values: list[int] = []
        self.done = False
        self.no_data = False

    def feed(self, pkt: bytes) -> bool:
        sub = pkt[1]
        if sub == 0xFF:
            self.no_data = self.done = True
        elif sub == 0:
            self.size = pkt[2]
            self.interval = pkt[3] if 1 <= pkt[3] <= 60 else 5
            if self.size <= 1:
                self.no_data = self.done = True
        elif sub == 1:
            self.base_ts = struct.unpack_from("<l", pkt, 2)[0]
            self.values.extend(pkt[6:15])
            if self.size and sub >= self.size - 1:
                self.done = True
        else:
            self.values.extend(pkt[2:15])
            if self.size and sub >= self.size - 1:
                self.done = True
        return self.done


class StepsParser:
    """
    Response to CMD_STEPS:
      first packet [1]=0xF0 header ([3]==1 -> calories are x10), or [1]=0xFF for no data
      then one packet per 15-minute slot:
        [1..3]=yy mm dd (BCD), [4]=slot index (15 min each), [5]=packet index, [6]=total packets
        [7:9]=calories (small cal), [9:11]=steps, [11:13]=distance (m), all LE uint16
    """

    def __init__(self):
        self.new_calories = False
        self.rows: list[dict] = []
        self.started = False
        self.done = False

    def feed(self, pkt: bytes) -> bool:
        if not self.started:
            self.started = True
            if pkt[1] == 0xFF:
                self.done = True
                return True
            if pkt[1] == 0xF0:
                self.new_calories = pkt[3] == 1
                return False
        year = from_bcd(pkt[1]) + 2000
        month = from_bcd(pkt[2])
        day = from_bcd(pkt[3])
        slot = pkt[4]
        calories = pkt[7] | (pkt[8] << 8)
        if self.new_calories:
            calories *= 10
        steps = pkt[9] | (pkt[10] << 8)
        distance = pkt[11] | (pkt[12] << 8)
        try:
            start = datetime(year, month, day) + timedelta(minutes=15 * slot)
        except ValueError:
            start = None
        self.rows.append(
            {"time": start, "steps": steps, "kcal": round(calories / 1000, 1), "distance_m": distance}
        )
        if pkt[5] >= pkt[6] - 1:
            self.done = True
        return self.done


class HalfHourLogParser:
    """
    Response to CMD_STRESS / CMD_HRV (one day, one byte per slot, 0 = no reading):
      sub 0   : [2]=number of packets, [3]=interval in minutes (30)
      sub 1   : [2]=days ago (echo), [3:15]=first 12 slots
      sub 2.. : [2:15]=13 slots each, last one has sub == size-1
      sub 255 : no data for that day
    """

    def __init__(self):
        self.size = 0
        self.interval = 30
        self.values: list[int] = []
        self.done = False

    def feed(self, pkt: bytes) -> bool:
        sub = pkt[1]
        if sub == 0xFF:
            self.done = True
        elif sub == 0:
            self.size = pkt[2]
            self.interval = pkt[3] if 1 <= pkt[3] <= 60 else 30
            if self.size <= 1:
                self.done = True
        else:
            self.values.extend(pkt[3:15] if sub == 1 else pkt[2:15])
            if self.size and sub >= self.size - 1:
                self.done = True
        return self.done


def parse_sleep(payload: bytes, today: date) -> list[dict]:
    """
    Big data 0x27 payload: [n days] then per day
      [days ago][len][start u16][end u16][(stage, minutes) * (len-4)/2]
    start/end are minutes after midnight of that day; start > end means sleep began the evening before.
    """
    sessions = []
    if not payload:
        return sessions
    i = 1
    for _ in range(payload[0]):
        if i + 6 > len(payload):
            break
        days_ago, n = payload[i], payload[i + 1]
        start_min, end_min = struct.unpack_from("<HH", payload, i + 2)
        pairs = payload[i + 6:i + 2 + n]
        i += 2 + n
        base = midnight(today - timedelta(days=days_ago))
        start = base + timedelta(minutes=start_min - (1440 if start_min > end_min else 0))
        stages, t = [], start
        for k in range(0, len(pairs) - 1, 2):
            stage, minutes = pairs[k], pairs[k + 1]
            if minutes == 0:
                continue
            end = t + timedelta(minutes=minutes)
            if stage in SLEEP_STAGES:
                stages.append((t, end, SLEEP_STAGES[stage]))
            t = end
        if stages:
            totals = {name: 0 for name in SLEEP_STAGES.values()}
            for a, b, name in stages:
                totals[name] += int((b - a).total_seconds() // 60)
            sessions.append({"start": start, "end": base + timedelta(minutes=end_min),
                             "stages": stages, **totals})
    return sessions


def parse_spo2(payload: bytes, today: date) -> list[tuple[datetime, int]]:
    """Big data 0x2A payload: 49-byte day records [days ago][(min, max) * 24 hours]; 0 = no reading."""
    rows, now = [], datetime.now()
    for i in range(0, len(payload) - 48, 49):
        base = midnight(today - timedelta(days=payload[i]))
        for h in range(24):
            lo, hi = payload[i + 1 + 2 * h], payload[i + 2 + 2 * h]
            t = base + timedelta(hours=h)
            if lo and hi and t <= now:
                rows.append((t, round((lo + hi) / 2)))
    return rows


def parse_temperature(payload: bytes, today: date) -> list[tuple[datetime, float]]:
    """
    Big data 0x25 payload (unverified on this band): per day
      [days ago][interval?][(value at :00, value at :30) * 24]; celsius = raw/10 + 20, 0 = none.
    """
    rows, now = [], datetime.now()
    if len(payload) < 50:
        return rows
    for i in range(0, len(payload) - 49, 50):
        base = midnight(today - timedelta(days=payload[i]))
        for k in range(48):
            raw = payload[i + 2 + k]
            t = base + timedelta(minutes=30 * k)
            if raw and t <= now:
                rows.append((t, round(raw / 10 + 20, 1)))
    return rows


def parse_today_totals(pkt: bytes) -> dict:
    """CMD_TODAY_TOTALS reply, big-endian: [1:4] steps, [4:7] running steps, [7:10] cal, [10:13] m, [13:15] min"""
    u24 = lambda i: (pkt[i] << 16) | (pkt[i + 1] << 8) | pkt[i + 2]
    return {"steps": u24(1), "running_steps": u24(4), "kcal": round(u24(7) / 1000, 1),
            "distance_m": u24(10), "active_min": (pkt[13] << 8) | pkt[14]}


def parse_alarms(payload: bytes) -> list[dict]:
    """Big data 0x2C reply: [op][count] then [len][flags: 0x80 on, bits 0-6 Sun..Sat][minute u16][name]"""
    alarms, i = [], 2
    for _ in range(payload[1] if len(payload) > 1 else 0):
        n = payload[i]
        flags, minute = payload[i + 1], struct.unpack_from("<H", payload, i + 2)[0]
        days = [d for b, d in enumerate(["Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"]) if flags & (1 << b)]
        alarms.append({"time": f"{minute // 60:02d}:{minute % 60:02d}", "on": bool(flags & 0x80), "days": days,
                       "name": payload[i + 4:i + n].decode(errors="replace")})
        i += n
    return alarms


def parse_capabilities(pkt: bytes) -> dict:
    """
    The band answers CMD_SET_TIME with a feature bitmap.
    Bit meanings come from the Colmi/QRing app (as decoded by colmi_r02_client / Gadgetbridge).
    """
    b = pkt[1:]
    caps = {
        "temperature": b[0] == 1,
        "custom_wallpaper": bool(b[3] & 0x01),
        "spo2": bool(b[3] & 0x02),
        "blood_pressure": bool(b[3] & 0x04),
        "one_key_check": bool(b[3] & 0x10),
        "weather": bool(b[3] & 0x20),
        "wechat": not (b[3] & 0x40),
        "new_sleep_protocol": b[8] == 1,
        "contacts": bool(b[10] & 0x01),
        "gps": bool(b[10] & 0x08),
        "manual_heart_rate": bool(b[11] & 0x01),
        "music": bool(b[11] & 0x10),
        "realtek_mcu": bool(b[11] & 0x20),
        "blood_sugar": bool(b[11] & 0x80),
        "bp_settings": bool(b[13] & 0x02),
        "stress": bool(b[13] & 0x10),
        "hrv": bool(b[13] & 0x20),
    }
    return caps


# --- Client ------------------------------------------------------------------
class Qore:
    def __init__(self, target, band_utc: bool = False, record: Path | None = None, verbose: bool = False,
                 on_packet=None):
        self.client = BleakClient(target)
        self.on_packet = on_packet  # optional hook, sees every UART packet (e.g. unsolicited 0x73 events)
        self.band_utc = band_utc
        self.record = record
        self.verbose = verbose
        self.queues: dict[int, asyncio.Queue] = defaultdict(asyncio.Queue)
        self.v2_queue: asyncio.Queue = asyncio.Queue()

    async def __aenter__(self) -> "Qore":
        await self.client.connect()
        await self.client.start_notify(UART_NOTIFY, self._on_uart)
        try:
            await self.client.start_notify(V2_NOTIFY, self._on_v2)
        except Exception as e:  # not fatal, only needed for bigdata
            print(f"(v2 notify unavailable: {e})", file=sys.stderr)
        return self

    async def __aexit__(self, *exc):
        try:
            await self.client.disconnect()
        except Exception:
            pass

    def _log(self, direction: str, channel: str, data: bytes):
        line = f"{datetime.now().isoformat(timespec='milliseconds')} {direction} {channel} {hexs(data)}"
        if self.verbose:
            print(line, file=sys.stderr)
        if self.record:
            with self.record.open("a") as f:
                f.write(line + "\n")

    def _on_uart(self, _char, data: bytearray):
        pkt = bytes(data)
        self._log("<-", "uart", pkt)
        if len(pkt) != 16:
            return
        if self.on_packet:
            self.on_packet(pkt)
        self.queues[pkt[0] & 0x7F].put_nowait(pkt)

    def _on_v2(self, _char, data: bytearray):
        pkt = bytes(data)
        self._log("<-", "v2", pkt)
        self.v2_queue.put_nowait(pkt)

    async def send(self, pkt: bytes):
        self._log("->", "uart", pkt)
        await self.client.write_gatt_char(UART_WRITE, pkt, response=False)

    async def send_v2(self, data: bytes):
        self._log("->", "v2", data)
        await self.client.write_gatt_char(V2_WRITE, data, response=False)

    async def _get(self, cmd: int, timeout: float = 3.0) -> bytes:
        pkt = await asyncio.wait_for(self.queues[cmd].get(), timeout)
        if pkt[0] & 0x80:
            raise RuntimeError(f"band returned error for cmd 0x{cmd:02x}: {hexs(pkt)}")
        return pkt

    def _drain(self, cmd: int):
        q = self.queues[cmd]
        while not q.empty():
            q.get_nowait()

    # -- simple reads --
    async def device_info(self) -> dict:
        info = {}
        for key, uuid in (("hardware", DEV_HW), ("firmware", DEV_FW)):
            try:
                info[key] = (await self.client.read_gatt_char(uuid)).decode(errors="replace")
            except Exception as e:
                info[key] = f"? ({e})"
        return info

    async def battery(self) -> tuple[int, bool]:
        self._drain(CMD_BATTERY)
        await self.send(make_packet(CMD_BATTERY))
        pkt = await self._get(CMD_BATTERY)
        return pkt[1], bool(pkt[2])

    async def hr_settings(self) -> tuple[bool, int]:
        self._drain(CMD_HR_SETTINGS)
        await self.send(make_packet(CMD_HR_SETTINGS, b"\x01"))
        pkt = await self._get(CMD_HR_SETTINGS)
        return pkt[2] == 1, pkt[3]

    async def set_time(self, when: datetime) -> bytes | None:
        """Sets the clock; returns the capability packet the band sends back (or None)."""
        self._drain(CMD_SET_TIME)
        d = bytes([bcd(when.year % 2000), bcd(when.month), bcd(when.day),
                   bcd(when.hour), bcd(when.minute), bcd(when.second), 1])
        await self.send(make_packet(CMD_SET_TIME, d))
        try:
            return await self._get(CMD_SET_TIME)
        except (asyncio.TimeoutError, RuntimeError):
            return None

    # -- history --
    def _day_request_ts(self, day: date) -> int:
        if self.band_utc:
            return int(datetime(day.year, day.month, day.day, tzinfo=timezone.utc).timestamp())
        # band clock holds local wall time encoded as if it were UTC
        return calendar.timegm(day.timetuple())

    def _band_ts_to_local(self, ts: int) -> datetime:
        dt = datetime.fromtimestamp(ts, timezone.utc)
        if self.band_utc:
            return dt.astimezone().replace(tzinfo=None)
        return dt.replace(tzinfo=None)

    async def hr_log(self, day: date) -> list[tuple[datetime, int]]:
        self._drain(CMD_HR_LOG)
        await self.send(make_packet(CMD_HR_LOG, struct.pack("<L", self._day_request_ts(day))))
        parser = HeartRateLogParser()
        try:
            while not parser.feed(await self._get(CMD_HR_LOG)):
                pass
        except asyncio.TimeoutError:
            if not parser.values:
                return []
        if parser.no_data:
            return []

        if parser.base_ts is not None:
            start = self._band_ts_to_local(parser.base_ts)
            start = start.replace(hour=0, minute=0, second=0, microsecond=0)
        else:
            start = datetime(day.year, day.month, day.day)
        slots = 1440 // parser.interval
        now = datetime.now()
        rows = []
        for i, bpm in enumerate(parser.values[:slots]):
            t = start + timedelta(minutes=i * parser.interval)
            if bpm > 0 and t <= now:
                rows.append((t, bpm))
        return rows

    async def steps(self, days_ago: int) -> list[dict]:
        self._drain(CMD_STEPS)
        await self.send(make_packet(CMD_STEPS, bytes([days_ago, 0x0F, 0x00, 0x5F, 0x01])))
        parser = StepsParser()
        try:
            while not parser.feed(await self._get(CMD_STEPS)):
                pass
        except asyncio.TimeoutError:
            pass
        return [r for r in parser.rows if r["time"] is not None]

    async def _half_hour_log(self, cmd: int, days_ago: int) -> list[tuple[datetime, int]]:
        self._drain(cmd)
        await self.send(make_packet(cmd, bytes([days_ago])))
        parser = HalfHourLogParser()
        try:
            while not parser.feed(await self._get(cmd)):
                pass
        except asyncio.TimeoutError:
            pass
        base = midnight(date.today() - timedelta(days=days_ago))
        now = datetime.now()
        rows = []
        for i, v in enumerate(parser.values[:1440 // parser.interval]):
            t = base + timedelta(minutes=i * parser.interval)
            if v and t <= now:
                rows.append((t, v))
        return rows

    async def stress(self, days_ago: int = 0) -> list[tuple[datetime, int]]:
        return await self._half_hour_log(CMD_STRESS, days_ago)

    async def hrv(self, days_ago: int = 0) -> list[tuple[datetime, int]]:
        return await self._half_hour_log(CMD_HRV, days_ago)

    async def bigdata(self, kind: int, payload: bytes, timeout: float = 4.0) -> bytes | None:
        """Send a v2 request and reassemble the (possibly multi-notification) reply payload."""
        while not self.v2_queue.empty():
            self.v2_queue.get_nowait()
        await self.send_v2(make_bigdata(kind, payload))
        buf = bytearray()
        need = None
        while need is None or len(buf) < need:
            pkt = await asyncio.wait_for(self.v2_queue.get(), timeout)
            if not buf and not (len(pkt) >= 6 and pkt[0] == 0xBC and pkt[1] == kind):
                continue  # not the start of our reply
            buf += pkt
            if need is None:
                need = 6 + struct.unpack_from("<H", buf, 2)[0]
        length, crc = struct.unpack_from("<HH", buf, 2)
        body = bytes(buf[6:6 + length])
        if crc16_modbus(body) != crc:
            print(f"(big data 0x{kind:02x}: checksum mismatch, data may be corrupt)", file=sys.stderr)
        return body

    async def sleep(self) -> list[dict]:
        return parse_sleep(await self.bigdata(BIG_SLEEP, b"\xff"), date.today())

    async def spo2(self) -> list[tuple[datetime, int]]:
        return parse_spo2(await self.bigdata(BIG_SPO2, b"\xff"), date.today())

    async def temperature(self) -> list[tuple[datetime, float]]:
        return parse_temperature(await self.bigdata(BIG_TEMP, b"\x02"), date.today())

    async def today_totals(self) -> dict:
        self._drain(CMD_TODAY_TOTALS)
        await self.send(make_packet(CMD_TODAY_TOTALS))
        return parse_today_totals(await self._get(CMD_TODAY_TOTALS))

    async def alarms(self) -> list[dict]:
        return parse_alarms(await self.bigdata(BIG_ALARMS, b"\x01"))

    async def goals(self) -> dict:
        self._drain(CMD_GOALS)
        await self.send(make_packet(CMD_GOALS, b"\x01"))
        p = await self._get(CMD_GOALS)
        u24 = lambda i: p[i] | (p[i + 1] << 8) | (p[i + 2] << 16)
        return {"steps": u24(2), "kcal": u24(5) // 1000, "distance_m": u24(8),
                "sport_min": p[11] | (p[12] << 8), "sleep_min": p[13] | (p[14] << 8)}

    async def auto_measure(self, kind: str) -> bool:
        cmd, prefix, idx = AUTO_MEASURE[kind]
        self._drain(cmd)
        await self.send(make_packet(cmd, prefix + b"\x01"))
        return (await self._get(cmd))[idx] == 1

    async def set_auto_measure(self, kind: str, on: bool):
        cmd, prefix, _ = AUTO_MEASURE[kind]
        self._drain(cmd)
        await self.send(make_packet(cmd, prefix + bytes([2, 1 if on else 0])))
        try:
            await self._get(cmd, timeout=1.5)
        except asyncio.TimeoutError:
            pass  # some firmware doesn't acknowledge writes

    async def set_hr_logging(self, on: bool, interval: int = 5):
        self._drain(CMD_HR_SETTINGS)
        await self.send(make_packet(CMD_HR_SETTINGS, bytes([2, 1 if on else 2, interval])))
        try:
            await self._get(CMD_HR_SETTINGS, timeout=1.5)
        except asyncio.TimeoutError:
            pass

    # -- live readings --
    async def realtime_hr(self):
        """
        Continuous heart rate (~1/s) via CMD_RT_CONTINUE: [1E 01] start, [1E 03] keep-alive, [1E 02] stop.
        Yields bpm, or 0 while the sensor warms up (~15 s). Runs until the caller stops iterating.
        """
        self._drain(CMD_RT_CONTINUE)
        await self.send(make_packet(CMD_RT_CONTINUE, b"\x01"))
        loop = asyncio.get_running_loop()
        last_keepalive = loop.time()
        try:
            while True:
                if loop.time() - last_keepalive > 20:  # the band ends real-time mode after ~60 s otherwise
                    await self.send(make_packet(CMD_RT_CONTINUE, b"\x03"))
                    last_keepalive = loop.time()
                try:
                    pkt = await self._get(CMD_RT_CONTINUE, timeout=3)
                except asyncio.TimeoutError:
                    await self.send(make_packet(CMD_RT_CONTINUE, b"\x03"))
                    last_keepalive = loop.time()
                    continue
                yield pkt[1]
        finally:
            await self.send(make_packet(CMD_RT_CONTINUE, b"\x02"))

    async def measure(self, kind: str, max_seconds: float = 60, settle: int = 8, keepalive: bool = False):
        """
        Run an on-demand measurement, yielding dicts as the band reports:
          {"state": "measuring", "rr": ms|None}           beat intervals while it works
          {"state": "value", "value": v, "extra": (x, y), "rr": ms|None}
          {"state": "error", "code": n}                   1 = not worn properly, 2 = no data
        Stops by itself `settle` readings after the first value (or after max_seconds);
        pass settle=0 to stream until the caller stops iterating.
        keepalive re-arms real-time mode every 20 s for long heart-rate streams; don't use it for
        stress/HRV/check, where the extra packet restarts the measurement.
        """
        rt_type, start_payload = MEASURE[kind]
        self._drain(CMD_RT_START)
        await self.send(make_packet(CMD_RT_START, start_payload))
        loop = asyncio.get_running_loop()
        deadline = loop.time() + max_seconds
        got = 0
        last_keepalive = loop.time()
        try:
            while loop.time() < deadline:
                if keepalive and loop.time() - last_keepalive > 20:  # real-time mode ends after ~60 s otherwise
                    await self.send(make_packet(CMD_RT_CONTINUE, b"\x33"))
                    last_keepalive = loop.time()
                try:
                    pkt = await self._get(CMD_RT_START, timeout=2)
                except asyncio.TimeoutError:
                    await self.send(make_packet(CMD_RT_CONTINUE, b"\x33"))
                    continue
                if pkt[1] != rt_type:
                    continue
                rr = pkt[6] | (pkt[7] << 8) or None
                if pkt[2]:
                    yield {"state": "error", "code": pkt[2]}
                    return
                if pkt[3]:
                    got += 1
                    yield {"state": "value", "value": pkt[3], "extra": (pkt[4], pkt[5]), "rr": rr}
                    if settle and got >= settle:
                        return
                else:
                    yield {"state": "measuring", "rr": rr}
        finally:
            await self.send(make_packet(CMD_RT_STOP, bytes([rt_type, 0, 0])))


# --- Connection helper -------------------------------------------------------
async def resolve_target(args):
    addr = args.address or os.environ.get("QORE_ADDR")
    if addr:
        return addr
    print(f"Looking for a device named '{args.name}...' (use --address to skip)", file=sys.stderr)
    dev = await BleakScanner.find_device_by_filter(
        lambda d, adv: (adv.local_name or d.name or "").startswith(args.name), timeout=15
    )
    if not dev:
        sys.exit("Band not found. Close the Pebble app / turn off phone Bluetooth and try again.")
    print(f"Found {dev.name} at {dev.address}", file=sys.stderr)
    return dev


def open_band(args, target) -> Qore:
    return Qore(target, band_utc=args.band_utc,
                record=Path(args.record) if args.record else None,
                verbose=args.verbose)


# --- Commands ----------------------------------------------------------------
async def cmd_scan(args):
    found = await BleakScanner.discover(timeout=8, return_adv=True)
    for addr, (d, adv) in sorted(found.items(), key=lambda x: -x[1][1].rssi):
        name = adv.local_name or d.name
        mark = "  <-- band" if name and name.startswith(args.name) else ""
        print(f"{addr}  rssi={adv.rssi:4}  {name}{mark}")


async def cmd_info(args):
    async with open_band(args, await resolve_target(args)) as q:
        for k, v in (await q.device_info()).items():
            print(f"{k:10}: {v}")
        level, charging = await q.battery()
        print(f"battery   : {level}%{' (charging)' if charging else ''}")
        try:
            enabled, interval = await q.hr_settings()
            print(f"hr logging: {'on' if enabled else 'off'}, every {interval} min")
        except (asyncio.TimeoutError, RuntimeError) as e:
            print(f"hr logging: unknown ({e})")


async def cmd_hr_log(args):
    async with open_band(args, await resolve_target(args)) as q:
        for offset in range(args.days):
            day = date.today() - timedelta(days=offset)
            rows = await q.hr_log(day)
            print(f"\n== {day}  ({len(rows)} readings)")
            for t, bpm in rows:
                print(f"{t:%Y-%m-%d %H:%M}  {bpm} bpm")


async def cmd_steps(args):
    async with open_band(args, await resolve_target(args)) as q:
        for offset in range(args.days):
            rows = await q.steps(offset)
            day = date.today() - timedelta(days=offset)
            total = sum(r["steps"] for r in rows)
            print(f"\n== {day}  total {total} steps")
            for r in rows:
                print(f"{r['time']:%Y-%m-%d %H:%M}  steps={r['steps']:5}  "
                      f"kcal={r['kcal']:5}  dist={r['distance_m']} m")


async def cmd_half_hour(args, kind):
    unit = "ms" if kind == "hrv" else ""
    async with open_band(args, await resolve_target(args)) as q:
        fetch = q.hrv if kind == "hrv" else q.stress
        for offset in range(args.days):
            rows = await fetch(offset)
            day = date.today() - timedelta(days=offset)
            avg = f", avg {sum(v for _, v in rows) / len(rows):.0f}" if rows else ""
            print(f"\n== {day}  ({len(rows)} readings{avg})")
            for t, v in rows:
                print(f"{t:%Y-%m-%d %H:%M}  {kind}={v}{unit}")


async def cmd_sleep(args):
    async with open_band(args, await resolve_target(args)) as q:
        sessions = await q.sleep()
    if not sessions:
        print("No sleep data.")
    for s in sessions:
        total = s["light"] + s["deep"] + s["rem"]
        print(f"\n== {s['start']:%Y-%m-%d %H:%M} -> {s['end']:%H:%M}  asleep {total // 60}h{total % 60:02d}m  "
              f"(light {s['light']}m, deep {s['deep']}m, rem {s['rem']}m, awake {s['awake']}m)")
        for a, b, stage in s["stages"]:
            print(f"   {a:%H:%M}-{b:%H:%M}  {stage}")


async def cmd_spo2(args):
    async with open_band(args, await resolve_target(args)) as q:
        rows = await q.spo2()
    for t, v in rows:
        print(f"{t:%Y-%m-%d %H:%M}  SpO2={v}%")
    print(f"{len(rows)} readings")


async def cmd_temp(args):
    async with open_band(args, await resolve_target(args)) as q:
        rows = await q.temperature()
    for t, v in rows:
        print(f"{t:%Y-%m-%d %H:%M}  {v} C")
    print(f"{len(rows)} readings" if rows else "No temperature data (the band may not record it).")


async def cmd_goals(args):
    async with open_band(args, await resolve_target(args)) as q:
        for k, v in (await q.goals()).items():
            print(f"{k:11}: {v}")


async def ensure_auto_measure(q: Qore, enable: set[str] | None = None) -> dict:
    """Read every automatic-measurement toggle; turn on the ones in `enable` (all if None) that are off."""
    status = {}
    try:
        on, interval = await q.hr_settings()
        if not on and (enable is None or "hr" in enable):
            await q.set_hr_logging(True, interval if 5 <= interval <= 60 else 5)
            on, interval = await q.hr_settings()
            print("enabled automatic heart rate logging")
        status["hr"] = on
        status["hr_interval"] = interval
    except (asyncio.TimeoutError, RuntimeError):
        pass
    for kind in AUTO_MEASURE:
        try:
            on = await q.auto_measure(kind)
            if not on and (enable is None or kind in enable):
                await q.set_auto_measure(kind, True)
                on = await q.auto_measure(kind)
                print(f"enabled automatic {kind} measurement" if on else f"could not enable {kind}")
            status[kind] = on
        except (asyncio.TimeoutError, RuntimeError):
            status[kind] = None  # not supported
    return status


async def cmd_auto_measure(args):
    enable = set(args.enable.split(",")) if args.enable else set()
    async with open_band(args, await resolve_target(args)) as q:
        status = await ensure_auto_measure(q, enable)
    for k, v in status.items():
        print(f"{k:12}: {v if k == 'hr_interval' else {True: 'on', False: 'off', None: 'unsupported'}[v]}")


async def cmd_measure(args, kind: str | None = None):
    kind = kind or args.kind
    unit = MEASURE_UNITS[kind]
    last = None
    async with open_band(args, await resolve_target(args)) as q:
        async for r in q.measure(kind, max_seconds=args.seconds, settle=args.count):
            now = f"{datetime.now():%H:%M:%S}"
            if r["state"] == "error":
                print(f"{now}  band reported error {r['code']} "
                      f"({'is it on your wrist?' if r['code'] == 1 else 'no data'})")
            elif r["state"] == "measuring":
                print(f"{now}  measuring...{f"  beat interval {r['rr']} ms" if r['rr'] else ''}")
            else:
                last = r
                extra = f"  (estimate {r['extra'][0]}/{r['extra'][1]}?)" if kind == "check" and any(r["extra"]) else ""
                print(f"{now}  {kind}: {r['value']}{' ' + unit if unit else ''}{extra}")
    if last:
        print(f"\nresult: {last['value']}{' ' + unit if unit else ''}")


async def cmd_stream(args):
    """
    Machine-readable live session (used by dashboard.py): one JSON event per stdout line,
    commands on stdin ("measure <kind>", "quit"). Streams heart rate continuously between measurements.
    """
    def emit(event: str, **kw):
        print(json.dumps({"type": event, "t": datetime.now().isoformat(timespec="seconds"), **kw}), flush=True)

    loop = asyncio.get_running_loop()
    cmds: asyncio.Queue = asyncio.Queue()
    quitting = asyncio.Event()

    def on_stdin():
        line = sys.stdin.readline().strip() if not quitting.is_set() else "quit"
        if not line or line == "quit":  # EOF means the dashboard went away
            quitting.set()
            loop.remove_reader(sys.stdin)
        cmds.put_nowait(line or "quit")

    def on_packet(pkt: bytes):
        # unsolicited updates the band pushes on its own
        if pkt[0] != 0x73:
            return
        be24 = lambda i: (pkt[i] << 16) | (pkt[i + 1] << 8) | pkt[i + 2]
        if pkt[1] == 0x0C:
            emit("battery", level=pkt[2], charging=bool(pkt[3]))
        elif pkt[1] == 0x12:
            emit("activity", steps=be24(2), kcal=round(be24(5) / 10), distance_m=be24(8))

    async def poll():
        level, charging = await q.battery()
        emit("battery", level=level, charging=charging)
        emit("totals", **(await q.today_totals()))

    async def run_measure(kind: str):
        emit("measure-start", kind=kind)
        await asyncio.sleep(2)  # the band ignores a new measurement started right after the HR stream stops
        start, values = loop.time(), []
        gen = q.measure(kind, max_seconds=90, settle=8)
        try:
            async for r in gen:
                if quitting.is_set():
                    return
                elapsed = round(loop.time() - start, 1)
                if r["state"] == "error":
                    emit("measure-error", kind=kind, code=r["code"])
                    return
                if r["state"] == "value":
                    values.append(r)
                emit("measure-progress", kind=kind, elapsed=elapsed, rr=r.get("rr"), value=r.get("value"))
        finally:
            await gen.aclose()
        if values:
            # readings wobble while the band settles; the median is what it converges on
            mid = sorted(values, key=lambda r: r["value"])[len(values) // 2]
            emit("measure-result", kind=kind, value=mid["value"], extra=list(mid["extra"]))
        else:
            emit("measure-error", kind=kind, code=2)

    loop.add_reader(sys.stdin, on_stdin)
    emit("state", state="connecting")
    try:
        try:
            target = await resolve_target(args)
        except SystemExit as e:  # resolve_target exits with a message when the scan finds nothing
            emit("error", message=str(e))
            return
        q = open_band(args, target)
        q.on_packet = on_packet
        async with q:
            emit("state", state="on")
            await sync_clock(q, args)
            await poll()
            last_poll = loop.time()
            while True:
                if cmds.empty():
                    gen = q.realtime_hr()
                    try:
                        async for bpm in gen:
                            if bpm:
                                emit("hr", value=bpm)
                            else:
                                emit("hr-warmup")
                            if not cmds.empty():
                                break
                            if loop.time() - last_poll > 30:  # other commands can run alongside
                                await poll()
                                last_poll = loop.time()
                    finally:
                        await gen.aclose()
                if not cmds.empty():
                    cmd = cmds.get_nowait().split()
                    if not cmd or cmd[0] == "quit":
                        break
                    if cmd[0] == "measure" and len(cmd) > 1 and cmd[1] in MEASURE:
                        await run_measure(cmd[1])
                if loop.time() - last_poll > 30:
                    await poll()
                    last_poll = loop.time()
    except Exception as e:  # report instead of dying silently; the dashboard shows it
        emit("error", message=str(e) or type(e).__name__)
    finally:
        emit("state", state="off")


async def cmd_totals(args):
    async with open_band(args, await resolve_target(args)) as q:
        for k, v in (await q.today_totals()).items():
            print(f"{k:14}: {v}")


async def cmd_alarms(args):
    async with open_band(args, await resolve_target(args)) as q:
        alarms = await q.alarms()
    for a in alarms:
        print(f"{a['time']}  {'on ' if a['on'] else 'off'}  {','.join(a['days']) or 'once'}  {a['name']}")
    if not alarms:
        print("No alarms set.")


async def cmd_listen(args):
    args.verbose = True
    async with open_band(args, await resolve_target(args)):
        print(f"Listening on both notify channels for {args.seconds}s (Ctrl+C to stop)...", file=sys.stderr)
        await asyncio.sleep(args.seconds)


async def cmd_raw(args):
    data = bytes.fromhex(args.hex)
    args.verbose = True
    async with open_band(args, await resolve_target(args)) as q:
        if args.v2:
            await q.send_v2(data)
        elif len(data) == 16:
            await q.send(data)
        elif 1 <= len(data) <= 15:
            await q.send(make_packet(data[0], data[1:]))
        else:
            sys.exit("give 1-15 bytes (checksum added) or a full 16-byte packet")
        await asyncio.sleep(args.wait)


async def cmd_bigdata(args):
    args.verbose = True
    out = Path(args.out or f"bigdata_{args.kind}_{datetime.now():%Y%m%d_%H%M%S}.txt")
    async with open_band(args, await resolve_target(args)) as q:
        await q.send_v2(make_bigdata(*BIGDATA_REQUESTS[args.kind]))
        packets = []
        try:
            while True:
                packets.append(await asyncio.wait_for(q.v2_queue.get(), timeout=args.wait))
        except asyncio.TimeoutError:
            pass
    out.write_text("\n".join(hexs(p) for p in packets) + "\n")
    print(f"{len(packets)} packets saved to {out}")


async def sync_clock(q: Qore, args) -> bytes | None:
    return await q.set_time(datetime.now(timezone.utc) if args.band_utc else datetime.now())


async def cmd_set_time(args):
    async with open_band(args, await resolve_target(args)) as q:
        now = datetime.now(timezone.utc) if args.band_utc else datetime.now()
        await q.set_time(now)
        print(f"band time set to {now:%Y-%m-%d %H:%M:%S} ({'UTC' if args.band_utc else 'local'})")


async def cmd_caps(args):
    async with open_band(args, await resolve_target(args)) as q:
        now = datetime.now(timezone.utc) if args.band_utc else datetime.now()
        pkt = await q.set_time(now)
        if pkt is None:
            sys.exit("No capability response from the band.")
        print(f"raw: {hexs(pkt)}\n")
        caps = parse_capabilities(pkt)
        for k, v in caps.items():
            print(f"  {'yes' if v else ' - '}  {k}")
        try:
            enabled, interval = await q.hr_settings()
            print(f"\n  HR logging: {'on' if enabled else 'off'}, every {interval} min")
        except (asyncio.TimeoutError, RuntimeError):
            pass


async def cmd_probe(args):
    """Walk back day by day and report how much history the band keeps for each data type."""
    rows, empty_streak = [], 0
    async with open_band(args, await resolve_target(args)) as q:
        sleep_days = {s["end"].date() for s in (await q.sleep())}
        spo2_days = {t.date() for t, _ in (await q.spo2())}
        for offset in range(args.max_days):
            day = date.today() - timedelta(days=offset)
            hr = len(await q.hr_log(day))
            steps = sum(r["steps"] for r in await q.steps(offset) if r["time"].date() == day)
            stress = len(await q.stress(offset))
            hrv = len(await q.hrv(offset))
            row = (day, offset, hr, steps, stress, hrv, "yes" if day in spo2_days else "-",
                   "yes" if day in sleep_days else "-")
            rows.append(row)
            print(f"{day}  {offset:>3}  HR {hr:>4}  steps {steps:>6}  stress {stress:>3}  HRV {hrv:>3}  "
                  f"SpO2 {row[6]:>3}  sleep {row[7]:>3}")
            if hr or steps or stress or hrv:
                empty_streak = 0
            else:
                empty_streak += 1
                if empty_streak >= args.stop_after:
                    break
    print("\nOldest day with data, per type:")
    for i, name in ((2, "heart rate"), (3, "steps"), (4, "stress"), (5, "HRV"), (6, "SpO2"), (7, "sleep")):
        days = [r for r in rows if r[i] and r[i] != "-"]
        print(f"  {name:10}: {f'{days[-1][0]} ({days[-1][1]} days ago), {len(days)} days with data' if days else 'none'}")


TABLES = {
    "heart_rate": "time TEXT PRIMARY KEY, bpm INTEGER",
    "steps": "time TEXT PRIMARY KEY, steps INTEGER, kcal REAL, distance_m INTEGER",
    "stress": "time TEXT PRIMARY KEY, value INTEGER",
    "hrv": "time TEXT PRIMARY KEY, ms INTEGER",
    "spo2": "time TEXT PRIMARY KEY, pct INTEGER",
    "temperature": "time TEXT PRIMARY KEY, celsius REAL",
    "sleep_sessions": "start TEXT PRIMARY KEY, end TEXT, light INTEGER, deep INTEGER, rem INTEGER, awake INTEGER",
    "sleep_stages": "start TEXT PRIMARY KEY, end TEXT, stage TEXT",
    "device": "key TEXT PRIMARY KEY, value TEXT",
}


def ts(t: datetime) -> str:
    return t.isoformat(sep=" ", timespec="minutes")


async def cmd_export(args):
    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    if not args.record:  # keep the raw packets of the latest sync for debugging parsers
        args.record = str(out / "raw.log")
        Path(args.record).write_text("")
    db = sqlite3.connect(out / "qore.db")
    for table, cols in TABLES.items():
        db.execute(f"CREATE TABLE IF NOT EXISTS {table} ({cols})")

    def put(table: str, rows):
        rows = list(rows)
        if rows:
            db.executemany(f"INSERT OR REPLACE INTO {table} VALUES ({', '.join('?' * len(rows[0]))})", rows)
            db.commit()
        return len(rows)

    async def step(label: str, coro):
        """Run one fetch; a feature the band lacks or a timeout shouldn't abort the whole sync."""
        try:
            return await coro
        except (asyncio.TimeoutError, RuntimeError) as e:
            print(f"{label}: skipped ({e or 'timeout'})")
            return None

    device = {}
    async with open_band(args, await resolve_target(args)) as q:
        # Like the Pebble app, set the clock first: the band files every reading under its own
        # date, so a clock that has drifted or jumped would silently misdate the whole export.
        await step("set time", sync_clock(q, args))
        device.update(await q.device_info())
        batt = await step("battery", q.battery())
        if batt:
            device["battery"], device["charging"] = batt[0], int(batt[1])
        totals = await step("today totals", q.today_totals())
        for k, v in (totals or {}).items():
            device[f"today_{k}"] = v
        alarms = await step("alarms", q.alarms())
        if alarms is not None:
            device["alarms"] = json.dumps(alarms)
        goals = await step("goals", q.goals())
        for k, v in (goals or {}).items():
            device[f"goal_{k}"] = v
        for k, v in (await ensure_auto_measure(q)).items():
            device[f"auto_{k}"] = "unsupported" if v is None else int(v)

        for offset in range(args.days):
            day = date.today() - timedelta(days=offset)
            hr = put("heart_rate", [(ts(t), bpm) for t, bpm in await q.hr_log(day)])
            step_rows = await q.steps(offset)
            put("steps", [(ts(r["time"]), r["steps"], r["kcal"], r["distance_m"]) for r in step_rows])
            stress = put("stress", [(ts(t), v) for t, v in (await step("stress", q.stress(offset))) or []])
            hrv = put("hrv", [(ts(t), v) for t, v in (await step("hrv", q.hrv(offset))) or []])
            print(f"{day}: {hr} HR, {sum(r['steps'] for r in step_rows)} steps, "
                  f"{stress} stress, {hrv} HRV readings")

        sessions = await step("sleep", q.sleep()) or []
        put("sleep_sessions", [(ts(s["start"]), ts(s["end"]), s["light"], s["deep"], s["rem"], s["awake"])
                               for s in sessions])
        put("sleep_stages", [(ts(a), ts(b), stage) for s in sessions for a, b, stage in s["stages"]])
        print(f"sleep: {len(sessions)} nights")
        print(f"spo2: {put('spo2', [(ts(t), v) for t, v in (await step('spo2', q.spo2())) or []])} readings")
        print(f"temperature: {put('temperature', [(ts(t), v) for t, v in (await step('temperature', q.temperature())) or []])} readings")

    device["synced_at"] = datetime.now().isoformat(timespec="seconds")
    put("device", [(k, str(v)) for k, v in device.items()])

    # Full CSVs regenerated from the DB, so repeated exports accumulate history
    for table in TABLES:
        cur = db.execute(f"SELECT * FROM {table} ORDER BY 1")
        with (out / f"{table}.csv").open("w", newline="") as f:
            w = csv.writer(f)
            w.writerow([c[0] for c in cur.description])
            w.writerows(cur)
    db.close()
    print(f"\nSaved to {out}/ (qore.db + one CSV per table)")


# --- CLI ---------------------------------------------------------------------
def main():
    p = argparse.ArgumentParser(description="Pebble Qore band BLE client")
    p.add_argument("--address", help="band address (macOS UUID); default: find by name")
    p.add_argument("--name", default=DEFAULT_NAME, help=f"name prefix to scan for (default '{DEFAULT_NAME}')")
    p.add_argument("--band-utc", action="store_true",
                   help="treat the band's clock as UTC (use if times look shifted by your UTC offset)")
    p.add_argument("--record", metavar="FILE", help="append every raw packet to FILE")
    p.add_argument("-v", "--verbose", action="store_true", help="print raw packets")
    sub = p.add_subparsers(dest="cmd", required=True)

    sub.add_parser("scan", help="list nearby BLE devices")
    sub.add_parser("info", help="firmware, battery, HR logging settings")

    s = sub.add_parser("hr-log", help="5-minute heart rate history")
    s.add_argument("--days", type=int, default=1)

    s = sub.add_parser("steps", help="15-minute step history")
    s.add_argument("--days", type=int, default=1)

    for name in ("live-hr", "live-spo2", "measure"):
        s = sub.add_parser(name, help="on-demand measurement" if name == "measure" else
                           f"trigger a live {'heart rate' if name == 'live-hr' else 'SpO2'} measurement")
        if name == "measure":
            s.add_argument("kind", choices=sorted(MEASURE))
        s.add_argument("--count", type=int, default=10, help="stop after this many valid readings")
        s.add_argument("--seconds", type=int, default=60, help="give up after this long")
    sub.add_parser("stream", help="JSON live session for the dashboard (commands on stdin)")
    sub.add_parser("totals", help="today's totals: steps, running steps, kcal, distance, active minutes")
    sub.add_parser("alarms", help="list the alarms stored on the band")

    for name in ("stress", "hrv"):
        s = sub.add_parser(name, help=f"30-minute {name} history")
        s.add_argument("--days", type=int, default=1)
    sub.add_parser("sleep", help="sleep sessions with light/deep/REM/awake stages")
    sub.add_parser("spo2", help="hourly SpO2 history")
    sub.add_parser("temp", help="temperature history (if the band records it)")
    sub.add_parser("goals", help="step / calorie / distance goals")
    s = sub.add_parser("auto-measure", help="show automatic measurement toggles, optionally enable some")
    s.add_argument("--enable", help="comma list of hr,stress,hrv,spo2,temp")

    s = sub.add_parser("probe", help="walk back day by day to see how much history the band keeps")
    s.add_argument("--max-days", type=int, default=31)
    s.add_argument("--stop-after", type=int, default=3, help="stop after this many empty days in a row")

    s = sub.add_parser("export", help="dump all history to SQLite and CSV (enables auto-measurement if off)")
    s.add_argument("--days", type=int, default=7)
    s.add_argument("--out", default="qore_data")

    s = sub.add_parser("listen", help="print everything the band sends")
    s.add_argument("--seconds", type=int, default=60)

    s = sub.add_parser("raw", help="send a raw packet (careful: unknown commands can reset the band)")
    s.add_argument("hex", help="hex bytes, e.g. '03' or '15 00 00 00 00'")
    s.add_argument("--v2", action="store_true", help="send as-is on the v2 (big data) channel")
    s.add_argument("--wait", type=float, default=5, help="seconds to listen for responses")

    s = sub.add_parser("bigdata", help="request a v2 big-data history, save raw packets")
    s.add_argument("kind", choices=sorted(BIGDATA_REQUESTS))
    s.add_argument("--wait", type=float, default=5, help="stop after this many silent seconds")
    s.add_argument("--out", help="output file")

    sub.add_parser("caps", help="ask the band which features it supports (also syncs its clock)")
    sub.add_parser("set-time", help="set the band clock to now (the Pebble app normally does this)")

    args = p.parse_args()
    handlers = {
        "scan": cmd_scan,
        "info": cmd_info,
        "hr-log": cmd_hr_log,
        "steps": cmd_steps,
        "live-hr": lambda a: cmd_measure(a, "hr"),
        "live-spo2": lambda a: cmd_measure(a, "spo2"),
        "measure": cmd_measure,
        "stream": cmd_stream,
        "totals": cmd_totals,
        "alarms": cmd_alarms,
        "stress": lambda a: cmd_half_hour(a, "stress"),
        "hrv": lambda a: cmd_half_hour(a, "hrv"),
        "sleep": cmd_sleep,
        "spo2": cmd_spo2,
        "temp": cmd_temp,
        "goals": cmd_goals,
        "auto-measure": cmd_auto_measure,
        "probe": cmd_probe,
        "export": cmd_export,
        "listen": cmd_listen,
        "raw": cmd_raw,
        "bigdata": cmd_bigdata,
        "caps": cmd_caps,
        "set-time": cmd_set_time,
    }
    try:
        asyncio.run(handlers[args.cmd](args))
    except KeyboardInterrupt:
        print("\nstopped", file=sys.stderr)


if __name__ == "__main__":
    main()