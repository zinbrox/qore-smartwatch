#!/usr/bin/env python3
"""
dashboard.py - local web dashboard for data exported by watch.py.

Usage:
    python dashboard.py                 # http://127.0.0.1:8765
    python dashboard.py --open --port 9000 --data data

The page reads data/qore.db fresh on every load. The "Sync" button runs
`watch.py export` in a subprocess, and "Live" runs `watch.py stream` (real-time heart rate
and on-demand measurements), so the band must be free (Pebble app closed).
"""
from __future__ import annotations

import argparse
import json
import queue
import sqlite3
import subprocess
import sys
import threading
import time
import webbrowser
from datetime import datetime
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

HERE = Path(__file__).resolve().parent
PAGE = HERE / "dashboard.html"


def band_python() -> str:
    """An interpreter that can talk to the band (has bleak): this one, else the project's venv."""
    try:
        import bleak  # noqa: F401
        return sys.executable
    except ImportError:
        pass
    for venv in (".venv312", ".venv"):
        py = HERE / venv / "bin" / "python"
        if py.exists() and subprocess.run([py, "-c", "import bleak"], capture_output=True).returncode == 0:
            return str(py)
    return sys.executable


class LiveSession:
    """
    Wraps a `watch.py stream` subprocess: its JSON events fan out to every connected
    page (Server-Sent Events); measurement requests go to its stdin.
    """

    IDLE_SECONDS = 600  # disconnect from the band when no page has been watching this long

    def __init__(self, cmd_base: list[str]):
        self.cmd_base = cmd_base
        self.lock = threading.Lock()
        self.proc: subprocess.Popen | None = None
        self.clients: set[queue.Queue] = set()
        self.last_seen = time.time()
        self.snapshot: dict = {"state": "off"}
        threading.Thread(target=self._watchdog, daemon=True).start()

    @property
    def running(self) -> bool:
        return self.proc is not None and self.proc.poll() is None

    def start(self) -> bool:
        with self.lock:
            if self.running:
                return False
            self.proc = subprocess.Popen(self.cmd_base + ["stream"], cwd=HERE, stdin=subprocess.PIPE,
                                         stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
            self.snapshot = {"state": "connecting"}
            self.last_seen = time.time()
        threading.Thread(target=self._read, args=(self.proc,), daemon=True).start()
        self._broadcast({"type": "state", "state": "connecting"})
        return True

    def stop(self, wait: float = 10):
        proc = self.proc
        if not proc or proc.poll() is not None:
            return
        try:
            proc.stdin.write("quit\n")
            proc.stdin.flush()
            proc.wait(wait)
        except (OSError, subprocess.TimeoutExpired):
            proc.terminate()
            try:
                proc.wait(5)
            except subprocess.TimeoutExpired:
                proc.kill()

    def measure(self, kind: str) -> bool:
        with self.lock:
            if not self.running or self.snapshot.get("state") != "on" or self.snapshot.get("measuring"):
                return False
            self.snapshot["measuring"] = kind
            self.proc.stdin.write(f"measure {kind}\n")
            self.proc.stdin.flush()
        return True

    def _read(self, proc: subprocess.Popen):
        for line in proc.stdout:
            line = line.strip()
            if not line:
                continue
            try:
                ev = json.loads(line)
            except ValueError:
                ev = {"type": "log", "text": line}  # e.g. "Looking for a device named ..."
            self._update(ev)
        proc.wait()
        if proc is self.proc:
            self._update({"type": "state", "state": "off"})

    def _update(self, ev: dict):
        kind = ev.get("type")
        with self.lock:
            snap = self.snapshot
            if kind == "state":
                snap["state"] = ev["state"]
                if ev["state"] == "off":
                    snap.pop("measuring", None)
            elif kind in ("hr", "battery", "totals", "activity", "error", "log"):
                snap[kind] = ev
            elif kind == "measure-start":
                snap["measuring"] = ev["kind"]
            elif kind in ("measure-result", "measure-error"):
                snap.pop("measuring", None)
                snap.setdefault("results", {})[ev["kind"]] = ev
        self._broadcast(ev)

    def _broadcast(self, ev: dict):
        for q in list(self.clients):
            try:
                q.put_nowait(ev)
            except queue.Full:
                pass

    def subscribe(self) -> queue.Queue:
        q: queue.Queue = queue.Queue(maxsize=500)
        self.clients.add(q)
        self.last_seen = time.time()
        return q

    def unsubscribe(self, q: queue.Queue):
        self.clients.discard(q)
        self.last_seen = time.time()

    def status(self) -> dict:
        with self.lock:
            return json.loads(json.dumps(self.snapshot))

    def _watchdog(self):
        while True:
            time.sleep(30)
            if self.running and not self.clients and time.time() - self.last_seen > self.IDLE_SECONDS:
                self.stop()


class SyncJob:
    """One export at a time; output lines are kept so the page can poll them."""

    def __init__(self, cmd_base: list[str], out_dir: Path, live: LiveSession):
        self.cmd_base = cmd_base
        self.out_dir = out_dir
        self.live = live
        self.lock = threading.Lock()
        self.running = False
        self.ok: bool | None = None
        self.log: list[str] = []
        self.finished_at: str | None = None

    def start(self, days: int) -> bool:
        with self.lock:
            if self.running:
                return False
            self.running, self.ok, self.log, self.finished_at = True, None, [], None
        threading.Thread(target=self._run, args=(days,), daemon=True).start()
        return True

    def _run(self, days: int):
        cmd = self.cmd_base + ["export", "--days", str(days), "--out", str(self.out_dir)]
        ok = False
        resume_live = self.live.running  # the band takes one connection at a time
        if resume_live:
            self.log.append("pausing live mode for the sync...")
            self.live.stop()
        try:
            proc = subprocess.Popen(cmd, cwd=HERE, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                                    text=True, bufsize=1)
            for line in proc.stdout:
                self.log.append(line.rstrip())
            ok = proc.wait() == 0
        except Exception as e:
            self.log.append(f"failed to start export: {e}")
        if resume_live:
            self.live.start()
        with self.lock:
            self.running, self.ok = False, ok
            self.finished_at = datetime.now().isoformat(timespec="seconds")

    def status(self) -> dict:
        return {"running": self.running, "ok": self.ok, "log": self.log[-50:],
                "finished_at": self.finished_at}


QUERIES = {
    "heart_rate": "SELECT time, bpm FROM heart_rate ORDER BY time",
    "steps": "SELECT time, steps, kcal, distance_m FROM steps ORDER BY time",
    "stress": "SELECT time, value FROM stress ORDER BY time",
    "hrv": "SELECT time, ms FROM hrv ORDER BY time",
    "spo2": "SELECT time, pct FROM spo2 ORDER BY time",
    "temperature": "SELECT time, celsius FROM temperature ORDER BY time",
    "sleep_sessions": "SELECT start, end, light, deep, rem, awake FROM sleep_sessions ORDER BY start",
    "sleep_stages": "SELECT start, end, stage FROM sleep_stages ORDER BY start",
}


def load_data(db_path: Path) -> dict:
    out = {k: [] for k in QUERIES} | {"device": {}, "synced_at": None}
    if not db_path.exists():
        return out
    out["synced_at"] = datetime.fromtimestamp(db_path.stat().st_mtime).isoformat(timespec="seconds")
    db = sqlite3.connect(f"file:{db_path}?mode=ro", uri=True)
    try:
        for key, sql in QUERIES.items():
            try:
                out[key] = db.execute(sql).fetchall()
            except sqlite3.OperationalError:  # table from an older export format
                pass
        try:
            out["device"] = dict(db.execute("SELECT key, value FROM device").fetchall())
        except sqlite3.OperationalError:
            pass
    finally:
        db.close()
    return out


def make_handler(db_path: Path, job: SyncJob, live: LiveSession):
    class Handler(BaseHTTPRequestHandler):
        def _send(self, code: int, body: bytes, ctype: str):
            self.send_response(code)
            self.send_header("Content-Type", ctype)
            self.send_header("Content-Length", str(len(body)))
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)

        def _json(self, obj, code: int = 200):
            self._send(code, json.dumps(obj).encode(), "application/json")

        def do_GET(self):
            path = urlparse(self.path).path
            if path in ("/", "/index.html"):
                self._send(200, PAGE.read_bytes(), "text/html; charset=utf-8")
            elif path == "/api/data":
                self._json(load_data(db_path))
            elif path == "/api/sync":
                self._json(job.status())
            elif path == "/api/live/status":
                self._json(live.status())
            elif path == "/api/live/stream":
                self._stream()
            else:
                self._send(404, b"not found", "text/plain")

        def _stream(self):
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            q = live.subscribe()
            try:
                self.wfile.write(f"data: {json.dumps({'type': 'snapshot', **live.status()})}\n\n".encode())
                self.wfile.flush()
                while True:
                    try:
                        ev = q.get(timeout=15)
                        self.wfile.write(f"data: {json.dumps(ev)}\n\n".encode())
                    except queue.Empty:
                        self.wfile.write(b": ping\n\n")  # keeps the connection (and idle check) honest
                    self.wfile.flush()
            except (BrokenPipeError, ConnectionResetError):
                pass
            finally:
                live.unsubscribe(q)

        def do_POST(self):
            url = urlparse(self.path)
            query = parse_qs(url.query)
            if url.path == "/api/live/start":
                if job.running:
                    return self._json({"error": "sync in progress"}, 409)
                live.start()
                return self._json(live.status(), 202)
            if url.path == "/api/live/stop":
                threading.Thread(target=live.stop, daemon=True).start()
                return self._json({"state": "stopping"}, 202)
            if url.path == "/api/live/measure":
                kind = query.get("kind", [""])[0]
                if kind not in ("hr", "spo2", "stress", "hrv", "check"):
                    return self._json({"error": "unknown measurement"}, 400)
                return self._json(live.status(), 202 if live.measure(kind) else 409)
            if url.path != "/api/sync":
                return self._send(404, b"not found", "text/plain")
            try:
                days = max(1, min(31, int(query.get("days", ["7"])[0])))
            except ValueError:
                days = 7
            if job.start(days):
                self._json(job.status(), 202)
            else:
                self._json(job.status(), 409)

        def log_message(self, fmt, *args):
            if not self.path.startswith(("/api/sync", "/api/live")):
                sys.stderr.write(f"{self.address_string()} - {fmt % args}\n")

    return Handler


def main():
    p = argparse.ArgumentParser(description="Qore band dashboard")
    p.add_argument("--port", type=int, default=8765)
    p.add_argument("--data", default="data", help="export directory holding qore.db (default 'data')")
    p.add_argument("--open", action="store_true", help="open the dashboard in a browser")
    p.add_argument("--address", help="passed to watch.py for syncing")
    p.add_argument("--band-utc", action="store_true", help="passed to watch.py for syncing")
    args = p.parse_args()

    data_dir = (HERE / args.data).resolve()
    cmd = [band_python(), str(HERE / "watch.py")]
    if args.address:
        cmd += ["--address", args.address]
    if args.band_utc:
        cmd.append("--band-utc")
    live = LiveSession(cmd)
    job = SyncJob(cmd, data_dir, live)

    server = ThreadingHTTPServer(("127.0.0.1", args.port), make_handler(data_dir / "qore.db", job, live))
    server.daemon_threads = True  # open live streams shouldn't block Ctrl+C
    url = f"http://127.0.0.1:{args.port}"
    print(f"Dashboard at {url}  (data: {data_dir})  Ctrl+C to stop", file=sys.stderr)
    if args.open:
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nstopped", file=sys.stderr)
    finally:
        live.stop(wait=5)


if __name__ == "__main__":
    main()
