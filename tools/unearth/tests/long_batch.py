#!/usr/bin/env python3
"""Live regression tests: python3 tools/unearth/tests/long_batch.py [case]."""

import argparse
from contextlib import closing, contextmanager
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import sys
import tempfile
import time


BUILD = Path(__file__).resolve().parents[3] / "target/debug"
FSXD = Path(os.environ.get("FSXD_BIN", BUILD / "fsxd")).resolve()
UNEARTH = Path(os.environ.get("UNEARTH_BIN", BUILD / "unearth")).resolve()


class Fixture:
    def __init__(self, base, delay="30000"):
        self.root = base / "root"
        self.root.mkdir()
        self.anchor = self.root / "existing-marker.txt"
        self.anchor.touch()
        self.env = {k: v for k, v in os.environ.items()
                    if not k.startswith(("FSX_", "UNEARTH_"))}
        self.env.update(XDG_CACHE_HOME=str(base / "cache"), XDG_CONFIG_HOME=str(base / "config"),
                        UNEARTH_WATCH_BATCH_MS=delay, UNEARTH_WATCH_RECONCILE_SECS="0")
        self.database = base / "cache/fsx/index/fsx.db"

    def query(self, path):
        return subprocess.run(
            [UNEARTH, "--require-live-index", "--index-binary", "--hidden", path.name, self.root],
            env=self.env, capture_output=True, timeout=5,
        )

    def contains(self, path):
        result = self.query(path)
        assert result.returncode == 0, result.stderr.decode(errors="replace")
        raw = os.fsencode(path)
        assert result.stdout in (b"", len(raw).to_bytes(4, "little") + raw), result.stdout
        return result.stdout != b""

    def status(self, *, starting=False):
        result = subprocess.run([UNEARTH, "--watch-status-json"], env=self.env,
                                capture_output=True, timeout=5)
        if starting and result.returncode:
            return {}
        result.check_returncode()
        return next((row for row in json.loads(result.stdout).get("watchers", [])
                     if row["root"] == str(self.root)), {})

    @contextmanager
    def daemon(self):
        with tempfile.TemporaryFile() as log, subprocess.Popen(
            [FSXD, "--threads", "1", self.root], env=self.env, stdout=log, stderr=log,
        ) as proc:
            try:
                until = time.monotonic() + 10
                while time.monotonic() < until and proc.poll() is None:
                    row = self.status(starting=True)
                    if (row.get("pid") == proc.pid and row.get("online") and not row.get("dirty")
                            and self.query(self.anchor).stdout):
                        break
                    time.sleep(.05)
                else:
                    raise AssertionError("test daemon did not become ready")
                yield proc
            except BaseException:
                log.seek(0)
                print(log.read().decode(errors="replace"), file=sys.stderr)
                raise
            finally:
                if proc.poll() is None:
                    proc.kill()
                    proc.wait(timeout=5)


def heartbeat(fixture):
    with fixture.daemon() as proc:
        started = time.monotonic()
        pending = fixture.root / "new-marker.txt"
        pending.touch()
        heartbeat_before = fixture.status()["heartbeat"]
        # Poll both paths across the 15-second liveness expiry and up to the
        # batch deadline. A heartbeat transaction must not publish file events.
        while time.monotonic() - started < 36:
            assert fixture.contains(fixture.anchor), "an existing indexed path became unavailable"
            if fixture.contains(pending):
                elapsed = time.monotonic() - started
                assert elapsed >= 28, f"published the 30s batch early at {elapsed:.2f}s"
                assert fixture.status()["heartbeat"] > heartbeat_before
                assert fixture.status()["config"]["effective"]["batch_delay_ms"] == 30_000
                break
            assert proc.poll() is None, "watcher exited during healthy batching"
            time.sleep(.2)
        else:
            raise AssertionError("a healthy batch never published its new file")
        print(f"heartbeat: existing search stayed available; new file visible after {elapsed:.2f}s")


def shutdown(fixture):
    with fixture.daemon() as proc:
        pending = fixture.root / "shutdown-marker.txt"
        pending.touch()
        # Let the backend enqueue the event but do not wait out the batch.
        time.sleep(.5)
        assert not fixture.contains(pending), "file was already committed before SIGTERM"
        started = time.monotonic()
        proc.terminate()
        assert proc.wait(timeout=4) == 0, "normal shutdown failed"
        elapsed = time.monotonic() - started
        with closing(sqlite3.connect(fixture.database.as_uri() + "?mode=ro", uri=True)) as database:
            stored = database.execute(
                "SELECT COUNT(*) FROM entries e JOIN strings s ON s.id=e.name_id "
                "WHERE s.value=?", (pending.name,),
            ).fetchone()
        assert stored == (1,), "graceful stop did not flush its pending create"
        offline = fixture.query(pending)
        assert offline.returncode != 0 and not offline.stdout, "offline search did not fail closed"
        print(f"shutdown: flushed a pending create and stopped in {elapsed:.2f}s")
    fixture.anchor.unlink()
    (fixture.root / "offline-marker.txt").touch()
    fixture.anchor = fixture.root / "offline-marker.txt"
    with fixture.daemon():
        assert fixture.contains(pending)
        assert not fixture.contains(fixture.root / "existing-marker.txt")
    print("shutdown: restart caught changes made while stopped")


def overflow(fixture):
    flood = fixture.root / "prewatched"
    flood.mkdir()
    with fixture.daemon() as proc:
        if fixture.status()["backend"] != "inotify":
            print("overflow: skipped (requires inotify)")
            return
        limit = int(Path("/proc/sys/fs/inotify/max_queued_events").read_text())
        # SIGSTOP makes the kernel queue overflow before the consumer can read
        # it. The final file is beyond the queue limit, so only recovery sees it.
        proc.send_signal(signal.SIGSTOP)
        until = time.monotonic() + 3
        while "\nState:\tT" not in Path(f"/proc/{proc.pid}/status").read_text():
            assert time.monotonic() < until, "watcher did not stop"
            time.sleep(.01)
        for i in range(limit + 1024):
            (flood / f"overflow-{i}.txt").touch()
        marker = flood / "only-after-overflow.txt"
        marker.touch()
        started = time.monotonic()
        proc.send_signal(signal.SIGCONT)
        while time.monotonic() - started < 15:
            row = fixture.status()
            if (any(event["kind"] == "overflow" for event in row.get("history", []))
                    and fixture.query(marker).stdout):
                print(f"overflow: recovered after {time.monotonic() - started:.2f}s")
                return
            time.sleep(.1)
        raise AssertionError("overflow recovery waited behind the normal 30s batch timer")


def invalid_config(base):
    for i, value in enumerate(("0", "30001", "garbage")):
        case = base / str(i)
        case.mkdir()
        fixture = Fixture(case, value)
        with subprocess.Popen([FSXD, fixture.root], env=fixture.env,
                              stdout=subprocess.DEVNULL, stderr=subprocess.PIPE) as proc:
            try:
                _, stderr = proc.communicate(timeout=2)
            except subprocess.TimeoutExpired:
                proc.kill()
                proc.communicate()
                raise AssertionError(f"watcher accepted invalid batch delay {value!r}") from None
        assert proc.returncode != 0 and b"UNEARTH_WATCH_BATCH_MS" in stderr, stderr
        assert not fixture.database.exists(), "invalid config modified the database"
    print("config: invalid values rejected before opening the index")


def main():
    cases = {"heartbeat": heartbeat, "shutdown": shutdown, "overflow": overflow,
             "config": invalid_config}
    args = argparse.ArgumentParser()
    args.add_argument("case", nargs="?", choices=cases)
    choice = args.parse_args().case
    for name, test in cases.items():
        if choice is None or name == choice:
            with tempfile.TemporaryDirectory(prefix="fsx-long-") as temp:
                base = Path(temp).resolve()
                test(base if name == "config" else Fixture(base))


if __name__ == "__main__":
    main()
