#!/usr/bin/env bash
set -euo pipefail

WORKSPACE_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CLIENT_BIN="${UNEARTH_BIN:-${WORKSPACE_ROOT}/target/release/unearth}"
DAEMON_BIN="${FSXD_BIN:-${WORKSPACE_ROOT}/target/release/fsxd}"
BASE="$(mktemp -d "${TMPDIR:-/tmp}/unearth-exclusions.XXXXXX")"
ROOT="$BASE/root"
XDG_CACHE_HOME="$BASE/cache"
XDG_CONFIG_HOME="$BASE/config"
LOG="$BASE/fsxd.log"
PID=""

cleanup() {
	if [[ -n "$PID" ]] && kill -0 "$PID" 2>/dev/null; then
		kill -TERM "$PID" 2>/dev/null || true
		wait "$PID" 2>/dev/null || true
	fi
	rm -rf -- "$BASE"
}
trap cleanup EXIT

if [[ ! -x "$CLIENT_BIN" || ! -x "$DAEMON_BIN" ]]; then
	cargo build --locked --release --manifest-path "$WORKSPACE_ROOT/Cargo.toml" -p unearth -p fsxd
fi

mkdir -p "$ROOT/a/opaque" "$ROOT/a/keep" "$ROOT/keep" "$ROOT/.git" "$XDG_CONFIG_HOME/fsx"
printf x >"$ROOT/a/opaque/hidden"
printf x >"$ROOT/a/keep/child"
printf x >"$ROOT/keep/.git"
printf x >"$ROOT/.git/hidden"
printf x >"$ROOT/.git-file"
python3 - "$ROOT" <<'PY'
import os, sys
fd = os.open(os.fsencode(sys.argv[1]) + b"/raw-\xff", os.O_CREAT | os.O_WRONLY, 0o600)
os.close(fd)
PY
printf 'name\t.git\npath\t%s/a/opaque\nregex\t^skip-[0-9]+$\n' "$ROOT" >"$XDG_CONFIG_HOME/fsx/excludes.tsv"

export XDG_CACHE_HOME XDG_CONFIG_HOME
"$DAEMON_BIN" --threads 1 "$ROOT" >"$LOG" 2>&1 &
PID=$!

for _ in $(seq 1 400); do
	status="$("$CLIENT_BIN" --watch-status 2>/dev/null || true)"
	if printf '%s\n' "$status" | grep -F "$ROOT" | grep -Fq $'\trunning\t'; then
		break
	fi
	if ! kill -0 "$PID" 2>/dev/null; then
		cat "$LOG" >&2
		exit 1
	fi
	sleep 0.025
done
printf '%s\n' "$status" | grep -F "$ROOT" | grep -Fq $'\trunning\t' || {
	cat "$LOG" >&2
	echo "fsxd did not become ready" >&2
	exit 1
}
backend="$(printf '%s\n' "$status" | grep -F "$ROOT" | head -n 1 | cut -f2)"
if [[ "$backend" == "inotify" ]]; then
	watch_count="$(awk '/^inotify wd:/ { count++ } END { print count + 0 }' /proc/"$PID"/fdinfo/* 2>/dev/null)"
	[[ "$watch_count" -eq 4 ]] || {
		echo "expected four inotify watches (root, a, a/keep, keep), found $watch_count" >&2
		exit 1
	}
fi

"$CLIENT_BIN" --hidden --index-binary '*' "$ROOT" >"$BASE/results.bin"
python3 - "$BASE/results.bin" "$ROOT" <<'PY'
import pathlib, struct, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
root = sys.argv[2].encode()
paths = []
offset = 0
while offset < len(data):
    if len(data) - offset < 4:
        raise AssertionError("truncated path length")
    size = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    if len(data) - offset < size:
        raise AssertionError("truncated path bytes")
    paths.append(data[offset:offset + size])
    offset += size
assert root + b"/a/keep/child" in paths, paths
assert root + b"/keep/.git" in paths, paths  # a .git file remains included
assert root + b"/raw-\xff" in paths, paths
assert root + b"/a/" in paths, paths  # directories retain the existing slash marker
assert not any(path.startswith(root + b"/a/opaque") for path in paths), paths
assert not any(path.startswith(root + b"/.git/") for path in paths), paths
PY

"$CLIENT_BIN" --require-live-index --hidden --index-binary '*' "$ROOT" >"$BASE/strict-live.bin"
cmp "$BASE/results.bin" "$BASE/strict-live.bin"
"$CLIENT_BIN" --require-live-index --limit 1 --hidden --index-binary '*' "$ROOT" \
	>"$BASE/strict-limited.bin"
python3 - "$BASE/strict-limited.bin" <<'PY'
import pathlib, struct, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
assert len(data) >= 4
size = struct.unpack_from("<I", data)[0]
assert len(data) == 4 + size, "strict query ignored --limit 1"
PY
"$CLIENT_BIN" --require-live-index --hidden --full --lossless-paths --index-binary '*' "$ROOT" \
	>"$BASE/strict-formatted.bin"
[[ -s "$BASE/strict-formatted.bin" ]]
"$CLIENT_BIN" --require-live-index --hidden --index-binary 'no-such-picker-entry-xyz' "$ROOT" >"$BASE/empty.bin"
[[ ! -s "$BASE/empty.bin" ]]

DB="$XDG_CACHE_HOME/fsx/index/fsx.db"
python3 - "$DB" "$ROOT" <<'PY'
import sqlite3, sys
db, root = sys.argv[1:]
con = sqlite3.connect(db)
paths = [row[0] for row in con.execute(
    "SELECT d.path || '/' || s.value FROM entries e JOIN dirs d ON e.dir_id=d.id "
    "JOIN strings s ON e.name_id=s.id"
)]
assert root + "/a/keep/child" in paths, paths
assert root + "/keep/.git" in paths, paths
assert not any(path.startswith(root + "/a/opaque/") for path in paths), paths
assert not any(path.startswith(root + "/.git/") for path in paths), paths
PY

wait_for_record() {
	local expected="$1" should_exist="$2"
	for _ in $(seq 1 400); do
		"$CLIENT_BIN" --hidden --index-binary '*' "$ROOT" >"$BASE/poll.bin" 2>/dev/null || true
		if python3 - "$BASE/poll.bin" "$expected" "$should_exist" 2>/dev/null <<'PY'
import pathlib, struct, sys
data = pathlib.Path(sys.argv[1]).read_bytes()
needle = sys.argv[2].encode()
found = False
offset = 0
while offset < len(data):
    size = struct.unpack_from("<I", data, offset)[0]
    offset += 4
    found |= data[offset:offset + size] == needle
    offset += size
assert found == (sys.argv[3] == "yes")
PY
		then
			return
		fi
		sleep 0.025
	done
	echo "timed out waiting for indexed record $expected ($should_exist)" >&2
	exit 1
}

wait_for_watch_count() {
	local expected="$1"
	[[ "$backend" == "inotify" ]] || return 0
	for _ in $(seq 1 400); do
		local count
		count="$(awk '/^inotify wd:/ { count++ } END { print count + 0 }' /proc/"$PID"/fdinfo/* 2>/dev/null)"
		[[ "$count" -eq "$expected" ]] && return
		sleep 0.025
	done
	echo "timed out waiting for $expected inotify watches" >&2
	exit 1
}

wait_for_clean_root() {
	local stable=0
	for _ in $(seq 1 400); do
		local current
		current="$("$CLIENT_BIN" --watch-status 2>/dev/null || true)"
		if printf '%s\n' "$current" | grep -F "$ROOT" | grep -Fq $'dirty=0\tonline=1'; then
			((stable += 1))
			[[ "$stable" -ge 40 ]] && return
		else
			stable=0
		fi
		sleep 0.025
	done
	echo "watch root did not remain clean after move reconciliation" >&2
	exit 1
}

wait_for_dirty_root() {
	for _ in $(seq 1 10); do
		local current
		current="$("$CLIENT_BIN" --watch-status 2>/dev/null || true)"
		if ! printf '%s\n' "$current" | grep -F "$ROOT" | grep -Fq $'dirty=1\tonline=1'; then
			echo "watch root did not remain dirty while checking strict query behavior" >&2
			exit 1
		fi
		sleep 0.025
	done
}

# Exact-path exclusions must be reevaluated after a parent rename. The newly
# included subtree must also gain inotify coverage after the move.
mv "$ROOT/a" "$ROOT/b"
wait_for_record "$ROOT/b/opaque/hidden" yes
wait_for_watch_count 5
wait_for_clean_root
touch "$ROOT/b/opaque/new-file"
wait_for_record "$ROOT/b/opaque/new-file" yes
mv "$ROOT/b/opaque" "$ROOT/.git/opaque"
wait_for_record "$ROOT/.git/opaque/hidden" no
wait_for_watch_count 4
wait_for_clean_root
mv "$ROOT/.git/opaque" "$ROOT/b/opaque"
wait_for_record "$ROOT/b/opaque/hidden" yes
wait_for_watch_count 5
mv "$ROOT/b" "$ROOT/a"
wait_for_record "$ROOT/a/opaque/hidden" no
wait_for_watch_count 4
mv "$ROOT/a" "$ROOT/b"
wait_for_record "$ROOT/b/opaque/hidden" yes
wait_for_watch_count 5
wait_for_clean_root

# Dirty roots are unavailable to strict callers even while fsxd's socket stays
# online. Restore the fixture before the normal shutdown path.
python3 - "$DB" "$ROOT" <<'PY'
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
updated = con.execute("UPDATE watch_state SET dirty=1, status='recovering' WHERE root=?", (sys.argv[2],)).rowcount
assert updated == 1, (updated, list(con.execute("SELECT root, status, dirty FROM watch_state")))
con.commit()
state = con.execute("SELECT status, dirty FROM watch_state WHERE root=?", (sys.argv[2],)).fetchone()
assert state == ("recovering", 1), state
PY
wait_for_dirty_root
if "$CLIENT_BIN" --require-live-index --hidden --index-binary '*' "$ROOT" >"$BASE/dirty.bin" 2>"$BASE/dirty.err"; then
	echo "strict live query unexpectedly succeeded for a dirty root" >&2
	exit 1
fi
[[ ! -s "$BASE/dirty.bin" ]]
python3 - "$DB" "$ROOT" <<'PY'
import sqlite3, sys
con = sqlite3.connect(sys.argv[1])
con.execute("UPDATE watch_state SET dirty=0, status='running' WHERE root=?", (sys.argv[2],))
con.commit()
PY

# Strict mode must not create a local database or emit stale results when no
# live socket is available.
kill -TERM "$PID"
wait "$PID" || true
PID=""
if "$CLIENT_BIN" --require-live-index --hidden --index-binary '*' "$ROOT" >"$BASE/stale.bin" 2>"$BASE/stale.err"; then
	echo "strict live query unexpectedly succeeded against a stopped daemon" >&2
	exit 1
fi
[[ ! -s "$BASE/stale.bin" ]]
rm -rf "$XDG_CACHE_HOME"
if "$CLIENT_BIN" --require-live-index --hidden --index-binary '*' "$ROOT" >"$BASE/unavailable.bin" 2>"$BASE/unavailable.err"; then
	echo "strict live query unexpectedly succeeded without fsxd" >&2
	exit 1
fi
[[ ! -s "$BASE/unavailable.bin" ]]
[[ ! -e "$DB" ]]

STALE_CACHE="$BASE/stale-cache"
STALE_SOCKET="$STALE_CACHE/fsx/index/fsxd.sock"
mkdir -p "$(dirname "$STALE_SOCKET")"
python3 - "$STALE_SOCKET" <<'PY'
import socket, sys
sock = socket.socket(socket.AF_UNIX)
sock.bind(sys.argv[1])
sock.close()
PY
if XDG_CACHE_HOME="$STALE_CACHE" "$CLIENT_BIN" --require-live-index --index-binary '*' "$ROOT" \
	>"$BASE/stale-socket.bin" 2>"$BASE/stale-socket.err"; then
	echo "strict query unexpectedly succeeded through a stale socket" >&2
	exit 1
fi
[[ ! -s "$BASE/stale-socket.bin" ]]
[[ ! -e "$STALE_CACHE/fsx/index/fsx.db" ]]

BAD_CACHE="$BASE/bad-cache"
BAD_CONFIG="$BASE/bad-config"
mkdir -p "$BAD_CONFIG/fsx"
printf 'regex\t[\n' >"$BAD_CONFIG/fsx/excludes.tsv"
if XDG_CACHE_HOME="$BAD_CACHE" XDG_CONFIG_HOME="$BAD_CONFIG" "$DAEMON_BIN" "$ROOT" >"$BASE/bad.log" 2>&1; then
	echo "fsxd unexpectedly accepted a malformed exclusion regex" >&2
	exit 1
fi
[[ ! -e "$BAD_CACHE/fsx/index/fsx.db" ]]

# A missing configuration file is equivalent to an empty rule set.
EMPTY_CACHE="$BASE/empty-cache"
EMPTY_CONFIG="$BASE/empty-config"
mkdir -p "$EMPTY_CONFIG"
XDG_CACHE_HOME="$EMPTY_CACHE" XDG_CONFIG_HOME="$EMPTY_CONFIG" "$CLIENT_BIN" \
	--index-refresh "$ROOT" >/dev/null
