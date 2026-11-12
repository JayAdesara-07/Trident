"""
bench_client.py  -  Headless benchmark client
===============================================
Connects to a C chat server (fork / thread / select), measures
end-to-end message delivery time by embedding a send-timestamp
and sequence number inside every BROADCAST message.

Delivery-time measurement protocol
------------------------------------
Sender encodes:
    BROADCAST __BENCH__<seq>__<send_epoch_ms>__ payload

Receiver (any other client) matches this pattern and computes:
    delivery_ms = receive_epoch_ms - send_epoch_ms

Both sender and receiver sides run in the same process; the sender
writes to _pending{seq: send_time} and the receiver thread
resolves them when the fan-out arrives back.
"""

import socket
import threading
import time
import re
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
from protocol import (
    DISCOVERY_IP, DISCOVERY_PORT,
    CHAT_PORT_FORK, CHAT_PORT_THREAD, CHAT_PORT_SELECT,
    read_line, send_line,
)

_BENCH_RE = re.compile(r'__BENCH__(\d+)__([0-9.]+)__')

SERVER_PORTS = {
    "fork":   CHAT_PORT_FORK,
    "thread": CHAT_PORT_THREAD,
    "select": CHAT_PORT_SELECT,
}


def _register_user(username: str, password: str, port: int) -> bool:
    """Register with the discovery server. Returns True on OK."""
    s = socket.socket()
    try:
        s.settimeout(5)
        s.connect((DISCOVERY_IP, DISCOVERY_PORT))
        send_line(s, f"REGISTER {username} {password} {port}")
        resp = read_line(s)
        return resp.startswith("OK")
    except Exception:
        return False
    finally:
        s.close()


class BenchClient:
    """
    One headless client.  Call setup() then start_sending() / stop().

    Attributes (read after stop()):
        delivery_times   - list of floats (milliseconds), one per resolved msg
        messages_sent    - int
        messages_recv    - int  (bench-tagged messages observed)
        errors           - int  (FAIL lines from server)
        connected        - bool
    """

    def __init__(self, server_type: str, username: str, password: str,
                 disc_port: int = 5000):
        self.server_type = server_type
        self.username    = username
        self.password    = password
        self.disc_port   = disc_port
        self.port        = SERVER_PORTS[server_type]

        self._sock       = None
        self._running    = False
        self._recv_thr   = None
        self._lock       = threading.Lock()

        # pending {seq_str: send_time_ms}
        self._pending    = {}
        self._seq        = 0

        # results
        self.delivery_times = []
        self.messages_sent  = 0
        self.messages_recv  = 0
        self.errors         = 0
        self.connected      = False

    # ── lifecycle ────────────────────────────────────────────────────────────

    def setup(self) -> bool:
        """Register with discovery, connect, login. Returns True on success."""
        _register_user(self.username, self.password, self.disc_port)

        self._sock = socket.socket()
        self._sock.settimeout(10)
        try:
            self._sock.connect((DISCOVERY_IP, self.port))
            send_line(self._sock, f"LOGIN {self.username} {self.password}")
            resp = read_line(self._sock)
            if not resp.startswith("OK"):
                return False
        except Exception:
            return False

        self._sock.settimeout(None)   # blocking, no timeout in recv loop
        self._running = True
        self._recv_thr = threading.Thread(
            target=self._receiver, daemon=True,
            name=f"recv-{self.username}")
        self._recv_thr.start()
        self.connected = True
        return True

    def send_broadcast(self, payload: str = "x"):
        """Send one timed BROADCAST. The receiver thread resolves it."""
        with self._lock:
            seq       = self._seq
            self._seq += 1
            send_ms   = time.perf_counter() * 1000
            self._pending[str(seq)] = send_ms

        tagged = f'__BENCH__{seq}__{send_ms:.3f}__ {payload}'
        try:
            send_line(self._sock, f"BROADCAST {tagged}")
            self.messages_sent += 1
        except Exception:
            with self._lock:
                self._pending.pop(str(seq), None)

    def stop(self):
        """Gracefully disconnect."""
        self._running = False
        try:
            send_line(self._sock, "QUIT")
        except Exception:
            pass
        try:
            self._sock.close()
        except Exception:
            pass
        if self._recv_thr:
            self._recv_thr.join(timeout=3)
        self.connected = False

    # ── receive thread ───────────────────────────────────────────────────────

    def _receiver(self):
        while self._running:
            try:
                line = read_line(self._sock)
            except Exception:
                break

            if line.startswith("MSG [broadcast]"):
                self.messages_recv += 1
                m = _BENCH_RE.search(line)
                if m:
                    seq     = m.group(1)
                    sent_ms = float(m.group(2))
                    recv_ms = time.perf_counter() * 1000
                    dt      = recv_ms - sent_ms
                    if dt >= 0:
                        self.delivery_times.append(dt)
                    with self._lock:
                        self._pending.pop(seq, None)

            elif line.startswith("FAIL"):
                self.errors += 1
