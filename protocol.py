"""
ChatProtocol  -  Python implementation matching the C chat system
==================================================================
This file mirrors the protocol defined in common.h exactly.

Wire format: plain UTF-8 text, one message per line, terminated by '\n'.
             '\r\n' (telnet style) is also accepted on receive.
             Every send appends exactly one '\n'.

Ports (must match common.h):
    DISCOVERY_PORT   = 9090
    CHAT_PORT_FORK   = 8080
    CHAT_PORT_THREAD = 8081
    CHAT_PORT_SELECT = 8082

════════════════════════════════════════════════════════════════
CLIENT  →  CHAT SERVER  (you send these)
════════════════════════════════════════════════════════════════
  LOGIN <username> <password>
  BROADCAST <message>
  PRIVATE <target_username> <message>
  LIST
  QUIT

════════════════════════════════════════════════════════════════
CHAT SERVER  →  CLIENT  (you receive these)
════════════════════════════════════════════════════════════════
  OK [optional text]
  FAIL <reason>
  MSG [broadcast] <sender>: <text>
  MSG [private] <sender>: <text>
  USERS <user1>,<user2>,...
  SERVER: <system notification text>

════════════════════════════════════════════════════════════════
CLIENT  →  DISCOVERY SERVER
════════════════════════════════════════════════════════════════
  REGISTER <username> <password> <port>
  AUTH     <username> <password>
  LOOKUP   <username>
  LIST
  UNREGISTER <username>

════════════════════════════════════════════════════════════════
DISCOVERY SERVER  →  CLIENT
════════════════════════════════════════════════════════════════
  OK
  FAIL <reason>
  FOUND <ip> <port>
  NOT_FOUND
  <username> <ip> <port>   (one per line, then END)
  END
"""

import socket


# ════════════════════════════════════════════════════════════════
# § 1  Constants  (mirror of common.h #defines)
# ════════════════════════════════════════════════════════════════

DISCOVERY_IP     = "127.0.0.1"
DISCOVERY_PORT   = 9090

CHAT_PORT_FORK   = 8080
CHAT_PORT_THREAD = 8081
CHAT_PORT_SELECT = 8082

BUFFER_SIZE  = 2048
MAX_USERNAME = 64
MAX_PASSWORD = 64
MAX_MSG      = 1400


# ════════════════════════════════════════════════════════════════
# § 2  Low-level I/O  (mirrors read_line / send_line in common.h)
# ════════════════════════════════════════════════════════════════

def read_line(sock: socket.socket) -> str:
    """
    Read one newline-terminated line from *sock*.

    Mirrors common.h  read_line():
      - Reads byte-by-byte until '\n'
      - Strips '\r' (tolerates \r\n from telnet / Windows)
      - Returns the line WITHOUT the trailing newline
      - Raises ConnectionError if the peer closes the connection
    """
    buf = []
    while True:
        byte = sock.recv(1)
        if not byte:
            raise ConnectionError("Server closed the connection")
        ch = byte.decode('utf-8', errors='replace')
        if ch == '\r':
            continue          # skip carriage-return
        if ch == '\n':
            break             # end of line
        buf.append(ch)
    return ''.join(buf)


def send_line(sock: socket.socket, message: str) -> None:
    """
    Send *message* followed by exactly one '\n'.

    Mirrors common.h  send_line():
      Appends '\n' and calls send() so the C server's read_line() sees
      a complete line.
    """
    line = message + '\n'
    sock.sendall(line.encode('utf-8'))


# ════════════════════════════════════════════════════════════════
# § 3  Message Builders  -  CLIENT → CHAT SERVER
# ════════════════════════════════════════════════════════════════

def make_login(username: str, password: str) -> str:
    """
    LOGIN <username> <password>
    First message after connecting. Server replies OK or FAIL.
    """
    return f"LOGIN {username} {password}"


def make_broadcast(message: str) -> str:
    """
    BROADCAST <message>
    Server fans the message out to all other connected clients as:
      MSG [broadcast] <sender>: <message>
    """
    return f"BROADCAST {message}"


def make_private(target_username: str, message: str) -> str:
    """
    PRIVATE <target_username> <message>
    Server delivers only to the named user as:
      MSG [private] <sender>: <message>
    If the target is not online the server sends:
      SERVER: User '<target>' is not online
    """
    return f"PRIVATE {target_username} {message}"


def make_list() -> str:
    """
    LIST
    Server responds with:
      USERS alice,bob,carol
    """
    return "LIST"


def make_quit() -> str:
    """
    QUIT
    Tells the server to clean up and close the connection gracefully.
    """
    return "QUIT"


# ════════════════════════════════════════════════════════════════
# § 4  Message Builders  -  CLIENT → DISCOVERY SERVER
# ════════════════════════════════════════════════════════════════

def make_register(username: str, password: str, port: int) -> str:
    """
    REGISTER <username> <password> <port>
    Register this client in the discovery server's user table.
    Server records (username, password, caller-IP, port).
    Responds: OK  or  FAIL <reason>
    """
    return f"REGISTER {username} {password} {port}"


def make_auth(username: str, password: str) -> str:
    """
    AUTH <username> <password>
    Verify credentials against the discovery server's table.
    Responds: OK  or  FAIL
    (Used internally by chat servers to validate logins.)
    """
    return f"AUTH {username} {password}"


def make_lookup(username: str) -> str:
    """
    LOOKUP <username>
    Ask the discovery server where a user is reachable.
    Responds: FOUND <ip> <port>  or  NOT_FOUND
    """
    return f"LOOKUP {username}"


def make_discovery_list() -> str:
    """
    LIST
    Ask the discovery server for all registered users.
    Responds: one line per user  "<username> <ip> <port>"  then "END"
    """
    return "LIST"


def make_unregister(username: str) -> str:
    """
    UNREGISTER <username>
    Remove this user from the discovery server's table.
    Responds: OK  or  FAIL
    """
    return f"UNREGISTER {username}"


# ════════════════════════════════════════════════════════════════
# § 5  Response Parsers  -  CHAT SERVER → CLIENT
# ════════════════════════════════════════════════════════════════

def parse_server_response(line: str) -> dict:
    """
    Parse one line received from the chat server.

    Returns a dict with key "type" set to one of:
      "ok"        - login succeeded or generic success
      "fail"      - command rejected; "reason" has the detail
      "broadcast" - group message; "sender" and "text" set
      "private"   - direct message; "sender" and "text" set
      "users"     - reply to LIST; "users" is a list of strings
      "notice"    - SERVER: notification; "text" has the message
      "unknown"   - unrecognised line; "raw" has the original text

    Examples
    --------
    >>> parse_server_response("OK Welcome alice!")
    {'type': 'ok', 'message': 'Welcome alice!'}

    >>> parse_server_response("MSG [broadcast] bob: Hello!")
    {'type': 'broadcast', 'sender': 'bob', 'text': 'Hello!'}

    >>> parse_server_response("MSG [private] alice: hey")
    {'type': 'private', 'sender': 'alice', 'text': 'hey'}

    >>> parse_server_response("USERS alice,bob,carol")
    {'type': 'users', 'users': ['alice', 'bob', 'carol']}

    >>> parse_server_response("SERVER: bob joined the chat")
    {'type': 'notice', 'text': 'bob joined the chat'}

    >>> parse_server_response("FAIL User already logged in")
    {'type': 'fail', 'reason': 'User already logged in'}
    """
    # OK [optional message]
    if line.startswith("OK"):
        return {"type": "ok", "message": line[2:].strip()}

    # FAIL <reason>
    if line.startswith("FAIL"):
        return {"type": "fail", "reason": line[4:].strip()}

    # MSG [broadcast] <sender>: <text>
    if line.startswith("MSG [broadcast]"):
        rest = line[len("MSG [broadcast]"):].strip()   # "sender: text"
        if ": " in rest:
            sender, text = rest.split(": ", 1)
        else:
            sender, text = rest, ""
        return {"type": "broadcast", "sender": sender.strip(), "text": text}

    # MSG [private] <sender>: <text>
    if line.startswith("MSG [private]"):
        rest = line[len("MSG [private]"):].strip()
        if ": " in rest:
            sender, text = rest.split(": ", 1)
        else:
            sender, text = rest, ""
        return {"type": "private", "sender": sender.strip(), "text": text}

    # USERS alice,bob,...
    if line.startswith("USERS"):
        user_part = line[5:].strip()
        users = [u.strip() for u in user_part.split(",") if u.strip()]
        return {"type": "users", "users": users}

    # SERVER: <text>
    if line.startswith("SERVER:"):
        return {"type": "notice", "text": line[7:].strip()}

    # Anything else
    return {"type": "unknown", "raw": line}


# ════════════════════════════════════════════════════════════════
# § 6  Response Parsers  -  DISCOVERY SERVER → CLIENT
# ════════════════════════════════════════════════════════════════

def parse_discovery_response(line: str) -> dict:
    """
    Parse one line from the discovery server.

    Returns a dict with key "type" set to one of:
      "ok"        - operation succeeded
      "fail"      - operation failed; "reason" set
      "found"     - LOOKUP hit; "ip" and "port" set
      "not_found" - LOOKUP miss
      "user_entry"- one row from LIST; "username", "ip", "port" set
      "end"       - signals end of LIST stream
      "unknown"   - unrecognised line

    Examples
    --------
    >>> parse_discovery_response("OK")
    {'type': 'ok', 'message': ''}

    >>> parse_discovery_response("FOUND 127.0.0.1 5000")
    {'type': 'found', 'ip': '127.0.0.1', 'port': 5000}

    >>> parse_discovery_response("alice 127.0.0.1 5000")
    {'type': 'user_entry', 'username': 'alice', 'ip': '127.0.0.1', 'port': 5000}
    """
    if line.startswith("OK"):
        return {"type": "ok", "message": line[2:].strip()}

    if line.startswith("FAIL"):
        return {"type": "fail", "reason": line[4:].strip()}

    if line.startswith("FOUND"):
        parts = line.split()          # ['FOUND', '<ip>', '<port>']
        if len(parts) == 3:
            return {"type": "found", "ip": parts[1], "port": int(parts[2])}

    if line == "NOT_FOUND":
        return {"type": "not_found"}

    if line == "END":
        return {"type": "end"}

    # Could be a LIST entry: "<username> <ip> <port>"
    parts = line.split()
    if len(parts) == 3:
        try:
            return {
                "type":     "user_entry",
                "username": parts[0],
                "ip":       parts[1],
                "port":     int(parts[2]),
            }
        except ValueError:
            pass

    return {"type": "unknown", "raw": line}


# ════════════════════════════════════════════════════════════════
# § 7  High-level Connection Helpers
# ════════════════════════════════════════════════════════════════

def connect_to(ip: str, port: int, timeout: float = 10.0) -> socket.socket:
    """
    Open a TCP connection to *ip*:*port*.
    Returns the connected socket.
    Raises ConnectionRefusedError / OSError on failure.
    """
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.settimeout(timeout)
    sock.connect((ip, port))
    return sock


class DiscoveryClient:
    """
    One-shot helper that opens a connection, sends ONE command,
    reads the response(s), then closes — matching how the C servers
    call discovery_auth() with a fresh TCP connection each time.

    Usage::
        dc = DiscoveryClient()
        dc.register("alice", "pass123", 5000)   # → True / False
        dc.auth("alice", "pass123")             # → True / False
        ip, port = dc.lookup("alice")           # → ("127.0.0.1", 5000) or None
        users = dc.list_users()                 # → [{"username":…,"ip":…,"port":…}]
        dc.unregister("alice")                  # → True / False
    """

    def __init__(self, ip: str = DISCOVERY_IP, port: int = DISCOVERY_PORT):
        self.ip   = ip
        self.port = port

    def _transact(self, message: str) -> list:
        """Send one line, collect all response lines until EOF or END."""
        sock = connect_to(self.ip, self.port)
        try:
            send_line(sock, message)
            lines = []
            while True:
                try:
                    line = read_line(sock)
                    lines.append(line)
                    if line in ("END", "OK", "FAIL") or \
                       line.startswith("OK") or \
                       line.startswith("FAIL") or \
                       line.startswith("FOUND") or \
                       line == "NOT_FOUND":
                        break
                except ConnectionError:
                    break
        finally:
            sock.close()
        return lines

    def register(self, username: str, password: str, port: int) -> bool:
        """REGISTER → True on OK, False on FAIL."""
        lines = self._transact(make_register(username, password, port))
        return bool(lines) and lines[0].startswith("OK")

    def auth(self, username: str, password: str) -> bool:
        """AUTH → True if credentials are valid."""
        lines = self._transact(make_auth(username, password))
        return bool(lines) and lines[0].startswith("OK")

    def lookup(self, username: str):
        """LOOKUP → (ip, port) tuple, or None if not found."""
        lines = self._transact(make_lookup(username))
        if not lines:
            return None
        parsed = parse_discovery_response(lines[0])
        if parsed["type"] == "found":
            return parsed["ip"], parsed["port"]
        return None

    def list_users(self) -> list:
        """LIST → list of dicts with keys username/ip/port."""
        sock = connect_to(self.ip, self.port)
        results = []
        try:
            send_line(sock, make_discovery_list())
            while True:
                try:
                    line = read_line(sock)
                except ConnectionError:
                    break
                parsed = parse_discovery_response(line)
                if parsed["type"] == "end":
                    break
                if parsed["type"] == "user_entry":
                    results.append(parsed)
        finally:
            sock.close()
        return results

    def unregister(self, username: str) -> bool:
        """UNREGISTER → True on OK."""
        lines = self._transact(make_unregister(username))
        return bool(lines) and lines[0].startswith("OK")


class ChatClient:
    """
    Interactive or programmatic client for the C chat servers.

    Usage::
        client = ChatClient()
        client.connect("thread")           # or "fork" / "select"
        client.login("alice", "pass123")   # raises on failure
        client.broadcast("Hello!")
        client.private_msg("bob", "hey")
        users = client.list_users()        # ["alice", "bob"]
        client.quit()
    """

    SERVER_PORTS = {
        "fork":   CHAT_PORT_FORK,
        "thread": CHAT_PORT_THREAD,
        "select": CHAT_PORT_SELECT,
    }

    def __init__(self, ip: str = DISCOVERY_IP):
        self.ip       = ip
        self.sock     = None
        self.username = None

    def connect(self, server_type: str = "thread") -> None:
        """Open TCP connection to the chosen server type."""
        port = self.SERVER_PORTS.get(server_type)
        if port is None:
            raise ValueError(f"Unknown server type '{server_type}'. "
                             f"Choose: fork, thread, select")
        self.sock = connect_to(self.ip, port)
        print(f"[*] Connected to {server_type} server on port {port}")

    def login(self, username: str, password: str) -> None:
        """
        Send LOGIN and wait for OK.
        Raises RuntimeError if the server returns FAIL.
        """
        send_line(self.sock, make_login(username, password))
        response = read_line(self.sock)
        parsed   = parse_server_response(response)
        if parsed["type"] != "ok":
            raise RuntimeError(f"Login failed: {parsed.get('reason', response)}")
        self.username = username
        print(f"[*] {parsed['message']}")

    def broadcast(self, message: str) -> None:
        """Send BROADCAST <message>."""
        send_line(self.sock, make_broadcast(message))

    def private_msg(self, target: str, message: str) -> None:
        """Send PRIVATE <target> <message>."""
        send_line(self.sock, make_private(target, message))

    def list_users(self) -> list:
        """
        Send LIST and parse the USERS response.
        Returns a list of username strings.
        """
        send_line(self.sock, make_list())
        response = read_line(self.sock)
        parsed   = parse_server_response(response)
        if parsed["type"] == "users":
            return parsed["users"]
        return []

    def receive(self) -> dict:
        """
        Block and read ONE message from the server.
        Returns a parsed dict (see parse_server_response).
        """
        line = read_line(self.sock)
        return parse_server_response(line)

    def quit(self) -> None:
        """Send QUIT and close the socket."""
        if self.sock:
            try:
                send_line(self.sock, make_quit())
            except OSError:
                pass
            self.sock.close()
            self.sock = None
        print(f"[*] Disconnected")