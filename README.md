# Multi-Client Chat System

## Table of Contents
1. [System Overview](#1-system-overview)
2. [Component Architecture Diagram](#2-component-architecture-diagram)
3. [Chat Server](#3-chat-server)
   - [1.1.1 Fork-Based Server](#311-fork-based-server)
   - [1.1.2 Thread-Based Server](#312-thread-based-server)
   - [1.1.3 Select-Based Server](#313-select-based-server)
   - [1.1.4 Broadcast Messages](#314-broadcast-messages)
   - [1.1.5 Private Messaging](#315-private-messaging)
   - [1.1.6 User Authentication](#316-user-authentication)
   - [1.1.7 Active User List](#317-active-user-list)
   - [1.1.8 Graceful Disconnection](#318-graceful-disconnection)
4. [Discovery Server](#4-discovery-server)
5. [Chat Client](#5-chat-client)
6. [How the Components Work Together](#6-how-the-components-work-together)
7. [File Structure](#7-file-structure)
8. [Compilation](#8-compilation)
9. [Execution](#9-execution)
10. [Testing Guide](#10-testing-guide)
11. [Troubleshooting](#11-troubleshooting)
12. [Protocol Specification](#12-protocol-specification)
    - [12.1 Transport Layer](#121-transport-layer)
    - [12.2 Message Framing](#122-message-framing)
    - [12.3 Discovery Server Protocol](#123-discovery-server-protocol)
    - [12.4 Chat Server Protocol](#124-chat-server-protocol)
    - [12.5 Client-Server Session Lifecycle](#125-clientserver-session-lifecycle)
    - [12.6 Message Format Reference](#126-message-format-reference)
    - [12.7 Error Handling](#127-error-handling)
    - [12.8 Protocol Flow Diagrams](#128-protocol-flow-diagrams)

---

## 1. System Overview

The system is a **multi-client real-time chat application** built entirely in C
using POSIX sockets. It consists of three independently running components:

| Component | File | Port | Role |
|---|---|---|---|
| Discovery Server | `discovery_server.c` | `9090` | DNS-like user registry |
| Chat Server (Fork) | `chat_server_fork.c` | `8080` | One process per client |
| Chat Server (Thread) | `chat_server_thread.c` | `8081` | One thread per client |
| Chat Server (Select) | `chat_server_select.c` | `8082` | Single-threaded I/O multiplexing |
| Chat Client | `chat_client.c` | — | Interactive CLI |
| Shared Definitions | `common.h` | — | Structs, constants, inline utilities |

All three chat servers implement **identical functionality** — broadcast, private
messaging, user list, authentication, and graceful disconnection — but use different
concurrency strategies. Clients connect to any one of the three servers using the
same protocol.

---

## 2. Component Architecture Diagram

```
                        +------------------+
                        | DISCOVERY SERVER |
                        |   port 9090      |
                        |                  |
                        | user table:      |
                        | name->ip:port    |
                        | + password       |
                        +--------+---------+
                                 |
               AUTH requests on every client LOGIN
                                 |
          +-----------+----------+-----------+
          |           |                      |
+---------v------+ +--v-----------+ +--------v------+
| CHAT SERVER    | | CHAT SERVER  | | CHAT SERVER   |
| fork-based     | | thread-based | | select-based  |
| port 8080      | | port 8081    | | port 8082     |
+---+--------+---+ +--+-------+--+ +---+-------+---+
    |        |        |       |         |       |
    | TCP    | TCP    | TCP   | TCP     | TCP   | TCP
    |        |        |       |         |       |
+---v--+ +---v--+ +---v--+ +--v---+ +---v--+ +--v---+
|Client| |Client| |Client| |Client| |Client| |Client|
|  A   | |  B   | |  C   | |  D   | |  E   | |  F   |
+------+ +------+ +------+ +------+ +------+ +------+
```

---

## 3. Chat Server

The chat server is responsible for managing all connected clients, routing messages,
enforcing authentication, and maintaining the active user list. Three implementations
are provided, each demonstrating a different concurrency model.

---

### 3.1.1 Fork-Based Server

**File:** `chat_server_fork.c` | **Port:** `8080`

#### Concurrency Model

```
                      Parent Process
                   +------------------+
                   | select() loop    |
                   | - server socket  |
                   | - ipc_pairs[0]   |
                   | - ipc_pairs[1]   |
                   | - ipc_pairs[N]   |
                   +--+------+------+-+
                      |      |      |
              socketpair()   |   socketpair()
              (IPC channel)  |   (IPC channel)
                      |      |      |
               +------v-+ +--v-----v+
               | Child 0| | Child 1 |  ...  Child N
               |        | |         |
               |select()| |select() |
               |client  | |client   |
               |ipc_fd  | |ipc_fd   |
               +--------+ +---------+
```

#### How It Works

1. The **parent process** runs a `select()` loop over the server socket and all
   IPC channels (`socketpair(AF_UNIX, SOCK_STREAM)`).
2. On each new connection, the parent calls `fork()` — the **child process** takes
   full ownership of that client's TCP socket.
3. The child authenticates the client, then enters its own `select()` loop watching
   both the client socket and its end of the IPC channel.
4. All inter-client communication (broadcast, private, LIST) is **routed through
   the parent** via fixed-size `ipc_msg_t` structs.
5. `SIGCHLD` is caught with `SA_RESTART` and `waitpid(WNOHANG)` to reap zombie
   child processes automatically.

#### IPC Message Types

| Direction | Type | Purpose |
|---|---|---|
| Child → Parent | `REGISTER_USER` | Child authenticated; record username |
| Child → Parent | `BROADCAST` | Relay message to all other clients |
| Child → Parent | `PRIVATE` | Relay message to one named client |
| Child → Parent | `LIST_REQ` | Request the active user list |
| Child → Parent | `QUIT` | Client disconnected cleanly |
| Parent → Child | `MSG_TO_CLIENT` | Deliver a broadcast/private/list to this client |

**Key Advantage:** Processes are fully isolated — a crash in one child does not affect
any other client or the parent. The OS guarantees memory isolation.

**Key Limitation:** All broadcast and private messages must make two IPC hops
(child → parent → child), adding latency compared to the shared-memory thread model.

---

### 3.1.2 Thread-Based Server

**File:** `chat_server_thread.c` | **Port:** `8081`

#### Concurrency Model

```
           Main Thread (accept loop)
                    |
         pthread_create() per client
                    |
       +------------+------------+
       |            |            |
  Thread 0      Thread 1     Thread N
  (client A)   (client B)   (client C)
       |            |            |
       +-----> clients[] <-------+
              (shared table,
              mutex-protected)
```

#### How It Works

1. The **main thread** runs a simple `accept()` loop.
2. Each accepted connection spawns a **detached `pthread`** via
   `pthread_attr_setdetachstate(PTHREAD_CREATE_DETACHED)` — threads clean up
   automatically on exit.
3. All threads share a single `clients[]` array protected by `pthread_mutex_t`.
4. Because threads share memory, **broadcast is a direct loop** over `clients[]` —
   no IPC channel needed.

#### Mutex-Protected Operations

| Operation | Protected Section |
|---|---|
| Login | Lock → check + insert → unlock |
| Broadcast | Lock → iterate + send → unlock |
| Private message | Lock → find target + send → unlock |
| LIST | Lock → build list string → unlock |
| Disconnect / cleanup | Lock → clear slot + broadcast departure → unlock |

**Key Advantage:** Simplest broadcast implementation — a single loop over the shared
`clients[]` array with no IPC overhead.

**Key Limitation:** `send()` is called while holding the mutex. If a slow client's TCP
buffer is full, `send()` will block and stall all other threads until it returns.

---

### 3.1.3 Select-Based Server

**File:** `chat_server_select.c` | **Port:** `8082`

#### Concurrency Model

```
             Single Thread — Event Loop
             +---------------------------+
             |  FD_ZERO / FD_SET rebuild |
             |  select(max_fd + 1, ...)  |
             |           |               |
             |    FD ready?              |
             |      |        |           |
             |  server_fd  clients[i]    |
             |  (accept)   (process cmd) |
             +---------------------------+
```

#### How It Works

1. A **single thread** manages all clients using `select()` for I/O multiplexing.
2. On each iteration, the `fd_set` is **rebuilt from scratch** (required because
   `select()` modifies the set in place).
3. `max_fd` is tracked dynamically as the highest open file descriptor.
4. When `select()` returns, the loop checks:
   - `server_fd` is set → `accept()` a new client and add it to `clients[]`
   - `clients[i].socket` is set → call `process_client_data(i)` to handle one command

#### State Machine Per Client

Each `sel_client_t` record tracks:

| Field | Type | Description |
|---|---|---|
| `socket` | int | TCP socket fd |
| `authenticated` | int | 0 = pending LOGIN, 1 = active |
| `username` | char[] | Set after successful LOGIN |
| `addr` | sockaddr_in | Client IP and port |

**Key Advantage:** No threads or processes — zero synchronisation overhead. One event
loop handles up to 10 clients with minimal memory footprint.

**Key Limitation:** `discovery_auth()` is a blocking TCP call. During authentication,
the entire event loop stalls for all other connected clients.

---

### 3.1.4 Broadcast Messages

All three servers implement broadcast identically from the client's perspective:

```
Client sends:            BROADCAST <message text>
All other clients get:   MSG [broadcast] <sender>: <message text>
```

The sender is excluded from receiving their own broadcast. The server iterates the
active client list and delivers only to clients with `authenticated == 1` and
`socket != sender_fd`.

---

### 3.1.5 Private Messaging

```
Client sends:       PRIVATE <target_username> <message text>
Target receives:    MSG [private] <sender>: <message text>

If target offline:  SERVER: User '<target>' is not online
```

The server searches the active client list for a matching `username` field. In the
fork server this search is done in the parent; in the thread and select servers it
is done inline with a table scan.

---

### 3.1.6 User Authentication

Authentication is **delegated to the discovery server** on every LOGIN attempt.
No credentials are stored locally in the chat server.

```
Chat Server receives:    LOGIN <username> <password>
Chat Server calls:       discovery_auth(username, password)
  -> opens TCP to discovery_server:9090
  -> sends:  AUTH <username> <password>
  -> reads:  OK  or  FAIL
  -> closes connection
Chat Server responds:    OK Welcome <username>!   or   FAIL Invalid credentials
```

---

### 3.1.7 Active User List

```
Client sends:      LIST
Server responds:   USERS alice,bob,charlie
```

The server scans the active client table for all entries with `authenticated == 1`
and concatenates their usernames into a single comma-separated string prefixed
with `USERS`.

---

### 3.1.8 Graceful Disconnection

#### Clean Disconnect (QUIT command)

```
Client sends:   QUIT
Server:         1. Broadcasts  SERVER: <username> left the chat
                2. Closes the TCP socket
                3. Frees the client slot
                4. Decrements the active client count
```

#### Abnormal Disconnect (crash / network drop)

The server detects disconnection when `recv()` or `read_line()` returns `<= 0`:

- **Fork server:** `recv()` on the IPC channel returns 0 when the child exits.
  The slot is cleaned up via `cleanup_slot()` which also broadcasts the departure.
- **Thread server:** `read_line()` in the client thread returns 0. The thread
  clears the slot under the mutex and broadcasts departure before exiting.
- **Select server:** `read_line()` in `process_client_data()` returns 0.
  `remove_client()` is called immediately, closing the socket and broadcasting
  departure.

In all three servers, abnormal disconnection is handled identically to a clean QUIT
from other clients' perspectives.

---

## 4. Discovery Server

**File:** `discovery_server.c` | **Port:** `9090`

### Role

The discovery server acts as a **DNS-like registry** that maps usernames to IP
addresses, ports, and passwords. It is the single source of truth for:

- Whether a username exists
- What password it was registered with
- What IP address and port the user is reachable at

### Architecture

```
        Discovery Server (port 9090)
        +-----------------------------+
        |  accept() loop              |
        |  pthread_create() per conn  |
        |          |                  |
        |    +-----------+            |
        |    | One-shot  |            |
        |    | handler   |            |
        |    | thread    |            |
        |    +-----------+            |
        |          |                  |
        |    users[] table            |
        |    (mutex-protected)        |
        |    max 100 entries          |
        +-----------------------------+
```

Each incoming TCP connection spawns a **detached thread** that reads exactly one
command, responds, and closes the connection. The user table is protected by a
single `pthread_mutex_t`.

### User Record Structure

| Field | Type | Description |
|---|---|---|
| `username` | char[64] | Unique display name |
| `password` | char[64] | Plaintext credential |
| `ip` | char[16] | IP captured from TCP connection via `getpeername()` |
| `port` | int | Port supplied by the client |
| `active` | int | 1 = slot in use, 0 = free |

### Supported Commands

| Command | Format | Response |
|---|---|---|
| `REGISTER` | `REGISTER <user> <pass> <port>` | `OK` / `FAIL <reason>` |
| `AUTH` | `AUTH <user> <pass>` | `OK` / `FAIL` |
| `LOOKUP` | `LOOKUP <user>` | `FOUND <ip> <port>` / `NOT_FOUND` |
| `LIST` | `LIST` | One line per user + `END` |
| `UNREGISTER` | `UNREGISTER <user>` | `OK` / `FAIL` |

The client's IP is recorded using `getpeername()` — the client does NOT supply its
own IP, preventing spoofing.

---

## 5. Chat Client

**File:** `chat_client.c`

### Architecture

The client uses a **two-thread design** to enable simultaneous sending and receiving:

```
          +----------------------------------------+
          |             CHAT CLIENT                |
          |                                        |
          |  Main Thread          recv_thread       |
          |  (stdin loop)         (server reader)  |
          |       |                    |           |
          |  read from stdin      read from socket |
          |  send to server       print to stdout  |
          |       |                    |           |
          |       +---- server_fd -----+           |
          +----------------------------------------+
                         |
                   TCP connection
                         |
                   Chat Server
```

`recv_thread` is **detached** (`pthread_detach`) — it sets the global `running = 0`
flag when the server closes the connection, causing the main thread's input loop to
exit on its next iteration.

### Startup Sequence

```
1. Prompt user for:  username, password, local port
2. REGISTER with discovery server
   -> TCP to 127.0.0.1:9090
   -> send:  REGISTER <user> <pass> <port>
   -> read:  OK  (failure is non-fatal; client proceeds)
3. TCP connect to chat server (port determined by argv[1])
4. Send:  LOGIN <user> <pass>
5. Read:  OK Welcome <user>!   (abort on FAIL)
6. Start recv_thread
7. Enter interactive command loop
```

### Command Parsing

| User Input | Sent to Server | Notes |
|---|---|---|
| `BROADCAST <text>` | `BROADCAST <text>` | Direct forward |
| `broadcast <text>` | `BROADCAST <text>` | Case-insensitive alias |
| `PRIVATE <u> <msg>` | `PRIVATE <u> <msg>` | Direct forward |
| `LIST` | `LIST` | Direct forward |
| `QUIT` | `QUIT` | Exits after sending |
| `HELP` | (local only) | Prints command reference |
| plain text | `BROADCAST <text>` | Treated as broadcast |

### Message Display

| Received Prefix | Displayed As |
|---|---|
| `MSG [broadcast] ...` | `[#] [broadcast] <sender>: <text>` |
| `MSG [private] ...` | `[PM] [private] <sender>: <text>` |
| `SERVER: ...` | `*** <text> ***` |
| Anything else | Printed as-is |

### Graceful Shutdown

On `QUIT` or `EOF` on stdin:
1. Sends `QUIT` to the chat server.
2. Closes the server socket.
3. Calls `UNREGISTER <username>` on the discovery server.
4. Prints goodbye message and exits.

---

## 6. How the Components Work Together

### End-to-End Flow: Two Clients Exchanging a Private Message

```
STEP 1 — alice starts her client
  alice client  -->  discovery:9090   :  REGISTER alice pass 5001
  discovery     -->  alice client     :  OK

STEP 2 — alice connects to chat server
  alice client  -->  chat_server:8081 :  LOGIN alice pass
  chat_server   -->  discovery:9090   :  AUTH alice pass
  discovery     -->  chat_server      :  OK
  chat_server   -->  alice client     :  OK Welcome alice!

STEP 3 — bob does the same
  bob client    -->  discovery:9090   :  REGISTER bob pass 5002
  bob client    -->  chat_server:8081 :  LOGIN bob pass
  chat_server   -->  alice client     :  SERVER: bob joined the chat

STEP 4 — alice sends a private message to bob
  alice client  -->  chat_server      :  PRIVATE bob hello!
  chat_server   -->  bob client       :  MSG [private] alice: hello!

STEP 5 — alice disconnects
  alice client  -->  chat_server      :  QUIT
  chat_server   -->  bob client       :  SERVER: alice left the chat
  alice client  -->  discovery:9090   :  UNREGISTER alice
```

---

## 7. File Structure

```
.
+-- common.h                 Shared constants, structs, inline socket utilities
+-- discovery_server.c       DNS-like user registry (port 9090)
+-- chat_server_fork.c       Fork-based concurrent chat server (port 8080)
+-- chat_server_thread.c     Thread-based concurrent chat server (port 8081)
+-- chat_server_select.c     Select-based I/O multiplexing server (port 8082)
+-- chat_client.c            Interactive two-thread CLI client
+-- Makefile                 Build targets
+-- benchmark.py             Load / stress test harness
+-- bench_client.py          Benchmark client
+-- monitor.py               CPU / memory sampler
+-- protocol.py              Python protocol helpers
+-- visualise.py             Plot generator
+-- README.md                This file
```

### Shared Definitions in `common.h`

| Definition | Value | Purpose |
|---|---|---|
| `DISCOVERY_IP` | `"127.0.0.1"` | Discovery server IP |
| `DISCOVERY_PORT` | `9090` | Discovery server port |
| `CHAT_PORT_FORK` | `8080` | Fork server port |
| `CHAT_PORT_THREAD` | `8081` | Thread server port |
| `CHAT_PORT_SELECT` | `8082` | Select server port |
| `BUFFER_SIZE` | `2048` | Max message length (bytes) |
| `MAX_CLIENTS` | `10` | Max simultaneous chat clients |
| `MAX_USERS` | `100` | Max registered users (discovery) |
| `MAX_USERNAME` | `64` | Max username length |
| `MAX_PASSWORD` | `64` | Max password length |
| `MAX_MSG` | `1400` | Max IPC message body (fork server) |

---

## 8. Compilation

All files must be in the same directory as `common.h`.

```bash
gcc -Wall -o discovery_server  discovery_server.c  -lpthread
gcc -Wall -o chat_server_fork   chat_server_fork.c   -lpthread
gcc -Wall -o chat_server_thread chat_server_thread.c -lpthread
gcc -Wall -o chat_server_select chat_server_select.c -lpthread
gcc -Wall -o chat_client        chat_client.c        -lpthread
```

**Prerequisites:** GCC compiler, POSIX-compliant OS (Linux / macOS), pthreads library,
and optionally `netcat` (`nc`) for manual protocol testing.

---

## 9. Execution

Open a **separate terminal for each process**. Always start the discovery server first.

### Terminal 1 — Discovery Server (always first)
```bash
./discovery_server
# Expected output:
# === Discovery Server ===
# Listening on port 9090
```

### Terminal 2 — Chat Server (pick one)
```bash
./chat_server_fork      # port 8080 — one process per client
./chat_server_thread    # port 8081 — one thread per client
./chat_server_select    # port 8082 — single-threaded select()
```

### Terminal 3, 4, ... — Clients (one per terminal)
```bash
./chat_client fork      # connects to fork server   (port 8080)
./chat_client thread    # connects to thread server (port 8081)
./chat_client select    # connects to select server  (port 8082)
```

Each client will prompt:
```
Username : alice
Password : mypassword
Local port (for discovery, e.g. 5000): 5001
```

> Use a **different local port** for each client (5001, 5002, 5003, ...).

### Available Client Commands

```
BROADCAST <message>           Send message to all online users
PRIVATE <username> <message>  Send message to one specific user
LIST                          Show all currently online users
QUIT                          Disconnect gracefully
HELP                          Show this command list
```

---

## 10. Testing Guide

### Test 1 — Basic Login

```
Terminal 1: ./discovery_server
Terminal 2: ./chat_server_thread
Terminal 3: ./chat_client thread
  > Username : alice
  > Password : pass123
  > Local port: 5001

Expected: [*] Registered with discovery server as 'alice'
          [*] Connected to chat server
          [*] OK Welcome alice!
```

### Test 2 — Broadcast Message

```
# Two clients logged in as alice (port 5001) and bob (port 5002)

alice types:   BROADCAST hello everyone!

bob sees:      [#] [broadcast] alice: hello everyone!
alice sees:    (no echo to self)
```

### Test 3 — Private Message

```
alice types:   PRIVATE bob hey bob!
bob sees:      [PM] [private] alice: hey bob!

# Test offline user
alice types:   PRIVATE charlie are you there?
alice sees:    *** User 'charlie' is not online ***
```

### Test 4 — User List

```
# Three clients online: alice, bob, charlie
alice types:   LIST
alice sees:    USERS alice,bob,charlie
```

### Test 5 — Graceful Disconnect

```
alice types:   QUIT
bob sees:      *** alice left the chat ***
alice sees:    [*] Disconnecting...
               [*] Unregistered 'alice' from discovery server
               [*] Goodbye, alice!
```

### Test 6 — Concurrent Clients (up to 10)

```bash
# Open 10 terminals, login each with a unique username and port
# On the 11th attempt:
Expected: FAIL Server full (max 10 clients)
```

### Test 7 — Abnormal Disconnection (Crash Simulation)

```
# alice is logged in; force-kill alice's client terminal (Ctrl+C)
bob sees:      *** alice left the chat ***
# Server correctly frees the slot — new clients can still connect
```

### Test 8 — All Three Server Types

```bash
./chat_server_fork    # Terminal 2
./chat_server_thread  # Terminal 3
./chat_server_select  # Terminal 4

./chat_client fork    # Terminal 5
./chat_client thread  # Terminal 6
./chat_client select  # Terminal 7

# All three support login, broadcast, private, list, and quit correctly
```

### Test 10 — Manual Test via Netcat

```bash
echo "REGISTER testuser pass123 5999" | nc 127.0.0.1 9090   # Expected: OK
echo "AUTH testuser pass123"          | nc 127.0.0.1 9090   # Expected: OK
echo "AUTH testuser wrongpass"        | nc 127.0.0.1 9090   # Expected: FAIL
echo "LOOKUP testuser"                | nc 127.0.0.1 9090   # Expected: FOUND 127.0.0.1 5999
echo "LIST"                           | nc 127.0.0.1 9090   # Expected: testuser ... + END
echo "UNREGISTER testuser"            | nc 127.0.0.1 9090   # Expected: OK
```

---

## 11. Troubleshooting

| Error | Cause | Fix |
|---|---|---|
| `Address already in use` | Port still bound from previous run | Wait ~30s or run `pkill chat_server_thread` |
| `Cannot reach discovery server` | Discovery server not running | Start `./discovery_server` first |
| `FAIL Username already registered` | Previous session not unregistered | Restart `./discovery_server` to clear all users |
| `FAIL Invalid credentials` | Wrong password or discovery server down | Check credentials; verify discovery server is running |
| `connect to chat server: Connection refused` | Chat server not running | Start the matching chat server |
| Client hangs after server disconnect | stdin blocks on `fgets` | Press Enter once to unblock and exit |

---

## 12. Protocol Specification

The system uses a **custom text-based application-layer protocol** over TCP. All
messages are plain ASCII strings, newline-terminated, and human-readable. The
protocol has two distinct sub-protocols:

| Sub-Protocol | Endpoint | Port | Purpose |
|---|---|---|---|
| Discovery Protocol | `discovery_server` | `9090` | User registration, authentication, lookup |
| Chat Protocol | `chat_server_*` | `8080 / 8081 / 8082` | Real-time messaging between clients |

---

### 12.1 Transport Layer

- **Protocol:** TCP (`SOCK_STREAM`) — guarantees ordered, reliable, byte-stream delivery.
- **Chat connections are STATEFUL:** a client holds one persistent TCP connection to
  the chat server for the entire session.
- **Discovery connections are STATELESS (one-shot):** each request opens a new TCP
  connection, sends exactly one command, reads one response, and closes.

---

### 12.2 Message Framing

#### Format

Every message in both sub-protocols uses newline framing:

```
<COMMAND> [ARG1] [ARG2] ... [BODY]\n
```

- Each message is terminated by a single LF (`\n`) character.
- `\r\n` (CRLF, from Telnet) is also accepted — the `\r` is silently discarded.
- A message is always **one line** — body text may not contain embedded newlines.
- Maximum message length: **2048 bytes** (`BUFFER_SIZE`), including the terminating `\n`.

#### Framing Implementation

Reading is done one byte at a time until `\n` is encountered (from `common.h`):

```c
while (i < buf_size - 1) {
    recv(fd, &c, 1, 0);
    if (c == '\r') continue;   // tolerate CRLF
    if (c == '\n') break;      // end of message
    buf[i++] = c;
}
buf[i] = '\0';                 // always NUL-terminated
```

This avoids partial-read issues and handles variable-length messages without a
separate length-prefix field.

#### Why Newline Framing?

| Property | Newline Framing | Length-Prefix Framing |
|---|---|---|
| Human-readable / Telnet testable | YES | NO |
| Simple to implement | YES | Moderate |
| Handles variable-length messages | YES | YES |
| Binary-safe | NO | YES |
| Testable with nc/netcat | YES | NO |

Newline framing is chosen because all messages are plain text and the system
benefits from being directly testable with `nc` or Telnet during development.

---

### 12.3 Discovery Server Protocol

**Port:** `9090` | **Connection model:** One command per TCP connection

#### REGISTER

```
Client -> Server:   REGISTER <username> <password> <port>
Server -> Client:   OK
                    FAIL <reason>
```

Failure responses: `FAIL Missing arguments` · `FAIL Username already registered` · `FAIL Registry full`

The server records the client's IP via `getpeername()` — the client does not supply it.

#### AUTH

```
Client -> Server:   AUTH <username> <password>
Server -> Client:   OK
                    FAIL
```

Used internally by chat servers on every LOGIN — clients never call AUTH directly.

#### LOOKUP

```
Client -> Server:   LOOKUP <username>
Server -> Client:   FOUND <ip> <port>
                    NOT_FOUND
```

#### LIST

```
Client -> Server:   LIST
Server -> Client:   <username1> <ip1> <port1>
                    <username2> <ip2> <port2>
                    ...
                    END
```

#### UNREGISTER

```
Client -> Server:   UNREGISTER <username>
Server -> Client:   OK
                    FAIL User not found
```

---

### 12.4 Chat Server Protocol

**Ports:** `8080` (fork) · `8081` (thread) · `8082` (select)  
**Connection model:** Persistent TCP connection per client session.

#### LOGIN

Must be the **first** message after connecting. No other command is accepted until
LOGIN succeeds.

```
Client -> Server:   LOGIN <username> <password>
Server -> Client:   OK Welcome <username>!
                    FAIL <reason>
```

Failure responses: `FAIL Please LOGIN first` · `FAIL Invalid credentials` ·
`FAIL User already logged in` · `FAIL Server full`

#### BROADCAST

```
Client -> Server:              BROADCAST <message text>
Server -> all other clients:   MSG [broadcast] <sender>: <message text>
```

The sender does **not** receive an echo of their own broadcast.

#### PRIVATE

```
Client -> Server:    PRIVATE <target_username> <message text>
Server -> target:    MSG [private] <sender>: <message text>

If target offline:
Server -> sender:    SERVER: User '<target>' is not online
```

#### LIST

```
Client -> Server:   LIST
Server -> Client:   USERS <user1>,<user2>,<user3>
```

#### QUIT

```
Client -> Server:   QUIT
```

On receiving QUIT (or a closed connection), the server broadcasts
`SERVER: <username> left the chat`, removes the user from the active table, and
closes the TCP socket.

---

### 12.5 Client-Server Session Lifecycle

#### Full Connection Sequence

```
CLIENT                  DISCOVERY SERVER         CHAT SERVER
  |                            |                       |
  |-- REGISTER user pass port->|                       |
  |<- OK ----------------------|                       |
  |                            |                       |
  |-- TCP connect ----------------------------------->  |
  |-- LOGIN user pass --------------------------------> |
  |                            |<-- AUTH user pass -----|
  |                            |--- OK / FAIL --------> |
  |<-- OK Welcome user! -----------------------------------
  |                            |                       |
  |====== interactive session ========================> |
  |-- BROADCAST hello -------------------------------------
  |<-- MSG [broadcast] other: hi --------------------------
  |-- PRIVATE bob hey -------------------------------------
  |-- LIST ------------------------------------------------
  |<-- USERS alice,bob ------------------------------------
  |                            |                       |
  |-- QUIT ------------------------------------------------
  |-- TCP close ------------------------------------------
  |-- UNREGISTER user -------->|                       |
  |<-- OK ---------------------|                       |
```

#### Abnormal Disconnection

If the client crashes or the TCP connection drops without `QUIT`:
- Server detects `recv()` returning `0` (EOF) or `-1` (error).
- The slot is immediately freed, `slot_count` is decremented.
- `SERVER: <username> left the chat` is broadcast to all remaining clients.
- All file descriptors associated with the slot are closed.

---

### 12.6 Message Format Reference

#### Client → Chat Server

| Command | Format | Description |
|---|---|---|
| `LOGIN` | `LOGIN <user> <pass>` | Authenticate |
| `BROADCAST` | `BROADCAST <text>` | Send to all users |
| `PRIVATE` | `PRIVATE <user> <text>` | Send to one user |
| `LIST` | `LIST` | Get online users |
| `QUIT` | `QUIT` | Disconnect |

#### Chat Server → Client

| Message | Format | Description |
|---|---|---|
| Success | `OK Welcome <user>!` | Login accepted |
| Failure | `FAIL <reason>` | Command rejected |
| Broadcast | `MSG [broadcast] <sender>: <text>` | Incoming broadcast |
| Private | `MSG [private] <sender>: <text>` | Incoming private message |
| User list | `USERS <u1>,<u2>,...` | Response to LIST |
| System | `SERVER: <text>` | Join/leave/error notice |

#### Client → Discovery Server

| Command | Format | Description |
|---|---|---|
| `REGISTER` | `REGISTER <user> <pass> <port>` | Register a new user |
| `AUTH` | `AUTH <user> <pass>` | Verify credentials |
| `LOOKUP` | `LOOKUP <user>` | Resolve user to IP:port |
| `LIST` | `LIST` | List all registered users |
| `UNREGISTER` | `UNREGISTER <user>` | Remove user |

#### Discovery Server → Client

| Response | Format | Description |
|---|---|---|
| Success | `OK` | Operation succeeded |
| Failure | `FAIL <reason>` | Operation failed |
| Found | `FOUND <ip> <port>` | LOOKUP result |
| Not found | `NOT_FOUND` | LOOKUP: no match |
| List entry | `<user> <ip> <port>` | One line per registered user |
| List end | `END` | Terminates LIST response |

---

### 12.7 Error Handling

#### Authentication Errors

| Scenario | Server Response |
|---|---|
| Wrong password | `FAIL Invalid credentials` |
| Username already online | `FAIL User already logged in` |
| Command before LOGIN | `FAIL Please LOGIN first` |
| Server at capacity | `FAIL Server full (max 10 clients)` |

#### Messaging Errors

| Scenario | Server Response |
|---|---|
| PRIVATE to offline user | `SERVER: User '<name>' is not online` |
| Unknown command | `FAIL Unknown command. Use: BROADCAST\|PRIVATE\|LIST\|QUIT` |

#### Connection Errors

| Scenario | Behaviour |
|---|---|
| Client crashes (no QUIT) | Server detects closed socket, frees slot, notifies others |
| Discovery server unreachable | `discovery_auth()` returns 0 → `FAIL Invalid credentials` |
| Chat server full on `accept()` | Sends `FAIL Server full`, closes socket immediately |
| Message body exceeds 1400 bytes | Silently truncated at `MAX_MSG`; no error sent to client |

---

### 12.8 Protocol Flow Diagrams

#### Successful Login

```
Client         Chat Server       Discovery Server
  |                 |                   |
  |--LOGIN u p ----->|                   |
  |                 |---AUTH u p ------->|
  |                 |<--OK --------------|
  |<--OK Welcome u!--|                   |
```

#### Broadcast Message

```
alice          Chat Server         bob           charlie
  |                 |               |               |
  |--BROADCAST hi-->|               |               |
  |                 |--MSG [broadcast] alice: hi --->|
  |                 |--MSG [broadcast] alice: hi ----------->|
```

#### Private Message

```
alice          Chat Server         bob
  |                 |               |
  |--PRIVATE bob hey->|              |
  |                 |--MSG [private] alice: hey -->|
```

#### Graceful Disconnect

```
alice          Chat Server         bob
  |                 |               |
  |-----QUIT ------->|               |
  |                 |--SERVER: alice left the chat -->|
  |                 |               |
 [slot freed, TCP closed]
```

#### Abnormal Disconnect (client crash)

```
alice          Chat Server         bob
  |                 |               |
  X (crash)         |               |
  ~--EOF detected-->|               |
                    |--SERVER: alice left the chat -->|
                    |               |
                   [slot freed automatically]
```