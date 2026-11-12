# ============================================================================
# Makefile – Multi-Client Chat System
# ============================================================================
#
# Targets:
#   make all            – Build all binaries
#   make discovery      – Build discovery server only
#   make chat_fork      – Build fork-based chat server
#   make chat_thread    – Build thread-based chat server
#   make chat_select    – Build select-based chat server
#   make client         – Build chat client
#   make clean          – Remove all binaries
#
# Ports (defined in common.h):
#   Discovery server : 9090
#   Fork server      : 8080
#   Thread server    : 8081
#   Select server    : 8082

CC      = gcc
CFLAGS  = -Wall -Wextra -g -I.
LDFLAGS = -lpthread

SRCS_DISC   = discovery_server.c
SRCS_FORK   = chat_server_fork.c
SRCS_THREAD = chat_server_thread.c
SRCS_SELECT = chat_server_select.c
SRCS_CLIENT = chat_client.c

BINS = discovery_server chat_server_fork chat_server_thread chat_server_select chat_client

.PHONY: all clean discovery chat_fork chat_thread chat_select client

all: $(BINS)

discovery_server: $(SRCS_DISC) common.h
	$(CC) $(CFLAGS) -o $@ $(SRCS_DISC) $(LDFLAGS)
	@echo "[OK] Built: discovery_server (port 9090)"

chat_server_fork: $(SRCS_FORK) common.h
	$(CC) $(CFLAGS) -o $@ $(SRCS_FORK) $(LDFLAGS)
	@echo "[OK] Built: chat_server_fork (port 8080)"

chat_server_thread: $(SRCS_THREAD) common.h
	$(CC) $(CFLAGS) -o $@ $(SRCS_THREAD) $(LDFLAGS)
	@echo "[OK] Built: chat_server_thread (port 8081)"

chat_server_select: $(SRCS_SELECT) common.h
	$(CC) $(CFLAGS) -o $@ $(SRCS_SELECT) $(LDFLAGS)
	@echo "[OK] Built: chat_server_select (port 8082)"

chat_client: $(SRCS_CLIENT) common.h
	$(CC) $(CFLAGS) -o $@ $(SRCS_CLIENT) $(LDFLAGS)
	@echo "[OK] Built: chat_client"

# Aliases
discovery: discovery_server
chat_fork: chat_server_fork
chat_thread: chat_server_thread
chat_select: chat_server_select
client: chat_client

clean:
	rm -f $(BINS)
	@echo "[OK] Cleaned build artifacts"