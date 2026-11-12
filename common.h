/* ============================================================================
 * COMMON DEFINITIONS FOR MULTI-CLIENT CHAT SYSTEM
 * ============================================================================
 * File: common.h
 *
 * Shared by: discovery_server.c, chat_server_fork.c, chat_server_thread.c,
 *            chat_server_select.c, chat_client.c
 *
 * Protocol (text-based, newline-terminated):
 *
 *   Client → Chat Server:
 *     LOGIN <username> <password>
 *     BROADCAST <message>
 *     PRIVATE <target_user> <message>
 *     LIST
 *     QUIT
 *
 *   Chat Server → Client:
 *     OK / FAIL <reason>
 *     MSG [broadcast] <sender>: <text>
 *     MSG [private] <sender>: <text>
 *     USERS <user1>,<user2>,...
 *     SERVER: <system text>
 *
 *   Client → Discovery Server:
 *     REGISTER <username> <password> <port>
 *     AUTH     <username> <password>
 *     LOOKUP   <username>
 *     LIST
 *     UNREGISTER <username>
 *
 *   Discovery Server → Client:
 *     OK / FAIL <reason>
 *     FOUND <ip> <port> / NOT_FOUND
 *     <user> <ip> <port>  (repeated lines, terminated by END)
 */

#ifndef COMMON_H
#define COMMON_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <unistd.h>
#include <errno.h>
#include <sys/socket.h>
#include <arpa/inet.h>

/* ---- Network ports ---- */
#define DISCOVERY_IP        "127.0.0.1"
#define DISCOVERY_PORT      9090        /* Discovery / DNS-like service     */
#define CHAT_PORT_FORK      8080        /* Fork-based chat server           */
#define CHAT_PORT_THREAD    8081        /* Thread-based chat server         */
#define CHAT_PORT_SELECT    8082        /* Select-based chat server         */

/* ---- Capacity limits ---- */
#define BUFFER_SIZE         2048
#define MAX_CLIENTS         10          /* Max simultaneous chat clients    */
#define MAX_USERS           100         /* Max registered users (discovery) */
#define MAX_USERNAME        64
#define MAX_PASSWORD        64
#define MAX_MSG             1400        /* Max chat message body            */

/* =========================================================================
 * IPC message passed between parent and child in the fork-based server.
 * Fixed-size struct so recv(MSG_WAITALL) can read it atomically.
 * ========================================================================= */
typedef struct {
    char type[20];              /* REGISTER_USER, BROADCAST, PRIVATE,       */
                                /* LIST_REQ, QUIT, MSG_TO_CLIENT            */
    char sender[MAX_USERNAME];  /* Who sent the original message            */
    char target[MAX_USERNAME];  /* Recipient username (PRIVATE / targeted)  */
    char content[MAX_MSG];      /* Message body or server response          */
} ipc_msg_t;

/* =========================================================================
 * A single connected client record used by thread and select servers.
 * ========================================================================= */
typedef struct {
    int  socket;
    char username[MAX_USERNAME];
    int  authenticated;             /* 0 = pending login, 1 = logged in     */
    struct sockaddr_in addr;
} client_t;

/* =========================================================================
 * User record stored in the discovery server.
 * ========================================================================= */
typedef struct {
    char username[MAX_USERNAME];
    char password[MAX_PASSWORD];
    char ip[INET_ADDRSTRLEN];
    int  port;                      /* Port the user registered with        */
    int  active;                    /* 1 = slot in use                      */
} user_record_t;

/* =========================================================================
 * Utility: read one newline-terminated line from a socket fd.
 * Returns number of chars read (>0), 0 on disconnect, -1 on error.
 * The newline is consumed but NOT stored; buf is always NUL-terminated.
 * ========================================================================= */
static inline int read_line(int fd, char *buf, int buf_size)
{
    int  i = 0;
    char c;
    while (i < buf_size - 1) {
        int n = recv(fd, &c, 1, 0);
        if (n <= 0) {
            buf[i] = '\0';
            return n;           /* 0 = peer closed, -1 = error            */
        }
        if (c == '\r') continue; /* tolerate \r\n from telnet             */
        if (c == '\n') break;
        buf[i++] = c;
    }
    buf[i] = '\0';
    return i;
}

/* =========================================================================
 * Utility: send a formatted line (adds '\n') to a socket.
 * ========================================================================= */
static inline int send_line(int fd, const char *fmt, ...)
{
    char buf[BUFFER_SIZE];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf) - 2, fmt, ap);
    va_end(ap);
    buf[n]     = '\n';
    buf[n + 1] = '\0';
    return send(fd, buf, n + 1, 0);
}

/* =========================================================================
 * Utility: open a TCP connection to ip:port.
 * Returns socket fd on success, -1 on failure.
 * ========================================================================= */
static inline int tcp_connect(const char *ip, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port   = htons(port);
    inet_pton(AF_INET, ip, &addr.sin_addr);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    return fd;
}

/* =========================================================================
 * Utility: authenticate a user against the discovery server.
 * Returns 1 on success, 0 on failure.
 * ========================================================================= */
static inline int discovery_auth(const char *username, const char *password)
{
    int fd = tcp_connect(DISCOVERY_IP, DISCOVERY_PORT);
    if (fd < 0) return 0;

    char buf[BUFFER_SIZE];
    send_line(fd, "AUTH %s %s", username, password);
    int n = read_line(fd, buf, sizeof(buf));
    close(fd);

    return (n > 0 && strncmp(buf, "OK", 2) == 0) ? 1 : 0;
}

#endif /* COMMON_H */