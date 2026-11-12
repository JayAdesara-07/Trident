/* ============================================================================
 * CHAT SERVER – SELECT-BASED NON-BLOCKING SERVER (I/O MULTIPLEXING)
 * ============================================================================
 * File: chat_server_select.c
 * Port: 8082
 *
 * Concepts Covered:
 * - select()            – Single-threaded I/O multiplexing
 * - fd_set macros       – FD_ZERO, FD_SET, FD_ISSET, FD_CLR
 * - State machine       – Each client tracks its own auth state
 * - Non-blocking design – No thread/process creation; one event loop handles all
 *
 * How it Works:
 *   1. Server maintains an array of up to MAX_CLIENTS client records
 *   2. Each iteration rebuilds the fd_set from active client sockets
 *   3. select() blocks until ANY socket has data ready to read
 *   4. The event loop checks:
 *       - server_fd  → accept new client
 *       - clients[i] → process incoming command
 *   5. All state (username, auth flag) lives in clients[] – no threads needed
 *
 * Limitation (noted in comments, as in program 5):
 *   FD_SETSIZE is typically 1024; use epoll/kqueue for production scale.
 *
 * Authentication:
 *   Each client in UNAUTHENTICATED state must send LOGIN first.
 *   Auth is validated by a short TCP call to the discovery server.
 *   Because this is single-threaded, discovery auth must be fast.
 *   (For production: use non-blocking connect to discovery instead.)
 */

#include "common.h"
#include <stdarg.h>
#include <sys/select.h>

/* Extended client record: persists across select() iterations */
typedef struct {
    int  socket;
    int  authenticated;
    char username[MAX_USERNAME];
    struct sockaddr_in addr;
} sel_client_t;

static sel_client_t clients[MAX_CLIENTS];
static int          client_count = 0;

/* ============================================================
 * broadcast_msg: send a formatted line to all authenticated
 *                clients except the one with fd == skip_fd
 * ============================================================ */
static void broadcast_msg(int skip_fd, const char *fmt, ...)
{
    char buf[BUFFER_SIZE];
    va_list ap;
    va_start(ap, fmt);
    int n = vsnprintf(buf, sizeof(buf) - 2, fmt, ap);
    va_end(ap);
    buf[n]     = '\n';
    buf[n + 1] = '\0';

    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket > 0 &&
            clients[i].authenticated &&
            clients[i].socket != skip_fd) {
            send(clients[i].socket, buf, n + 1, 0);
        }
    }
}

/* ============================================================
 * remove_client: close socket and clear slot
 * ============================================================ */
static void remove_client(int idx)
{
    if (!clients[idx].socket) return;

    printf("Client disconnected: %s:%d (socket %d, user '%s')\n",
           inet_ntoa(clients[idx].addr.sin_addr),
           ntohs(clients[idx].addr.sin_port),
           clients[idx].socket,
           clients[idx].username);

    if (clients[idx].authenticated) {
        broadcast_msg(clients[idx].socket,
                      "SERVER: %s left the chat", clients[idx].username);
    }

    close(clients[idx].socket);
    memset(&clients[idx], 0, sizeof(clients[idx]));
    client_count--;
}

/* ============================================================
 * process_client_data: called when clients[idx].socket is
 *                       ready to read. Reads one line and acts.
 * ============================================================ */
static void process_client_data(int idx)
{
    char buf[BUFFER_SIZE];
    int  fd = clients[idx].socket;

    int n = read_line(fd, buf, sizeof(buf));
    if (n <= 0) {
        remove_client(idx);
        return;
    }

    char cmd[32], arg1[MAX_USERNAME];
    cmd[0] = arg1[0] = '\0';
    sscanf(buf, "%31s %63s", cmd, arg1);

    /* Advance body pointer past "CMD arg1 " */
    char *body = buf;
    int   spaces = 0;
    while (*body && spaces < 2) {
        if (*body++ == ' ') spaces++;
    }

    /* ================================================================
     * UNAUTHENTICATED state – only LOGIN is accepted
     * ================================================================ */
    if (!clients[idx].authenticated) {
        if (strcmp(cmd, "LOGIN") != 0) {
            send_line(fd, "FAIL Please LOGIN first");
            return;
        }

        char password[MAX_PASSWORD] = {0};
        sscanf(buf, "%*s %*s %63s", password);

        /* Check for duplicate active username */
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (i != idx &&
                clients[i].socket > 0 &&
                clients[i].authenticated &&
                strcmp(clients[i].username, arg1) == 0) {
                send_line(fd, "FAIL User already logged in");
                return;
            }
        }

        /* Blocking auth call to discovery server */
        if (!discovery_auth(arg1, password)) {
            send_line(fd, "FAIL Invalid credentials");
            return;
        }

        strncpy(clients[idx].username, arg1, MAX_USERNAME - 1);
        clients[idx].authenticated = 1;

        send_line(fd, "OK Welcome %s!", arg1);
        broadcast_msg(fd, "SERVER: %s joined the chat", arg1);

        printf("Client authenticated: '%s' (socket %d, slot %d)\n",
               arg1, fd, idx);
        return;
    }

    /* ================================================================
     * AUTHENTICATED state – handle chat commands
     * ================================================================ */

    /* ---- BROADCAST ---- */
    if (strcmp(cmd, "BROADCAST") == 0) {
        char *msg_body = buf;
        while (*msg_body && *msg_body != ' ') msg_body++;
        if (*msg_body == ' ') msg_body++;

        printf("Socket %d ('%s') broadcast: %s\n",
               fd, clients[idx].username, msg_body);
        broadcast_msg(fd, "MSG [broadcast] %s: %s",
                      clients[idx].username, msg_body);
    }

    /* ---- PRIVATE ---- */
    else if (strcmp(cmd, "PRIVATE") == 0) {
        char line[BUFFER_SIZE];
        snprintf(line, sizeof(line), "MSG [private] %s: %s\n",
                 clients[idx].username, body);

        int delivered = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket > 0 &&
                clients[i].authenticated &&
                strcmp(clients[i].username, arg1) == 0) {
                send(clients[i].socket, line, strlen(line), 0);
                delivered = 1;
                break;
            }
        }
        if (!delivered)
            send_line(fd, "SERVER: User '%s' is not online", arg1);
    }

    /* ---- LIST ---- */
    else if (strcmp(cmd, "LIST") == 0) {
        char list[BUFFER_SIZE] = "USERS ";
        int  first = 1;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket > 0 && clients[i].authenticated) {
                if (!first) strncat(list, ",", sizeof(list) - strlen(list) - 1);
                strncat(list, clients[i].username,
                        sizeof(list) - strlen(list) - 1);
                first = 0;
            }
        }
        send_line(fd, "%s", list);
    }

    /* ---- QUIT ---- */
    else if (strcmp(cmd, "QUIT") == 0) {
        remove_client(idx);
    }

    /* ---- Unknown ---- */
    else {
        send_line(fd, "FAIL Unknown command. Use: BROADCAST | PRIVATE | LIST | QUIT");
    }
}

/* ============================================================
 * main
 * ============================================================ */
int main(void)
{
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len;
    fd_set read_fds;
    int    max_fd, activity;

    memset(clients, 0, sizeof(clients));

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family      = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(CHAT_PORT_SELECT);

    if (bind(server_fd, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) { perror("bind"); exit(EXIT_FAILURE); }

    if (listen(server_fd, 10) < 0) { perror("listen"); exit(EXIT_FAILURE); }

    printf("=== Select-Based Chat Server (I/O Multiplexing) ===\n");
    printf("Listening on port %d\n", CHAT_PORT_SELECT);
    printf("Single thread managing up to %d clients via select()\n\n",
           MAX_CLIENTS);

    /* ---- Main select() event loop ---- */
    while (1) {
        /* Rebuild fd_set every iteration (select modifies it in-place) */
        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);
        max_fd = server_fd;

        for (int i = 0; i < MAX_CLIENTS; i++) {
            int sd = clients[i].socket;
            if (sd > 0) {
                FD_SET(sd, &read_fds);
                if (sd > max_fd) max_fd = sd;
            }
        }

        /* Block until at least one fd is ready */
        activity = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (activity < 0) {
            perror("select");
            continue;
        }

        /* ---- New incoming connection ---- */
        if (FD_ISSET(server_fd, &read_fds)) {
            client_addr_len = sizeof(client_addr);
            int client_fd   = accept(server_fd,
                                     (struct sockaddr *)&client_addr,
                                     &client_addr_len);
            if (client_fd < 0) { perror("accept"); continue; }

            if (client_count >= MAX_CLIENTS) {
                send_line(client_fd, "FAIL Server full (max %d clients)", MAX_CLIENTS);
                close(client_fd);
                continue;
            }

            /* Add to client list */
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].socket == 0) {
                    clients[i].socket        = client_fd;
                    clients[i].authenticated = 0;
                    clients[i].addr          = client_addr;
                    memset(clients[i].username, 0, MAX_USERNAME);
                    client_count++;

                    printf("New connection from %s:%d (socket %d, slot %d)\n",
                           inet_ntoa(client_addr.sin_addr),
                           ntohs(client_addr.sin_port),
                           client_fd, i);
                    break;
                }
            }
        }

        /* ---- Data on existing client sockets ---- */
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket > 0 &&
                FD_ISSET(clients[i].socket, &read_fds)) {
                process_client_data(i);
            }
        }
    }

    close(server_fd);
    return 0;
}