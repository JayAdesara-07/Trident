/* ============================================================================
 * CHAT SERVER – THREAD-BASED CONCURRENT SERVER
 * ============================================================================
 * File: chat_server_thread.c
 * Port: 8081
 *
 * Concepts Covered:
 * - pthread_create()           – One thread per connected client
 * - pthread_detach()           – Threads clean themselves up on exit
 * - pthread_mutex_lock/unlock  – Protect the shared client table
 * - Shared memory model        – All threads see the same client[] array,
 *                                enabling direct broadcast with a single loop
 * - User authentication        – Validated against discovery server
 *
 * Architecture:
 *
 *   Main thread: accept() loop → pthread_create() per client
 *
 *   Per-client thread:
 *     1. Receive LOGIN, authenticate via discovery server
 *     2. Register in shared clients[] table (mutex-protected)
 *     3. recv loop:
 *          BROADCAST → iterate clients[], send to all others
 *          PRIVATE   → find target in clients[], send directly
 *          LIST      → build comma list from clients[], send back
 *          QUIT      → break
 *     4. Deregister from clients[], exit thread
 *
 * Key advantage over fork: threads share memory so broadcast is a simple
 * loop over the clients[] array – no IPC channel needed.
 */

#include "common.h"
#include <pthread.h>
#include <stdarg.h>

/* ---- Shared client table ---- */
static client_t      clients[MAX_CLIENTS];
static pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================
 * Utility: send a formatted message to every authenticated
 *          client EXCEPT the one with socket 'skip_fd'.
 * CALLER must hold clients_mutex.
 * ============================================================ */
static void broadcast_all(int skip_fd, const char *fmt, ...)
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
 * Utility: send to one specific username.
 * Returns 1 if found & sent, 0 if not online.
 * CALLER must hold clients_mutex.
 * ============================================================ */
static int send_to_user(const char *username, const char *line)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (clients[i].socket > 0 &&
            clients[i].authenticated &&
            strcmp(clients[i].username, username) == 0) {
            send(clients[i].socket, line, strlen(line), 0);
            return 1;
        }
    }
    return 0;
}

/* ============================================================
 * handle_client_thread: full lifecycle of one client
 * ============================================================ */
static void *handle_client_thread(void *arg)
{
    client_t local = *(client_t *)arg;
    free(arg);

    int  fd = local.socket;
    char buf[BUFFER_SIZE];
    char username[MAX_USERNAME] = {0};

    printf("[Thread %lu] New client %s:%d\n",
           (unsigned long)pthread_self(),
           inet_ntoa(local.addr.sin_addr),
           ntohs(local.addr.sin_port));

    /* ---- Step 1: Authenticate ---- */
    char password[MAX_PASSWORD];
    int slot_idx = -1;

    while (slot_idx < 0) {
        if (read_line(fd, buf, sizeof(buf)) <= 0) goto disconnect_noauth;

        char cmd[32];
        cmd[0] = username[0] = password[0] = '\0';
        sscanf(buf, "%31s %63s %63s", cmd, username, password);

        if (strcmp(cmd, "LOGIN") != 0) {
            send_line(fd, "FAIL Please LOGIN first");
            continue;
        }

        if (!discovery_auth(username, password)) {
            send_line(fd, "FAIL Invalid credentials");
            username[0] = '\0';
            continue;
        }

        /* Check if username already active */
        pthread_mutex_lock(&clients_mutex);
        int dup = 0;
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket > 0 &&
                clients[i].authenticated &&
                strcmp(clients[i].username, username) == 0) {
                dup = 1; break;
            }
        }
        if (dup) {
            pthread_mutex_unlock(&clients_mutex);
            send_line(fd, "FAIL User already logged in");
            continue;
        }

        /* Find a free slot */
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (clients[i].socket == 0) {
                clients[i].socket        = fd;
                clients[i].authenticated = 1;
                strncpy(clients[i].username, username, MAX_USERNAME - 1);
                clients[i].addr          = local.addr;
                slot_idx = i;
                break;
            }
        }

        if (slot_idx < 0) {
            pthread_mutex_unlock(&clients_mutex);
            send_line(fd, "FAIL Server full");
            goto disconnect_noauth;
        }

        /* Announce to everyone else */
        broadcast_all(fd, "SERVER: %s joined the chat", username);
        pthread_mutex_unlock(&clients_mutex);

        send_line(fd, "OK Welcome %s!", username);
        printf("[Thread %lu] '%s' authenticated (slot %d)\n",
               (unsigned long)pthread_self(), username, slot_idx);
    }

    /* ---- Step 2: Command loop ---- */
    while (1) {
        if (read_line(fd, buf, sizeof(buf)) <= 0) break;

        char cmd[32], arg1[MAX_USERNAME];
        cmd[0] = arg1[0] = '\0';
        sscanf(buf, "%31s %63s", cmd, arg1);

        /* Advance past "CMD arg1 " to find message body */
        char *body = buf;
        int spaces = 0;
        while (*body && spaces < 2) {
            if (*body++ == ' ') spaces++;
        }

        if (strcmp(cmd, "BROADCAST") == 0) {
            char *msg_body = buf;
            while (*msg_body && *msg_body != ' ') msg_body++;
            if (*msg_body == ' ') msg_body++;

            printf("[Thread %lu] Broadcast from '%s': %s\n",
                   (unsigned long)pthread_self(), username, msg_body);

            pthread_mutex_lock(&clients_mutex);
            broadcast_all(fd, "MSG [broadcast] %s: %s", username, msg_body);
            pthread_mutex_unlock(&clients_mutex);
        }

        else if (strcmp(cmd, "PRIVATE") == 0) {
            char line[BUFFER_SIZE];
            snprintf(line, sizeof(line), "MSG [private] %s: %s\n",
                     username, body);

            pthread_mutex_lock(&clients_mutex);
            int sent = send_to_user(arg1, line);
            pthread_mutex_unlock(&clients_mutex);

            if (!sent)
                send_line(fd, "SERVER: User '%s' is not online", arg1);
        }

        else if (strcmp(cmd, "LIST") == 0) {
            char list[BUFFER_SIZE] = "USERS ";
            int  first = 1;

            pthread_mutex_lock(&clients_mutex);
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (clients[i].socket > 0 && clients[i].authenticated) {
                    if (!first) strncat(list, ",", sizeof(list) - strlen(list) - 1);
                    strncat(list, clients[i].username,
                            sizeof(list) - strlen(list) - 1);
                    first = 0;
                }
            }
            pthread_mutex_unlock(&clients_mutex);

            send_line(fd, "%s", list);
        }

        else if (strcmp(cmd, "QUIT") == 0) {
            break;
        }

        else {
            send_line(fd, "FAIL Unknown command");
        }
    }

    /* ---- Step 3: Clean up ---- */
    pthread_mutex_lock(&clients_mutex);
    if (slot_idx >= 0) {
        clients[slot_idx].socket        = 0;
        clients[slot_idx].authenticated = 0;
        memset(clients[slot_idx].username, 0, MAX_USERNAME);
        broadcast_all(fd, "SERVER: %s left the chat", username);
    }
    pthread_mutex_unlock(&clients_mutex);

disconnect_noauth:
    printf("[Thread %lu] '%s' disconnected\n",
           (unsigned long)pthread_self(), username);
    close(fd);
    return NULL;
}

/* ============================================================
 * main
 * ============================================================ */
int main(void)
{
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len;

    memset(clients, 0, sizeof(clients));

    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family      = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(CHAT_PORT_THREAD);

    if (bind(server_fd, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) { perror("bind"); exit(EXIT_FAILURE); }

    if (listen(server_fd, 10) < 0) { perror("listen"); exit(EXIT_FAILURE); }

    printf("=== Thread-Based Chat Server ===\n");
    printf("Listening on port %d\n", CHAT_PORT_THREAD);
    printf("Each client handled by a separate detached pthread\n");
    printf("Broadcast uses direct shared-memory iteration (mutex-protected)\n\n");

    while (1) {
        client_addr_len = sizeof(client_addr);
        int client_fd   = accept(server_fd,
                                 (struct sockaddr *)&client_addr,
                                 &client_addr_len);
        if (client_fd < 0) { perror("accept"); continue; }

        printf("New connection: %s:%d\n",
               inet_ntoa(client_addr.sin_addr),
               ntohs(client_addr.sin_port));

        /* Pack client info for the new thread */
        client_t *cl = malloc(sizeof(client_t));
        if (!cl) { perror("malloc"); close(client_fd); continue; }

        cl->socket        = client_fd;
        cl->authenticated = 0;
        cl->addr          = client_addr;
        memset(cl->username, 0, sizeof(cl->username));

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

        if (pthread_create(&tid, &attr, handle_client_thread, cl) != 0) {
            perror("pthread_create");
            free(cl);
            close(client_fd);
        } else {
            printf("Created thread %lu for client\n\n",
                   (unsigned long)tid);
        }
        pthread_attr_destroy(&attr);
    }

    close(server_fd);
    return 0;
}