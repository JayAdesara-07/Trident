/* ============================================================================
 * DISCOVERY SERVER  –  DNS-LIKE USER REGISTRY
 * ============================================================================
 * File: discovery_server.c
 * Port: 9090
 *
 * Concepts Covered:
 * - Acts as a DNS-like service: maps username → {IP, port, password}
 * - Thread-per-client concurrency (pthreads + pthread_detach)
 * - Mutex-protected shared user table
 * - Stateless, one-shot connections (client connects, sends one command, disconnects)
 *
 * Supported Commands:
 *   REGISTER <username> <password> <port>
 *       → OK  |  FAIL <reason>
 *
 *   AUTH <username> <password>
 *       → OK  |  FAIL
 *
 *   LOOKUP <username>
 *       → FOUND <ip> <port>  |  NOT_FOUND
 *
 *   LIST
 *       → one line per registered user: "<username> <ip> <port>"
 *          terminated by "END"
 *
 *   UNREGISTER <username>
 *       → OK  |  FAIL
 */

#include "common.h"
#include <pthread.h>
#include <stdarg.h>
#include <sys/types.h>
#include <netinet/in.h>

/* ---- Shared user table ---- */
static user_record_t users[MAX_USERS];
static int           user_count = 0;
static pthread_mutex_t users_mutex = PTHREAD_MUTEX_INITIALIZER;

/* ============================================================
 * find_user_idx:  returns index of user in table, or -1
 * CALLER must hold users_mutex
 * ============================================================ */
static int find_user_idx(const char *username)
{
    for (int i = 0; i < MAX_USERS; i++) {
        if (users[i].active &&
            strcmp(users[i].username, username) == 0) {
            return i;
        }
    }
    return -1;
}

/* ============================================================
 * find_free_slot:  returns first unused slot index, or -1
 * CALLER must hold users_mutex
 * ============================================================ */
static int find_free_slot(void)
{
    for (int i = 0; i < MAX_USERS; i++) {
        if (!users[i].active) return i;
    }
    return -1;
}

/* ============================================================
 * handle_client_thread:  processes ONE command from a client
 *                         then closes the connection.
 * ============================================================ */
static void *handle_client_thread(void *arg)
{
    int client_fd = *(int *)arg;
    free(arg);

    char buf[BUFFER_SIZE];
    if (read_line(client_fd, buf, sizeof(buf)) <= 0) {
        close(client_fd);
        return NULL;
    }

    /* --- Tokenise the command line --- */
    char cmd[32], arg1[MAX_USERNAME], arg2[MAX_PASSWORD], arg3[32];
    cmd[0] = arg1[0] = arg2[0] = arg3[0] = '\0';
    sscanf(buf, "%31s %63s %63s %31s", cmd, arg1, arg2, arg3);

    printf("[Discovery] Command: %s | args: '%s' '%s' '%s'\n",
           cmd, arg1, arg2, arg3);

    /* ---- REGISTER <username> <password> <port> ---- */
    if (strcmp(cmd, "REGISTER") == 0) {
        if (arg1[0] == '\0' || arg2[0] == '\0' || arg3[0] == '\0') {
            send_line(client_fd, "FAIL Missing arguments");
            goto done;
        }

        /* Capture the client's IP address */
        struct sockaddr_in peer;
        socklen_t peer_len = sizeof(peer);
        getpeername(client_fd, (struct sockaddr *)&peer, &peer_len);
        char client_ip[INET_ADDRSTRLEN];
        inet_ntop(AF_INET, &peer.sin_addr, client_ip, sizeof(client_ip));

        pthread_mutex_lock(&users_mutex);

        if (find_user_idx(arg1) >= 0) {
            pthread_mutex_unlock(&users_mutex);
            send_line(client_fd, "FAIL Username already registered");
            goto done;
        }

        int slot = find_free_slot();
        if (slot < 0) {
            pthread_mutex_unlock(&users_mutex);
            send_line(client_fd, "FAIL Registry full");
            goto done;
        }

        strncpy(users[slot].username, arg1, MAX_USERNAME - 1);
        strncpy(users[slot].password, arg2, MAX_PASSWORD - 1);
        strncpy(users[slot].ip,       client_ip, INET_ADDRSTRLEN - 1);
        users[slot].port   = atoi(arg3);
        users[slot].active = 1;
        user_count++;

        pthread_mutex_unlock(&users_mutex);

        printf("[Discovery] Registered: %s @ %s:%d\n", arg1, client_ip, atoi(arg3));
        send_line(client_fd, "OK");
    }

    /* ---- AUTH <username> <password> ---- */
    else if (strcmp(cmd, "AUTH") == 0) {
        pthread_mutex_lock(&users_mutex);
        int idx = find_user_idx(arg1);
        int ok  = (idx >= 0 && strcmp(users[idx].password, arg2) == 0);
        pthread_mutex_unlock(&users_mutex);

        send_line(client_fd, ok ? "OK" : "FAIL");
    }

    /* ---- LOOKUP <username> ---- */
    else if (strcmp(cmd, "LOOKUP") == 0) {
        pthread_mutex_lock(&users_mutex);
        int idx = find_user_idx(arg1);
        if (idx >= 0) {
            send_line(client_fd, "FOUND %s %d",
                      users[idx].ip, users[idx].port);
        } else {
            send_line(client_fd, "NOT_FOUND");
        }
        pthread_mutex_unlock(&users_mutex);
    }

    /* ---- LIST ---- */
    else if (strcmp(cmd, "LIST") == 0) {
        pthread_mutex_lock(&users_mutex);
        for (int i = 0; i < MAX_USERS; i++) {
            if (users[i].active) {
                send_line(client_fd, "%s %s %d",
                          users[i].username,
                          users[i].ip,
                          users[i].port);
            }
        }
        pthread_mutex_unlock(&users_mutex);
        send_line(client_fd, "END");
    }

    /* ---- UNREGISTER <username> ---- */
    else if (strcmp(cmd, "UNREGISTER") == 0) {
        pthread_mutex_lock(&users_mutex);
        int idx = find_user_idx(arg1);
        if (idx >= 0) {
            memset(&users[idx], 0, sizeof(users[idx]));
            user_count--;
            pthread_mutex_unlock(&users_mutex);
            printf("[Discovery] Unregistered: %s\n", arg1);
            send_line(client_fd, "OK");
        } else {
            pthread_mutex_unlock(&users_mutex);
            send_line(client_fd, "FAIL User not found");
        }
    }

    /* ---- Unknown command ---- */
    else {
        send_line(client_fd, "FAIL Unknown command: %s", cmd);
    }

done:
    close(client_fd);
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

    /* Initialise user table */
    memset(users, 0, sizeof(users));

    /* Create server socket */
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family      = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(DISCOVERY_PORT);

    if (bind(server_fd, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) {
        perror("bind"); exit(EXIT_FAILURE);
    }

    if (listen(server_fd, 20) < 0) {
        perror("listen"); exit(EXIT_FAILURE);
    }

    printf("=== Discovery Server ===\n");
    printf("Listening on port %d\n", DISCOVERY_PORT);
    printf("Commands: REGISTER | AUTH | LOOKUP | LIST | UNREGISTER\n\n");

    /* Accept loop: spawn a detached thread per connection */
    while (1) {
        client_addr_len = sizeof(client_addr);
        int *client_fd  = malloc(sizeof(int));
        if (!client_fd) { perror("malloc"); continue; }

        *client_fd = accept(server_fd,
                            (struct sockaddr *)&client_addr,
                            &client_addr_len);
        if (*client_fd < 0) { perror("accept"); free(client_fd); continue; }

        pthread_t tid;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        pthread_create(&tid, &attr, handle_client_thread, client_fd);
        pthread_attr_destroy(&attr);
    }

    close(server_fd);
    return 0;
}