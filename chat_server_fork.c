/* ============================================================================
 * CHAT SERVER – FORK-BASED CONCURRENT SERVER
 * ============================================================================
 * File: chat_server_fork.c
 * Port: 8080
 *
 * Concepts Covered:
 * - fork()          – One child process per connected client
 * - socketpair()    – Bidirectional IPC channel between parent and each child
 * - select()        – Parent multiplexes over server socket + all IPC channels
 * - SIGCHLD + waitpid(WNOHANG) – Reap zombie child processes
 * - User authentication via discovery server (AUTH command)
 * - Broadcast and private messaging via parent as message broker
 *
 * Architecture:
 *
 *   Parent process
 *   ┌─────────────────────────────────────────────────┐
 *   │  select() on:                                   │
 *   │    server_socket    → accept new client → fork  │
 *   │    ipc_pairs[i][0]  → route message from child  │
 *   │                                                  │
 *   │  Writes to any client_socket[i] for broadcast   │
 *   └────────────┬────────────────────────────────────┘
 *                │  socketpair (bidirectional IPC)
 *         ┌──────┴──────┐
 *   Child 0            Child 1  ...  Child N
 *   ┌────────────┐     ┌────────────┐
 *   │ select() on│     │ select() on│
 *   │  client_fd │     │  client_fd │
 *   │  ipc_fd    │     │  ipc_fd    │
 *   │            │     │            │
 *   │ Receives   │     │ Receives   │
 *   │ from client│     │ from client│
 *   │ forwards   │     │ forwards   │
 *   │ to parent  │     │ to parent  │
 *   └────────────┘     └────────────┘
 *
 * IPC message types (child → parent):
 *   REGISTER_USER  – child authenticated, announce username to parent
 *   BROADCAST      – relay message to all other clients
 *   PRIVATE        – relay message to one named client
 *   LIST_REQ       – parent should send user list back to this client
 *   QUIT           – client disconnected
 *
 * IPC message types (parent → child):
 *   MSG_TO_CLIENT  – parent delivering a broadcast/private to this child's client
 *   LIST_RESP      – comma-separated active user list
 *   AUTH_RESULT    – "OK" or "FAIL" (initial login handshake)
 */

#include "common.h"
#include <stdarg.h>
#include <sys/wait.h>
#include <sys/select.h>
#include <signal.h>

/* ---- Per-slot tracking in the parent ---- */
typedef struct {
    int  client_socket;         /* TCP socket to the connected client       */
    int  ipc_parent_fd;         /* Parent end of the socketpair             */
    pid_t pid;                  /* Child PID                                */
    char username[MAX_USERNAME];/* Set after child authenticates            */
    int  authenticated;
} slot_t;

static slot_t slots[MAX_CLIENTS];
static int    slot_count = 0;

/* ============================================================
 * Signal handler: reap zombie children
 * ============================================================ */
static void sigchld_handler(int sig)
{
    (void)sig;
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;
}

/* ============================================================
 * find_slot_by_username (parent only)
 * ============================================================ */
static int find_slot_by_username(const char *name)
{
    for (int i = 0; i < MAX_CLIENTS; i++) {
        if (slots[i].pid > 0 &&
            slots[i].authenticated &&
            strcmp(slots[i].username, name) == 0) {
            return i;
        }
    }
    return -1;
}

/* ============================================================
 * cleanup_slot:
 *   Frees a slot regardless of whether the child exited cleanly
 *   (sent QUIT) or crashed (IPC channel closed unexpectedly).
 *   Broadcasts a "left the chat" notice to all other clients,
 *   closes the IPC and TCP fds, zeroes the slot, decrements
 *   slot_count.
 *
 *   Called from:
 *     - parent_route_ipc_message()  when msg.type == "QUIT"
 *     - parent_route_ipc_message()  when recv() returns <= 0
 *       (child crashed / was killed without sending QUIT)
 * ============================================================ */
static void cleanup_slot(int slot_idx, const char *reason)
{
    printf("[Parent] Slot %d ('%s') removed: %s\n",
           slot_idx, slots[slot_idx].username, reason);

    /* Notify every other authenticated client that this user left */
    if (slots[slot_idx].authenticated) {
        char notice[BUFFER_SIZE];
        snprintf(notice, sizeof(notice),
                 "SERVER: %s left the chat\n", slots[slot_idx].username);

        ipc_msg_t fwd;
        memset(&fwd, 0, sizeof(fwd));
        strncpy(fwd.type, "MSG_TO_CLIENT", 19);
        strncpy(fwd.content, notice, MAX_MSG - 1);

        for (int j = 0; j < MAX_CLIENTS; j++) {
            if (j != slot_idx &&
                slots[j].pid > 0 &&
                slots[j].authenticated) {
                send(slots[j].ipc_parent_fd, &fwd, sizeof(fwd), 0);
            }
        }
    }

    close(slots[slot_idx].ipc_parent_fd);
    close(slots[slot_idx].client_socket);
    memset(&slots[slot_idx], 0, sizeof(slots[slot_idx]));
    slot_count--;
}

/* ============================================================
 * parent_route_ipc_message:
 *   Called when parent's select() fires on ipc_pairs[i][0].
 *   Reads one ipc_msg_t from the IPC channel and acts on it.
 * ============================================================ */
static void parent_route_ipc_message(int slot_idx)
{
    ipc_msg_t msg;
    int n = recv(slots[slot_idx].ipc_parent_fd, &msg, sizeof(msg), MSG_WAITALL);
    if (n <= 0) {
        /*
         * IPC channel closed without the child sending QUIT.
         * This happens when the child process crashes, is killed
         * by a signal, or exits abnormally.  Without this handler
         * the slot would never be freed, slot_count would never
         * be decremented, and the server would eventually report
         * "Server full" even with no real clients connected.
         */
        cleanup_slot(slot_idx, n == 0 ? "child exited (no QUIT)" : "IPC read error");
        return;
    }

    /* ---- REGISTER_USER: child finished auth, record username ---- */
    if (strcmp(msg.type, "REGISTER_USER") == 0) {
        strncpy(slots[slot_idx].username, msg.sender, MAX_USERNAME - 1);
        slots[slot_idx].authenticated = 1;
        printf("[Parent] Slot %d authenticated as '%s'\n",
               slot_idx, slots[slot_idx].username);

        /* Announce arrival to all other authenticated clients */
        char notice[BUFFER_SIZE];
        snprintf(notice, sizeof(notice),
                 "SERVER: %s joined the chat\n", msg.sender);
        for (int j = 0; j < MAX_CLIENTS; j++) {
            if (j != slot_idx &&
                slots[j].pid > 0 &&
                slots[j].authenticated) {
                ipc_msg_t notify;
                memset(&notify, 0, sizeof(notify));
                strncpy(notify.type, "MSG_TO_CLIENT", 19);
                strncpy(notify.content, notice, MAX_MSG - 1);
                send(slots[j].ipc_parent_fd, &notify, sizeof(notify), 0);
            }
        }
    }

    /* ---- BROADCAST: deliver to every other authenticated client ---- */
    else if (strcmp(msg.type, "BROADCAST") == 0) {
        char line[BUFFER_SIZE];
        snprintf(line, sizeof(line),
                 "MSG [broadcast] %s: %s\n", msg.sender, msg.content);
        printf("[Parent] Broadcast from '%s'\n", msg.sender);

        ipc_msg_t fwd;
        memset(&fwd, 0, sizeof(fwd));
        strncpy(fwd.type, "MSG_TO_CLIENT", 19);
        strncpy(fwd.content, line, MAX_MSG - 1);

        for (int j = 0; j < MAX_CLIENTS; j++) {
            if (j != slot_idx &&
                slots[j].pid > 0 &&
                slots[j].authenticated) {
                send(slots[j].ipc_parent_fd, &fwd, sizeof(fwd), 0);
            }
        }
    }

    /* ---- PRIVATE: deliver to one named client ---- */
    else if (strcmp(msg.type, "PRIVATE") == 0) {
        int target = find_slot_by_username(msg.target);
        if (target < 0) {
            /* Target not online – bounce an error back to sender */
            ipc_msg_t err;
            memset(&err, 0, sizeof(err));
            strncpy(err.type, "MSG_TO_CLIENT", 19);
            snprintf(err.content, MAX_MSG,
                     "SERVER: User '%s' is not online\n", msg.target);
            send(slots[slot_idx].ipc_parent_fd, &err, sizeof(err), 0);
        } else {
            char line[BUFFER_SIZE];
            snprintf(line, sizeof(line),
                     "MSG [private] %s: %s\n", msg.sender, msg.content);
            ipc_msg_t fwd;
            memset(&fwd, 0, sizeof(fwd));
            strncpy(fwd.type, "MSG_TO_CLIENT", 19);
            strncpy(fwd.content, line, MAX_MSG - 1);
            send(slots[target].ipc_parent_fd, &fwd, sizeof(fwd), 0);
        }
    }

    /* ---- LIST_REQ: build user list and send back to requesting child ---- */
    else if (strcmp(msg.type, "LIST_REQ") == 0) {
        char list[BUFFER_SIZE] = "USERS ";
        int  first = 1;
        for (int j = 0; j < MAX_CLIENTS; j++) {
            if (slots[j].pid > 0 && slots[j].authenticated) {
                if (!first) strncat(list, ",", sizeof(list) - strlen(list) - 1);
                strncat(list, slots[j].username, sizeof(list) - strlen(list) - 1);
                first = 0;
            }
        }
        strncat(list, "\n", sizeof(list) - strlen(list) - 1);

        ipc_msg_t resp;
        memset(&resp, 0, sizeof(resp));
        strncpy(resp.type, "MSG_TO_CLIENT", 19);
        strncpy(resp.content, list, MAX_MSG - 1);
        send(slots[slot_idx].ipc_parent_fd, &resp, sizeof(resp), 0);
    }

    /* ---- QUIT: child disconnected cleanly ---- */
    else if (strcmp(msg.type, "QUIT") == 0) {
        cleanup_slot(slot_idx, "client sent QUIT");
    }
}

/* ============================================================
 * child_run:  the entire child-process logic.
 *   - Performs login (AUTH via discovery server)
 *   - Then loops: recv from client OR recv from parent via IPC
 * ============================================================ */
static void child_run(int client_fd, int ipc_child_fd,
                      struct sockaddr_in client_addr)
{
    char buf[BUFFER_SIZE];

    printf("[PID %d] Handling client %s:%d\n",
           getpid(),
           inet_ntoa(client_addr.sin_addr),
           ntohs(client_addr.sin_port));

    /* ---- Step 1: Wait for LOGIN command from client ---- */
    char username[MAX_USERNAME] = {0};
    char password[MAX_PASSWORD] = {0};
    int  authenticated = 0;

    while (!authenticated) {
        if (read_line(client_fd, buf, sizeof(buf)) <= 0) goto disconnect;

        char cmd[32];
        sscanf(buf, "%31s %63s %63s", cmd, username, password);

        if (strcmp(cmd, "LOGIN") != 0) {
            send_line(client_fd, "FAIL Please LOGIN first");
            continue;
        }

        /* Verify credentials against discovery server */
        int auth_ok = discovery_auth(username, password);
        if (!auth_ok) {
            send_line(client_fd, "FAIL Invalid username or password");
            continue;
        }

        send_line(client_fd, "OK Welcome %s!", username);
        authenticated = 1;

        /* Notify parent of our username */
        ipc_msg_t reg;
        memset(&reg, 0, sizeof(reg));
        strncpy(reg.type,   "REGISTER_USER", 19);
        strncpy(reg.sender, username, MAX_USERNAME - 1);
        send(ipc_child_fd, &reg, sizeof(reg), 0);
    }

    /* ---- Step 2: Main communication loop ---- */
    fd_set fds;
    int max_fd = (client_fd > ipc_child_fd ? client_fd : ipc_child_fd) + 1;

    while (1) {
        FD_ZERO(&fds);
        FD_SET(client_fd,   &fds);
        FD_SET(ipc_child_fd, &fds);

        if (select(max_fd, &fds, NULL, NULL, NULL) < 0) {
            if (errno == EINTR) continue;
            break;
        }

        /* --- Data from the connected client --- */
        if (FD_ISSET(client_fd, &fds)) {
            int n = read_line(client_fd, buf, sizeof(buf));
            if (n <= 0) goto disconnect;

            char cmd[32], arg1[MAX_USERNAME];
            cmd[0] = arg1[0] = '\0';
            sscanf(buf, "%31s %63s", cmd, arg1);
            /* message body starts after "CMD arg1 " */
            char *body = buf;
            {
                int spaces = 0;
                while (*body && spaces < 2) {
                    if (*body++ == ' ') spaces++;
                }
            }

            ipc_msg_t out;
            memset(&out, 0, sizeof(out));
            strncpy(out.sender, username, MAX_USERNAME - 1);

            if (strcmp(cmd, "BROADCAST") == 0) {
                strncpy(out.type, "BROADCAST", 19);
                /* body of BROADCAST is everything after "BROADCAST " */
                char *msg_body = buf;
                while (*msg_body && *msg_body != ' ') msg_body++;
                if (*msg_body == ' ') msg_body++;
                strncpy(out.content, msg_body, MAX_MSG - 1);
                send(ipc_child_fd, &out, sizeof(out), 0);
            }
            else if (strcmp(cmd, "PRIVATE") == 0) {
                strncpy(out.type,   "PRIVATE", 19);
                strncpy(out.target, arg1, MAX_USERNAME - 1);
                strncpy(out.content, body, MAX_MSG - 1);
                send(ipc_child_fd, &out, sizeof(out), 0);
            }
            else if (strcmp(cmd, "LIST") == 0) {
                strncpy(out.type, "LIST_REQ", 19);
                send(ipc_child_fd, &out, sizeof(out), 0);
            }
            else if (strcmp(cmd, "QUIT") == 0) {
                goto disconnect;
            }
            else {
                send_line(client_fd,
                          "FAIL Unknown command. Use: BROADCAST | PRIVATE | LIST | QUIT");
            }
        }

        /* --- Message forwarded from parent (broadcast/private/list) --- */
        if (FD_ISSET(ipc_child_fd, &fds)) {
            ipc_msg_t in;
            int n = recv(ipc_child_fd, &in, sizeof(in), MSG_WAITALL);
            if (n <= 0) break;

            if (strcmp(in.type, "MSG_TO_CLIENT") == 0) {
                /* content already has trailing \n */
                send(client_fd, in.content, strlen(in.content), 0);
            }
        }
    }

disconnect:
    /* Tell parent we are done */
    {
        ipc_msg_t bye;
        memset(&bye, 0, sizeof(bye));
        strncpy(bye.type,   "QUIT", 19);
        strncpy(bye.sender, username, MAX_USERNAME - 1);
        send(ipc_child_fd, &bye, sizeof(bye), 0);
    }
    printf("[PID %d] Client '%s' disconnected\n", getpid(), username);
    close(client_fd);
    close(ipc_child_fd);
    exit(0);
}

/* ============================================================
 * main
 * ============================================================ */
int main(void)
{
    int server_fd;
    struct sockaddr_in server_addr, client_addr;
    socklen_t client_addr_len;

    /* Initialise slots */
    memset(slots, 0, sizeof(slots));

    /* SIGCHLD handler to reap zombies */
    struct sigaction sa;
    sa.sa_handler = sigchld_handler;
    sigemptyset(&sa.sa_mask);
    sa.sa_flags = SA_RESTART;
    sigaction(SIGCHLD, &sa, NULL);

    /* Create and bind server socket */
    server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) { perror("socket"); exit(EXIT_FAILURE); }

    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family      = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port        = htons(CHAT_PORT_FORK);

    if (bind(server_fd, (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0) { perror("bind"); exit(EXIT_FAILURE); }

    if (listen(server_fd, 10) < 0) { perror("listen"); exit(EXIT_FAILURE); }

    printf("=== Fork-Based Chat Server ===\n");
    printf("Listening on port %d\n", CHAT_PORT_FORK);
    printf("Each client handled by a separate child process\n");
    printf("Broadcast/private messages routed through parent via socketpair IPC\n\n");

    /* ---- Parent select loop ---- */
    while (1) {
        fd_set read_fds;
        FD_ZERO(&read_fds);
        FD_SET(server_fd, &read_fds);
        int max_fd = server_fd;

        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (slots[i].pid > 0) {
                FD_SET(slots[i].ipc_parent_fd, &read_fds);
                if (slots[i].ipc_parent_fd > max_fd)
                    max_fd = slots[i].ipc_parent_fd;
            }
        }

        int activity = select(max_fd + 1, &read_fds, NULL, NULL, NULL);
        if (activity < 0) {
            if (errno == EINTR) continue;  /* interrupted by SIGCHLD – retry */
            perror("select");
            continue;
        }

        /* ---- New connection ---- */
        if (FD_ISSET(server_fd, &read_fds)) {
            client_addr_len = sizeof(client_addr);
            int client_fd   = accept(server_fd,
                                     (struct sockaddr *)&client_addr,
                                     &client_addr_len);
            if (client_fd < 0) { perror("accept"); continue; }

            if (slot_count >= MAX_CLIENTS) {
                send_line(client_fd, "FAIL Server full (max %d clients)", MAX_CLIENTS);
                close(client_fd);
                continue;
            }

            /* Find a free slot */
            int slot = -1;
            for (int i = 0; i < MAX_CLIENTS; i++) {
                if (slots[i].pid == 0) { slot = i; break; }
            }

            /* Create the IPC socketpair BEFORE forking */
            int ipc_pair[2];
            if (socketpair(AF_UNIX, SOCK_STREAM, 0, ipc_pair) < 0) {
                perror("socketpair");
                close(client_fd);
                continue;
            }

            pid_t pid = fork();
            if (pid < 0) {
                perror("fork");
                close(client_fd);
                close(ipc_pair[0]);
                close(ipc_pair[1]);
                continue;
            }

            if (pid == 0) {
                /* --- Child process --- */
                close(server_fd);          /* child doesn't accept new clients */
                close(ipc_pair[0]);        /* child uses ipc_pair[1]           */

                /* Close all other IPC fds inherited from parent */
                for (int i = 0; i < MAX_CLIENTS; i++) {
                    if (slots[i].pid > 0)
                        close(slots[i].ipc_parent_fd);
                }

                child_run(client_fd, ipc_pair[1], client_addr);
                /* child_run() calls exit() – never returns */
            }

            /* --- Parent process --- */
            close(ipc_pair[1]);           /* parent uses ipc_pair[0]          */
            /* Parent KEEPS client_fd open (needed for broadcasts in future)  */
            /* Actually parent doesn't write directly – it uses IPC channel   */
            /* but keep it open so the kernel connection stays alive           */

            slots[slot].client_socket  = client_fd;
            slots[slot].ipc_parent_fd  = ipc_pair[0];
            slots[slot].pid            = pid;
            slots[slot].authenticated  = 0;
            slot_count++;

            printf("[Parent] Accepted client %s:%d → slot %d, PID %d\n",
                   inet_ntoa(client_addr.sin_addr),
                   ntohs(client_addr.sin_port),
                   slot, pid);
        }

        /* ---- IPC message from a child ---- */
        for (int i = 0; i < MAX_CLIENTS; i++) {
            if (slots[i].pid > 0 &&
                FD_ISSET(slots[i].ipc_parent_fd, &read_fds)) {
                parent_route_ipc_message(i);
            }
        }
    }

    close(server_fd);
    return 0;
}