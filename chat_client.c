/* ============================================================================
 * CHAT CLIENT – INTERACTIVE COMMAND-LINE CLIENT
 * ============================================================================
 * File: chat_client.c
 *
 * Concepts Covered:
 * - Two-thread design:
 *     recv_thread  – continuously reads from server socket, prints to stdout
 *     main thread  – reads user input from stdin, sends commands to server
 * - Discovery server registration on startup
 * - Connection to a chosen chat server with username/password
 * - Graceful disconnection (QUIT command or Ctrl-C / EOF)
 *
 * Usage:
 *   ./chat_client [fork|thread|select]   (default: thread)
 *
 * After connecting, supported commands:
 *   BROADCAST <message>          Send to all connected users
 *   PRIVATE <username> <message> Send to one specific user
 *   LIST                         Show online users
 *   QUIT                         Disconnect gracefully
 *   (anything else)              Treated as a broadcast message
 *
 * Startup flow:
 *   1. Prompt for username, password, and local port
 *   2. Register with discovery server  (REGISTER username password port)
 *   3. Connect to the chosen chat server
 *   4. Send LOGIN username password
 *   5. Wait for OK, then enter interactive mode
 */

#include "common.h"
#include <pthread.h>
#include <stdarg.h>
#include <signal.h>

static int server_fd = -1;      /* Socket connected to chat server */
static volatile int running = 1;

/* ============================================================
 * recv_thread: continuously print messages from the server.
 * Exits when the server closes the connection or running == 0.
 * ============================================================ */
static void *recv_thread(void *arg)
{
    (void)arg;
    char buf[BUFFER_SIZE];

    while (running) {
        int n = read_line(server_fd, buf, sizeof(buf));
        if (n <= 0) {
            if (running)
                printf("\n[!] Server disconnected.\n");
            running = 0;
            break;
        }
        /* Pretty-print different message types */
        if (strncmp(buf, "MSG [broadcast]", 15) == 0) {
            printf("\r\033[K[#] %s\n> ", buf + 4);   /* strip "MSG " prefix */
        } else if (strncmp(buf, "MSG [private]", 13) == 0) {
            printf("\r\033[K[PM] %s\n> ", buf + 4);
        } else if (strncmp(buf, "SERVER:", 7) == 0) {
            printf("\r\033[K*** %s ***\n> ", buf + 8);
        } else {
            printf("\r\033[K%s\n> ", buf);
        }
        fflush(stdout);
    }
    return NULL;
}

/* ============================================================
 * register_with_discovery: send REGISTER to discovery server.
 * Returns 1 on success, 0 on failure.
 * ============================================================ */
static int register_with_discovery(const char *username,
                                   const char *password,
                                   int         local_port)
{
    int fd = tcp_connect(DISCOVERY_IP, DISCOVERY_PORT);
    if (fd < 0) {
        printf("[!] Cannot reach discovery server at %s:%d\n",
               DISCOVERY_IP, DISCOVERY_PORT);
        return 0;
    }

    send_line(fd, "REGISTER %s %s %d", username, password, local_port);

    char resp[BUFFER_SIZE];
    int n = read_line(fd, resp, sizeof(resp));
    close(fd);

    if (n <= 0 || strncmp(resp, "OK", 2) != 0) {
        printf("[!] Discovery registration failed: %s\n", resp);
        return 0;
    }
    printf("[*] Registered with discovery server as '%s'\n", username);
    return 1;
}

/* ============================================================
 * unregister_from_discovery
 * ============================================================ */
static void unregister_from_discovery(const char *username)
{
    int fd = tcp_connect(DISCOVERY_IP, DISCOVERY_PORT);
    if (fd < 0) return;
    send_line(fd, "UNREGISTER %s", username);
    char resp[BUFFER_SIZE];
    read_line(fd, resp, sizeof(resp));
    close(fd);
    printf("[*] Unregistered '%s' from discovery server\n", username);
}

/* ============================================================
 * print_help: show available commands
 * ============================================================ */
static void print_help(void)
{
    printf("Commands:\n");
    printf("  BROADCAST <message>           Send to all users\n");
    printf("  PRIVATE <username> <message>  Send to one user\n");
    printf("  LIST                          Show online users\n");
    printf("  QUIT                          Disconnect\n");
    printf("  HELP                          Show this help\n");
    printf("  (or just type text)           Treated as broadcast\n\n");
}

/* ============================================================
 * main
 * ============================================================ */
int main(int argc, char *argv[])
{
    /* ---- Determine which server to connect to ---- */
    int chat_port = CHAT_PORT_THREAD;    /* default: thread-based */
    const char *server_type = "thread";

    if (argc >= 2) {
        if (strcmp(argv[1], "fork") == 0) {
            chat_port   = CHAT_PORT_FORK;
            server_type = "fork";
        } else if (strcmp(argv[1], "select") == 0) {
            chat_port   = CHAT_PORT_SELECT;
            server_type = "select";
        } else if (strcmp(argv[1], "thread") == 0) {
            chat_port   = CHAT_PORT_THREAD;
            server_type = "thread";
        } else {
            /* treat as raw port number */
            chat_port   = atoi(argv[1]);
            server_type = "custom";
        }
    }

    printf("==============================================\n");
    printf("  Multi-Client Chat System – CLI Client\n");
    printf("  Server type : %s (port %d)\n", server_type, chat_port);
    printf("  Discovery   : %s:%d\n", DISCOVERY_IP, DISCOVERY_PORT);
    printf("==============================================\n\n");

    /* ---- Collect credentials ---- */
    char username[MAX_USERNAME];
    char password[MAX_PASSWORD];
    int  local_port;

    printf("Username : "); fflush(stdout);
    if (!fgets(username, sizeof(username), stdin)) return 1;
    username[strcspn(username, "\r\n")] = '\0';

    printf("Password : "); fflush(stdout);
    if (!fgets(password, sizeof(password), stdin)) return 1;
    password[strcspn(password, "\r\n")] = '\0';

    printf("Local port (for discovery, e.g. 5000): "); fflush(stdout);
    char port_buf[16];
    if (!fgets(port_buf, sizeof(port_buf), stdin)) return 1;
    local_port = atoi(port_buf);
    if (local_port <= 0) local_port = 5000;

    /* ---- Step 1: Register with discovery server ---- */
    if (!register_with_discovery(username, password, local_port)) {
        /* Not fatal – server may allow re-registration; try to continue */
        printf("[!] Proceeding without discovery registration…\n");
    }

    /* ---- Step 2: Connect to chat server ---- */
    printf("[*] Connecting to chat server at %s:%d …\n",
           DISCOVERY_IP, chat_port);

    server_fd = tcp_connect(DISCOVERY_IP, chat_port);
    if (server_fd < 0) {
        perror("[!] connect to chat server");
        return 1;
    }
    printf("[*] Connected to chat server\n");

    /* ---- Step 3: Send LOGIN ---- */
    send_line(server_fd, "LOGIN %s %s", username, password);

    char resp[BUFFER_SIZE];
    if (read_line(server_fd, resp, sizeof(resp)) <= 0) {
        printf("[!] Server closed connection during login\n");
        close(server_fd);
        return 1;
    }

    if (strncmp(resp, "OK", 2) != 0) {
        printf("[!] Login failed: %s\n", resp);
        close(server_fd);
        return 1;
    }
    printf("[*] %s\n\n", resp);

    /* ---- Step 4: Start receive thread ---- */
    pthread_t tid;
    pthread_create(&tid, NULL, recv_thread, NULL);
    pthread_detach(tid);

    /* ---- Step 5: Interactive send loop ---- */
    print_help();
    printf("> "); fflush(stdout);

    char line[BUFFER_SIZE];
    while (running && fgets(line, sizeof(line), stdin)) {
        line[strcspn(line, "\r\n")] = '\0';

        if (strlen(line) == 0) {
            printf("> "); fflush(stdout);
            continue;
        }

        char cmd[32];
        sscanf(line, "%31s", cmd);

        /* Convert lower-case convenience aliases */
        if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "QUIT") == 0) {
            send_line(server_fd, "QUIT");
            running = 0;
            break;
        }
        else if (strcmp(cmd, "help") == 0 || strcmp(cmd, "HELP") == 0) {
            print_help();
        }
        else if (strcmp(cmd, "list") == 0 || strcmp(cmd, "LIST") == 0) {
            send_line(server_fd, "LIST");
        }
        else if (strncasecmp(cmd, "BROADCAST", 9) == 0 ||
                 strncasecmp(cmd, "broadcast", 9) == 0) {
            send_line(server_fd, "%s", line);
        }
        else if (strncasecmp(cmd, "PRIVATE", 7) == 0 ||
                 strncasecmp(cmd, "private", 7) == 0) {
            send_line(server_fd, "%s", line);
        }
        else {
            /* Plain text → treat as broadcast */
            send_line(server_fd, "BROADCAST %s", line);
        }

        if (running) { printf("> "); fflush(stdout); }
    }

    /* ---- Step 6: Graceful teardown ---- */
    printf("\n[*] Disconnecting…\n");
    close(server_fd);
    unregister_from_discovery(username);
    printf("[*] Goodbye, %s!\n", username);
    return 0;
}