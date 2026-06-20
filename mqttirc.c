#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <signal.h>
#include <winsock2.h>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

#define MAX_NICK 32
#define MAX_TOPIC 128
#define MAX_LINE 1024
#define MAX_CHANNELS 32
#define BUFFER_SIZE 4096
#define PORT 6667

static volatile int running = 1;
static SOCKET sock = INVALID_SOCKET;
static CRITICAL_SECTION cs;
static char nick[MAX_NICK] = {0};
static char channels[MAX_CHANNELS][MAX_TOPIC] = {{0}};
static int channel_count = 0;
static char host[256] = {0};
static int port = PORT;

static void irc_send(const char *fmt, ...) {
    char buf[MAX_LINE];
    va_list ap;
    va_start(ap, fmt);
    int len = vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    if (len < 0 || len >= (int)sizeof(buf)) return;
    send(sock, buf, len, 0);
}

static void display(const char *fmt, ...) {
    char buf[MAX_LINE];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    printf("%s\n", buf);
    fflush(stdout);
}

static void handle_server_msg(const char *line) {
    char prefix[256] = {0};
    char cmd[32] = {0};
    char params[MAX_LINE] = {0};

    const char *p = line;
    if (*p == ':') {
        p++;
        const char *sp = strchr(p, ' ');
        if (!sp) return;
        size_t len = sp - p;
        if (len >= sizeof(prefix)) len = sizeof(prefix) - 1;
        memcpy(prefix, p, len);
        prefix[len] = '\0';
        p = sp + 1;
    }

    const char *sp = strchr(p, ' ');
    if (sp) {
        size_t len = sp - p;
        if (len >= sizeof(cmd)) len = sizeof(cmd) - 1;
        memcpy(cmd, p, len);
        cmd[len] = '\0';
        p = sp + 1;
    } else {
        strncpy(cmd, p, sizeof(cmd) - 1);
        p = "";
    }

    strncpy(params, p, sizeof(params) - 1);

    if (strcmp(cmd, "PING") == 0) {
        const char *srv = (*params == ':') ? params + 1 : params;
        irc_send("PONG :%s\r\n", srv);
        return;
    }

    if (strcmp(cmd, "PRIVMSG") == 0) {
        char sender[64] = {0};
        const char *ex = strchr(prefix, '!');
        size_t slen = ex ? (size_t)(ex - prefix) : strlen(prefix);
        if (slen >= sizeof(sender)) slen = sizeof(sender) - 1;
        memcpy(sender, prefix, slen);
        sender[slen] = '\0';

        char target[128] = {0}, msg[MAX_LINE] = {0};
        if (*params == ':') {
            strncpy(msg, params + 1, sizeof(msg) - 1);
        } else {
            const char *col = strchr(params, ':');
            if (col) {
                size_t tlen = col - params;
                while (tlen > 0 && params[tlen - 1] == ' ') tlen--;
                if (tlen >= sizeof(target)) tlen = sizeof(target) - 1;
                memcpy(target, params, tlen);
                target[tlen] = '\0';
                strncpy(msg, col + 1, sizeof(msg) - 1);
            } else {
                strncpy(target, params, sizeof(target) - 1);
            }
        }

        if (target[0] == '#')
            display("[#%s] <%s> %s", target + 1, sender, msg);
        else if (target[0])
            display("[PM from %s] %s", sender, msg);
        return;
    }

    if (strcmp(cmd, "JOIN") == 0) {
        char sender[64] = {0};
        const char *ex = strchr(prefix, '!');
        size_t slen = ex ? (size_t)(ex - prefix) : strlen(prefix);
        if (slen >= sizeof(sender)) slen = sizeof(sender) - 1;
        memcpy(sender, prefix, slen);
        sender[slen] = '\0';

        if (strcmp(sender, nick) == 0) return;

        char chan[128] = {0};
        if (*params == ':') {
            strncpy(chan, params + 1, sizeof(chan) - 1);
        } else {
            const char *sp2 = strchr(params, ' ');
            if (sp2) {
                size_t clen = sp2 - params;
                if (clen >= sizeof(chan)) clen = sizeof(chan) - 1;
                memcpy(chan, params, clen);
                chan[clen] = '\0';
            } else {
                strncpy(chan, params, sizeof(chan) - 1);
            }
        }
        if (chan[0] == '#')
            display("[#%s] %s joined", chan + 1, sender);
        return;
    }

    if (strcmp(cmd, "PART") == 0) {
        char sender[64] = {0};
        const char *ex = strchr(prefix, '!');
        size_t slen = ex ? (size_t)(ex - prefix) : strlen(prefix);
        if (slen >= sizeof(sender)) slen = sizeof(sender) - 1;
        memcpy(sender, prefix, slen);
        sender[slen] = '\0';

        if (strcmp(sender, nick) == 0) return;

        char chan[128] = {0};
        const char *csp = strchr(params, ' ');
        if (csp) {
            size_t clen = csp - params;
            if (clen >= sizeof(chan)) clen = sizeof(chan) - 1;
            memcpy(chan, params, clen);
            chan[clen] = '\0';
        } else {
            const char *pc = (*params == ':') ? params + 1 : params;
            strncpy(chan, pc, sizeof(chan) - 1);
        }
        if (chan[0] == '#')
            display("[#%s] %s left", chan + 1, sender);
        return;
    }

    if (strcmp(cmd, "QUIT") == 0) {
        char sender[64] = {0};
        const char *ex = strchr(prefix, '!');
        size_t slen = ex ? (size_t)(ex - prefix) : strlen(prefix);
        if (slen >= sizeof(sender)) slen = sizeof(sender) - 1;
        memcpy(sender, prefix, slen);
        sender[slen] = '\0';
        display("[%s quit]", sender);
        return;
    }

    if (strcmp(cmd, "NOTICE") == 0) {
        char msg[MAX_LINE] = {0};
        const char *col = strchr(params, ':');
        if (col) {
            strncpy(msg, col + 1, sizeof(msg) - 1);
        } else if (*params) {
            strncpy(msg, params, sizeof(msg) - 1);
        }
        if (*msg) display("[-] %s", msg);
        return;
    }

    if (strcmp(cmd, "MODE") == 0) {
        char target[128] = {0}, modestr[128] = {0};
        if (sscanf(params, "%127s %127s", target, modestr) >= 2) {
            if (target[0] == '#')
                display("[#%s] mode %s", target + 1, modestr);
        }
        return;
    }

    if (strcmp(cmd, "TOPIC") == 0) {
        char target[128] = {0}, topic[MAX_LINE] = {0};
        const char *col = strchr(params, ':');
        if (col) {
            size_t tlen = col - params;
            while (tlen > 0 && params[tlen - 1] == ' ') tlen--;
            if (tlen >= sizeof(target)) tlen = sizeof(target) - 1;
            memcpy(target, params, tlen);
            target[tlen] = '\0';
            strncpy(topic, col + 1, sizeof(topic) - 1);
            if (target[0] == '#')
                display("[#%s] topic: %s", target + 1, topic);
        }
        return;
    }

    if (strcmp(cmd, "KICK") == 0) {
        char sender[64] = {0};
        const char *ex = strchr(prefix, '!');
        size_t slen = ex ? (size_t)(ex - prefix) : strlen(prefix);
        if (slen >= sizeof(sender)) slen = sizeof(sender) - 1;
        memcpy(sender, prefix, slen);
        sender[slen] = '\0';

        char target[128] = {0}, kicked[64] = {0}, reason[MAX_LINE] = {0};
        const char *col = strchr(params, ':');
        if (col) {
            size_t midlen = col - params;
            char mid[256];
            if (midlen >= sizeof(mid)) midlen = sizeof(mid) - 1;
            memcpy(mid, params, midlen);
            mid[midlen] = '\0';
            sscanf(mid, "%127s %63s", target, kicked);
            strncpy(reason, col + 1, sizeof(reason) - 1);
        }
        if (target[0] == '#')
            display("[#%s] %s was kicked by %s (%s)", target + 1, kicked[0] ? kicked : "?", sender, reason);
        return;
    }
}

static void join_channel(const char *name) {
    char chan[MAX_TOPIC];
    snprintf(chan, sizeof(chan), "%s", name[0] == '#' ? name + 1 : name);

    EnterCriticalSection(&cs);
    for (int i = 0; i < channel_count; i++) {
        if (strcmp(channels[i], chan) == 0) {
            LeaveCriticalSection(&cs);
            return;
        }
    }
    if (channel_count >= MAX_CHANNELS) {
        LeaveCriticalSection(&cs);
        display("[max channels reached]");
        return;
    }
    strncpy(channels[channel_count], chan, sizeof(channels[0]) - 1);
    channel_count++;
    LeaveCriticalSection(&cs);

    irc_send("JOIN #%s\r\n", chan);
    display("[joined #%s]", chan);
}

static void part_channel(const char *name) {
    char chan[MAX_TOPIC];
    snprintf(chan, sizeof(chan), "%s", name[0] == '#' ? name + 1 : name);

    EnterCriticalSection(&cs);
    int found = 0;
    for (int i = 0; i < channel_count; i++) {
        if (strcmp(channels[i], chan) == 0) {
            memmove(&channels[i], &channels[i + 1],
                    (channel_count - i - 1) * sizeof(channels[0]));
            channel_count--;
            found = 1;
            break;
        }
    }
    LeaveCriticalSection(&cs);

    if (found) {
        irc_send("PART #%s\r\n", chan);
        display("[left #%s]", chan);
    } else {
        display("[not in #%s]", chan);
    }
}

static void send_channel_msg(const char *chan, const char *text) {
    irc_send("PRIVMSG #%s :%s\r\n", chan, text);
    display("[#%s] <%s> %s", chan, nick, text);
}

static void send_private_msg(const char *target, const char *text) {
    irc_send("PRIVMSG %s :%s\r\n", target, text);
    display("[PM to %s] %s", target, text);
}

static void list_channels(void) {
    EnterCriticalSection(&cs);
    if (channel_count == 0) {
        display("[no channels joined]");
    } else {
        printf("[channels:");
        for (int i = 0; i < channel_count; i++)
            printf(" #%s", channels[i]);
        printf("]\n");
        fflush(stdout);
    }
    LeaveCriticalSection(&cs);
}

static void process_line(const char *line) {
    if (line[0] != '/') {
        if (channel_count > 0) {
            EnterCriticalSection(&cs);
            char buf[MAX_TOPIC];
            strncpy(buf, channels[0], sizeof(buf) - 1);
            LeaveCriticalSection(&cs);
            send_channel_msg(buf, line);
        } else {
            display("[join a channel first: /join <name>]");
        }
        return;
    }

    char cmd[MAX_LINE], arg[MAX_LINE], arg2[MAX_LINE];
    arg[0] = arg2[0] = '\0';

    if (sscanf(line + 1, "%s %[^\n]", cmd, arg) < 1)
        return;

    if (strcmp(cmd, "join") == 0) {
        join_channel(arg);
    } else if (strcmp(cmd, "part") == 0 || strcmp(cmd, "leave") == 0) {
        if (arg[0] == '\0' && channel_count > 0) {
            EnterCriticalSection(&cs);
            char buf[MAX_TOPIC];
            strncpy(buf, channels[0], sizeof(buf) - 1);
            LeaveCriticalSection(&cs);
            part_channel(buf);
        } else {
            part_channel(arg);
        }
    } else if (strcmp(cmd, "nick") == 0) {
        strncpy(nick, arg, sizeof(nick) - 1);
        nick[sizeof(nick) - 1] = '\0';
        irc_send("NICK %s\r\n", nick);
        display("[nick set to %s]", nick);
    } else if (strcmp(cmd, "msg") == 0) {
        if (sscanf(line + 1, "msg %s %[^\n]", arg, arg2) >= 2)
            send_private_msg(arg, arg2);
        else
            display("[usage: /msg <nick> <text>]");
    } else if (strcmp(cmd, "list") == 0) {
        list_channels();
    } else if (strcmp(cmd, "clear") == 0) {
        printf("\033[2J\033[H");
        fflush(stdout);
    } else if (strcmp(cmd, "quit") == 0 || strcmp(cmd, "exit") == 0) {
        irc_send("QUIT :Client quit\r\n");
        shutdown(sock, SD_BOTH);
        running = 0;
    } else if (strcmp(cmd, "help") == 0) {
        printf(
            "Commands:\n"
            "  /join <chan>       - join a channel\n"
            "  /part [<chan>]     - leave a channel\n"
            "  /msg <nick> <txt>  - send private message\n"
            "  /nick <name>       - change nickname\n"
            "  /list              - list joined channels\n"
            "  /clear             - clear screen\n"
            "  /quit              - disconnect and exit\n"
            "  /help              - this help\n"
            "Plain text sends to the first joined channel.\n");
        fflush(stdout);
    } else {
        display("[unknown: /%s]", cmd);
    }
}

static DWORD WINAPI receiver_thread(LPVOID param) {
    (void)param;
    char recv_buf[BUFFER_SIZE];
    int recv_len = 0;

    while (running) {
        int space = (int)sizeof(recv_buf) - recv_len - 1;
        if (space <= 0) {
            recv_len = 0;
            space = (int)sizeof(recv_buf) - 1;
        }
        int n = recv(sock, recv_buf + recv_len, space, 0);
        if (n <= 0) {
            if (running) {
                display("[connection lost]");
                running = 0;
            }
            break;
        }
        recv_len += n;
        recv_buf[recv_len] = '\0';

        char *p = recv_buf;
        while (*p) {
            char *crlf = strstr(p, "\r\n");
            if (!crlf) break;
            *crlf = '\0';
            char *line = p;
            p = crlf + 2;

            if (*line)
                handle_server_msg(line);
        }

        if (p > recv_buf) {
            recv_len -= (int)(p - recv_buf);
            if (recv_len > 0)
                memmove(recv_buf, p, recv_len);
            else
                recv_len = 0;
        }
    }
    return 0;
}

static void sigint(int sig) {
    (void)sig;
    irc_send("QUIT :Client quit\r\n");
    shutdown(sock, SD_BOTH);
    running = 0;
}

int main(int argc, char **argv) {
    const char *host_arg = "127.0.0.1";
    int port_arg = PORT;

    if (argc > 1) host_arg = argv[1];
    if (argc > 2) port_arg = atoi(argv[2]);

    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        fprintf(stderr, "WSAStartup failed\n");
        return 1;
    }

    InitializeCriticalSection(&cs);

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (sock == INVALID_SOCKET) {
        fprintf(stderr, "socket() failed: %d\n", WSAGetLastError());
        DeleteCriticalSection(&cs);
        WSACleanup();
        return 1;
    }

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(host_arg);
    addr.sin_port = htons((unsigned short)port_arg);

    if (connect(sock, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        fprintf(stderr, "connect to %s:%d failed: %d\n", host_arg, port_arg, WSAGetLastError());
        closesocket(sock);
        DeleteCriticalSection(&cs);
        WSACleanup();
        return 1;
    }

    strncpy(host, host_arg, sizeof(host) - 1);
    port = port_arg;

    srand((unsigned)time(NULL));
    snprintf(nick, sizeof(nick), "user%d", rand() % 10000);

    signal(SIGINT, sigint);

    // Register with IRC server
    irc_send("NICK %s\r\n", nick);
    irc_send("USER %s 0 * :%s\r\n", nick, nick);

    printf("--- MQTT IRC ---\n");
    printf("server:  %s:%d\n", host_arg, port_arg);
    printf("nick:    %s\n", nick);
    printf("type /help for commands\n\n");

    HANDLE recv_h = CreateThread(NULL, 0, receiver_thread, NULL, 0, NULL);
    if (!recv_h) {
        fprintf(stderr, "CreateThread failed\n");
        closesocket(sock);
        DeleteCriticalSection(&cs);
        WSACleanup();
        return 1;
    }
    CloseHandle(recv_h);

    while (running) {
        char input[MAX_LINE];
        if (!fgets(input, sizeof(input), stdin)) break;
        input[strcspn(input, "\r\n")] = '\0';
        if (input[0]) process_line(input);
    }

    printf("\n[shutting down...]\n");
    closesocket(sock);
    DeleteCriticalSection(&cs);
    WSACleanup();
    return 0;
}
