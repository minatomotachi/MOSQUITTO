// mqtt-irc-server.c - IRCv3 server with MQTT-style publish/subscribe messaging
// aligned to https://modern.ircdocs.horse/
// Compile: gcc mqtt-irc-server.c -o mqtt-irc-server.exe -lws2_32

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdarg.h>
#include <time.h>
#include <winsock2.h>
#include <windows.h>

#pragma comment(lib, "ws2_32.lib")

#define PORT 6667
#define MAX_CLIENTS 64
#define BUFFER_SIZE 4096
#define NICK_LEN 32
#define TOPIC_LEN 128
#define MAX_SUBSCRIBERS 64
#define MAX_TOPICS 64
#define HOSTNAME "mqtt-irc.local"
#define NETWORK "MQTT-IRC"
#define VERSION "mqtt-irc-1.0.0"

typedef struct {
    SOCKET sock;
    char nick[NICK_LEN];
} Subscriber;

typedef struct {
    char name[TOPIC_LEN];
    char topic[TOPIC_LEN];
    int topic_set;
    time_t topic_time;
    char topic_who[NICK_LEN];
    Subscriber subscribers[MAX_SUBSCRIBERS];
    int sub_count;
} Topic;

typedef struct {
    SOCKET sock;
    char nick[NICK_LEN];
    char user[NICK_LEN];
    char real[128];
    struct sockaddr_in addr;
    int cap_echo_message;
    int cap_server_time;
    int cap_message_tags;
    int cap_extended_join;
    int cap_multi_prefix;
    int cap_account_tag;
    int cap_userhost_in_names;
    int registered;
    int away;
    char away_msg[256];
    char account[32];
    time_t signon;
    time_t idle;
} Client;

static Topic topics[MAX_TOPICS];
static int topic_count = 0;
static Client clients[MAX_CLIENTS];
static int client_count = 0;
static CRITICAL_SECTION cs;
static SOCKET server_sock = INVALID_SOCKET;
static volatile int running = 1;
static volatile LONG next_msg_id = 1;

static const char* ircv3_caps[] = {
    "echo-message",
    "server-time",
    "message-tags",
    "extended-join",
    "multi-prefix",
    "userhost-in-names",
    "account-tag",
    NULL
};

static void send_to(SOCKET sock, const char* fmt, ...) {
    char buf[BUFFER_SIZE];
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(buf, sizeof(buf), fmt, args);
    va_end(args);
    if (len > 0) send(sock, buf, len, 0);
}

static char* get_timestamp(void) {
    static char ts[32];
    time_t now = time(NULL);
    struct tm* t = gmtime(&now);
    strftime(ts, sizeof(ts), "%Y-%m-%dT%H:%M:%S.000Z", t);
    return ts;
}

static void send_with_tags(SOCKET sock, Client* c, const char* tag_extra, const char* fmt, ...) {
    char tag_buf[1024] = {0};
    int pos = 0;
    if (c && c->cap_message_tags) {
        int left = (int)sizeof(tag_buf) - 1;
        if (tag_extra && tag_extra[0]) {
            int n = snprintf(tag_buf + pos, left, "%s", tag_extra);
            if (n > 0) { pos += n; left -= n; }
        }
        if (c->cap_server_time) {
            if (pos > 0 && pos < (int)sizeof(tag_buf) - 1) tag_buf[pos++] = ';';
            int n = snprintf(tag_buf + pos, left, "time=%s", get_timestamp());
            if (n > 0) { pos += n; left -= n; }
        }
    }
    char msg[BUFFER_SIZE];
    va_list args;
    va_start(args, fmt);
    int body_len = vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);
    if (body_len <= 0) return;
    char full[BUFFER_SIZE + 2048];
    int len;
    if (tag_buf[0]) {
        len = snprintf(full, sizeof(full), "@%s %s", tag_buf, msg);
    } else {
        len = snprintf(full, sizeof(full), "%s", msg);
    }
    send(sock, full, len, 0);
}

static void send_with_msgid(SOCKET sock, Client* c, const char* fmt, ...) {
    char tag_extra[64];
    int mid = InterlockedIncrement(&next_msg_id) - 1;
    if (c && c->cap_message_tags) {
        snprintf(tag_extra, sizeof(tag_extra), "msgid=%d", mid);
    } else {
        tag_extra[0] = '\0';
    }
    char msg[BUFFER_SIZE];
    va_list args;
    va_start(args, fmt);
    int body_len = vsnprintf(msg, sizeof(msg), fmt, args);
    va_end(args);
    if (body_len <= 0) return;
    send_with_tags(sock, c, tag_extra, "%s", msg);
}

static Topic* get_topic(const char* name) {
    for (int i = 0; i < topic_count; i++)
        if (_stricmp(topics[i].name, name) == 0)
            return &topics[i];
    if (topic_count >= MAX_TOPICS) return NULL;
    Topic* t = &topics[topic_count++];
    strncpy(t->name, name, TOPIC_LEN - 1);
    t->name[TOPIC_LEN - 1] = '\0';
    t->topic[0] = '\0';
    t->topic_set = 0;
    t->topic_time = 0;
    t->topic_who[0] = '\0';
    t->sub_count = 0;
    return t;
}

static void subscribe(SOCKET sock, const char* nick, const char* topic_name) {
    Topic* t = get_topic(topic_name);
    if (!t) return;
    for (int i = 0; i < t->sub_count; i++)
        if (t->subscribers[i].sock == sock) return;
    if (t->sub_count >= MAX_SUBSCRIBERS) return;
    strncpy(t->subscribers[t->sub_count].nick, nick, NICK_LEN - 1);
    t->subscribers[t->sub_count].nick[NICK_LEN - 1] = '\0';
    t->subscribers[t->sub_count].sock = sock;
    t->sub_count++;
}

static void unsubscribe(SOCKET sock, const char* topic_name) {
    for (int i = 0; i < topic_count; i++) {
        if (topic_name && _stricmp(topics[i].name, topic_name) != 0) continue;
        Topic* t = &topics[i];
        int j = 0;
        while (j < t->sub_count) {
            if (t->subscribers[j].sock == sock) {
                memmove(&t->subscribers[j], &t->subscribers[j + 1],
                        (t->sub_count - j - 1) * sizeof(Subscriber));
                t->sub_count--;
            } else {
                j++;
            }
        }
    }
}

static void publish(Client* sender, const char* topic_name, const char* message) {
    char tag_extra[128] = {0};
    if (sender->cap_message_tags) {
        int mid = InterlockedIncrement(&next_msg_id) - 1;
        snprintf(tag_extra, sizeof(tag_extra), "msgid=%d", mid);
    }
    for (int i = 0; i < topic_count; i++) {
        if (_stricmp(topics[i].name, topic_name) != 0) continue;
        Topic* t = &topics[i];
        for (int j = 0; j < t->sub_count; j++) {
            SOCKET target = t->subscribers[j].sock;
            if (target == sender->sock) continue;
            Client* tc = NULL;
            for (int k = 0; k < client_count; k++) {
                if (clients[k].sock == target) { tc = &clients[k]; break; }
            }
            if (!tc) continue;
            send_with_tags(target, tc, tag_extra,
                           ":%s!%s@%s PRIVMSG %s :%s\r\n",
                           sender->nick, sender->user[0] ? sender->user : sender->nick,
                           HOSTNAME, topic_name, message);
        }
    }
}

static Client* find_client(SOCKET sock) {
    for (int i = 0; i < client_count; i++)
        if (clients[i].sock == sock) return &clients[i];
    return NULL;
}

static Client* find_client_by_nick(const char* nick) {
    for (int i = 0; i < client_count; i++)
        if (_stricmp(clients[i].nick, nick) == 0) return &clients[i];
    return NULL;
}

static void remove_client(SOCKET sock) {
    unsubscribe(sock, NULL);
    for (int i = 0; i < client_count; i++) {
        if (clients[i].sock == sock) {
            closesocket(sock);
            memmove(&clients[i], &clients[i + 1], (client_count - i - 1) * sizeof(Client));
            client_count--;
            return;
        }
    }
}

static int valid_channel(const char* name) {
    return name && name[0] == '#';
}

static int parse_tags(char* line, char* tag_out, int tag_max) {
    if (line[0] != '@') {
        if (tag_out) tag_out[0] = '\0';
        return 1;
    }
    char* space = strchr(line, ' ');
    if (!space) {
        if (tag_out) tag_out[0] = '\0';
        return 1;
    }
    if (tag_out) {
        int len = (int)(space - line - 1);
        if (len > tag_max - 1) len = tag_max - 1;
        memcpy(tag_out, line + 1, len);
        tag_out[len] = '\0';
    }
    memmove(line, space + 1, strlen(space + 1) + 1);
    return 1;
}

static void send_names(SOCKET sock, Client* c, Topic* t, const char* chan) {
    char names[512] = {0};
    int pos = 0;
    for (int j = 0; j < t->sub_count; j++) {
        if (pos > 0) names[pos++] = ' ';
        int left = (int)sizeof(names) - pos - 1;
        if (left <= 0) break;
        Client* nc = NULL;
        for (int k = 0; k < client_count; k++) {
            if (clients[k].sock == t->subscribers[j].sock) {
                nc = &clients[k]; break;
            }
        }
        if (c->cap_multi_prefix && nc) {
            (void)0; // no prefixes on this server
        }
        if (c->cap_userhost_in_names && nc) {
            pos += snprintf(names + pos, left, "%s!%s@%s",
                            nc->nick, nc->user[0] ? nc->user : nc->nick, HOSTNAME);
        } else {
            pos += snprintf(names + pos, left, "%s", t->subscribers[j].nick);
        }
    }
    send_to(sock, ":%s 353 %s = %s :%s\r\n", HOSTNAME, c->nick, chan, names);
    send_to(sock, ":%s 366 %s %s :End of /NAMES list\r\n", HOSTNAME, c->nick, chan);
}

static const char* nick_or_star(Client* c) {
    return (c && c->nick[0]) ? c->nick : "*";
}

static DWORD WINAPI handle_client(LPVOID param) {
    SOCKET sock = *(SOCKET*)param;
    free(param);

    char nickbuf[NICK_LEN] = {0};
    int nick_set = 0;
    int user_set = 0;
    char buf[BUFFER_SIZE];

    send_to(sock, ":%s NOTICE AUTH :*** Welcome to MQTT IRC Server\r\n", HOSTNAME);
    send_to(sock, ":%s NOTICE AUTH :*** Set your nick: NICK <nickname>\r\n", HOSTNAME);
    send_to(sock, ":%s NOTICE AUTH :*** For IRCv3 capabilities: CAP LS\r\n", HOSTNAME);

    while (1) {
        int n = recv(sock, buf, sizeof(buf) - 1, 0);
        if (n <= 0) break;
        buf[n] = '\0';

        char* line = buf;
        while (line && *line) {
            char* crlf = strstr(line, "\r\n");
            if (!crlf) crlf = strchr(line, '\n');
            if (!crlf) break;
            *crlf = '\0';
            char* next = crlf + 1;
            if (*next == '\n') next++;

            char tags[512] = {0};
            parse_tags(line, tags, sizeof(tags));

            char* cmd = strtok(line, " ");
            if (!cmd) { line = next; continue; }

            Client* c = find_client(sock);

            // Commands allowed before registration
            int is_preauth = (_stricmp(cmd, "NICK") == 0 || _stricmp(cmd, "USER") == 0 ||
                              _stricmp(cmd, "CAP") == 0 || _stricmp(cmd, "PASS") == 0 ||
                              _stricmp(cmd, "PING") == 0 || _stricmp(cmd, "PONG") == 0 ||
                              _stricmp(cmd, "QUIT") == 0 || _stricmp(cmd, "AUTHENTICATE") == 0);

            if (!is_preauth && (!c || !c->registered)) {
                send_to(sock, ":%s 451 %s :You have not registered\r\n", HOSTNAME, nick_or_star(c));
                line = next;
                continue;
            }

            if (_stricmp(cmd, "CAP") == 0) {
                char* subcmd = strtok(NULL, " ");
                char* arg = strtok(NULL, "");
                if (arg) {
                    while (*arg == ' ' || *arg == ':') arg++;
                }
                if (_stricmp(subcmd, "LS") == 0) {
                    char cap_list[512] = {0};
                    int pos = 0;
                    for (int i = 0; ircv3_caps[i]; i++) {
                        if (pos > 0) cap_list[pos++] = ' ';
                        pos += snprintf(cap_list + pos, sizeof(cap_list) - pos - 1, "%s", ircv3_caps[i]);
                    }
                    send_to(sock, ":%s CAP * LS :%s\r\n", HOSTNAME, cap_list);
                } else if (_stricmp(subcmd, "REQ") == 0) {
                    int any_ok = 0;
                    int any_fail = 0;
                    char* req = _strdup(arg);
                    char* tok = strtok(req, " ");
                    while (tok) {
                        int found = 0;
                        for (int i = 0; ircv3_caps[i]; i++) {
                            if (strcmp(tok, ircv3_caps[i]) == 0) {
                                found = 1;
                                any_ok = 1;
                                if (c) {
                                    if (strcmp(tok, "echo-message") == 0) c->cap_echo_message = 1;
                                    else if (strcmp(tok, "server-time") == 0) c->cap_server_time = 1;
                                    else if (strcmp(tok, "message-tags") == 0) c->cap_message_tags = 1;
                                    else if (strcmp(tok, "extended-join") == 0) c->cap_extended_join = 1;
                                    else if (strcmp(tok, "multi-prefix") == 0) c->cap_multi_prefix = 1;
                                    else if (strcmp(tok, "account-tag") == 0) c->cap_account_tag = 1;
                                    else if (strcmp(tok, "userhost-in-names") == 0) c->cap_userhost_in_names = 1;
                                }
                                break;
                            }
                        }
                        if (!found) any_fail = 1;
                        tok = strtok(NULL, " ");
                    }
                    free(req);
                    if (any_ok) {
                        send_to(sock, ":%s CAP %s ACK :%s\r\n", HOSTNAME, nick_or_star(c), arg);
                    }
                    if (any_fail) {
                        send_to(sock, ":%s CAP %s NAK :%s\r\n", HOSTNAME, nick_or_star(c), arg);
                    }
                } else if (_stricmp(subcmd, "END") == 0) {
                    // CAP negotiation done
                } else if (_stricmp(subcmd, "LIST") == 0) {
                    send_to(sock, ":%s CAP %s LIST :\r\n", HOSTNAME, nick_or_star(c));
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "NICK") == 0) {
                char* arg = strtok(NULL, " ");
                if (!arg) {
                    send_to(sock, ":%s 431 %s :No nickname given\r\n", HOSTNAME, nick_or_star(c));
                    line = next;
                    continue;
                }
                strncpy(nickbuf, arg, NICK_LEN - 1);
                nickbuf[NICK_LEN - 1] = '\0';
                nick_set = 1;
                EnterCriticalSection(&cs);
                if (c) {
                    strncpy(c->nick, nickbuf, NICK_LEN - 1);
                    c->nick[NICK_LEN - 1] = '\0';
                    if (c->registered) {
                        send_with_msgid(sock, c, ":%s!%s@%s NICK %s\r\n",
                                        nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, nickbuf);
                    } else {
                        send_to(sock, ":%s NICK %s\r\n", nickbuf, nickbuf);
                    }
                }
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "USER") == 0) {
                if (c && c->registered) {
                    send_to(sock, ":%s 462 %s :You may not reregister\r\n", HOSTNAME, nick_or_star(c));
                    line = next;
                    continue;
                }
                char* u = strtok(NULL, " ");
                strtok(NULL, " "); // mode
                strtok(NULL, " "); // unused
                char* real = strtok(NULL, "");
                if (!u || !real) {
                    send_to(sock, ":%s 461 %s USER :Not enough parameters\r\n",
                            HOSTNAME, nick_or_star(c));
                    line = next;
                    continue;
                }
                while (*real == ' ' || *real == ':') real++;
                if (!u[0]) u = "unknown";
                user_set = 1;
                EnterCriticalSection(&cs);
                if (c && !c->registered && nick_set) {
                    strncpy(c->user, u, NICK_LEN - 1);
                    c->user[NICK_LEN - 1] = '\0';
                    strncpy(c->real, real, 127);
                    c->real[127] = '\0';
                    c->registered = 1;
                    c->signon = time(NULL);
                    c->idle = time(NULL);

                    char ts[64];
                    time_t now = time(NULL);
                    struct tm* tm_gmt = gmtime(&now);
                    strftime(ts, sizeof(ts), "%a %b %d %Y at %H:%M:%S UTC", tm_gmt);

                    send_to(sock, ":%s 001 %s :Welcome to the %s Network, %s!%s@%s\r\n",
                            HOSTNAME, nickbuf, NETWORK, nickbuf,
                            c->user[0] ? c->user : nickbuf, HOSTNAME);
                    send_to(sock, ":%s 002 %s :Your host is %s, running version %s\r\n",
                            HOSTNAME, nickbuf, HOSTNAME, VERSION);
                    send_to(sock, ":%s 003 %s :This server was created %s\r\n",
                            HOSTNAME, nickbuf, ts);
                    send_to(sock, ":%s 004 %s %s %s aiw oiv\r\n",
                            HOSTNAME, nickbuf, HOSTNAME, VERSION);
                    send_to(sock, ":%s 005 %s CHANTYPES=# CHANNELLEN=%d NICKLEN=%d TOPICLEN=%d "
                            "PREFIX=(ov)@+ NETWORK=%s CASEMAPPING=rfc1459 "
                            "ELIST=CTU :are supported by this server\r\n",
                            HOSTNAME, nickbuf, TOPIC_LEN, NICK_LEN, TOPIC_LEN, NETWORK);
                    send_to(sock, ":%s 251 %s :There are %d users and 0 invisible on 1 server\r\n",
                            HOSTNAME, nickbuf, client_count);
                    send_to(sock, ":%s 254 %s %d :channels formed\r\n",
                            HOSTNAME, nickbuf, topic_count);
                    send_to(sock, ":%s 255 %s :I have %d clients and 1 server\r\n",
                            HOSTNAME, nickbuf, client_count);
                    send_to(sock, ":%s 375 %s :- MQTT IRC Server Message of the Day -\r\n",
                            HOSTNAME, nickbuf);
                    send_to(sock, ":%s 372 %s :- Welcome to MQTT IRC Server\r\n", HOSTNAME, nickbuf);
                    send_to(sock, ":%s 372 %s :- Topics are MQTT channels - publish/subscribe\r\n",
                            HOSTNAME, nickbuf);
                    send_to(sock, ":%s 376 %s :End of MOTD command\r\n", HOSTNAME, nickbuf);
                } else if (c && !nick_set) {
                    // store user info, complete registration later when NICK arrives
                    strncpy(c->user, u, NICK_LEN - 1);
                    c->user[NICK_LEN - 1] = '\0';
                    strncpy(c->real, real, 127);
                    c->real[127] = '\0';
                }
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "JOIN") == 0) {
                if (!c || !c->registered) { line = next; continue; }
                char* chan = strtok(NULL, " ");
                while (chan && *chan == ':') chan++;
                if (!chan) {
                    send_to(sock, ":%s 461 %s JOIN :Not enough parameters\r\n", HOSTNAME, nickbuf);
                    line = next;
                    continue;
                }
                if (!valid_channel(chan)) {
                    send_to(sock, ":%s 403 %s %s :No such channel\r\n", HOSTNAME, nickbuf, chan);
                    line = next;
                    continue;
                }
                EnterCriticalSection(&cs);
                subscribe(sock, nickbuf, chan);
                char tag_extra[128] = {0};
                if (c->cap_message_tags) {
                    int mid = InterlockedIncrement(&next_msg_id) - 1;
                    snprintf(tag_extra, sizeof(tag_extra), "msgid=%d", mid);
                }
                if (c->cap_extended_join) {
                    send_with_tags(sock, c, tag_extra,
                                   ":%s!%s@%s JOIN %s %s :%s\r\n",
                                   nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME,
                                   chan, c->account[0] ? c->account : "*", c->real);
                } else {
                    send_with_tags(sock, c, tag_extra,
                                   ":%s!%s@%s JOIN %s\r\n",
                                   nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, chan);
                }
                Topic* t = NULL;
                for (int i = 0; i < topic_count; i++) {
                    if (_stricmp(topics[i].name, chan) == 0) { t = &topics[i]; break; }
                }
                if (t) {
                    for (int j = 0; j < t->sub_count; j++) {
                        SOCKET other = t->subscribers[j].sock;
                        if (other == sock) continue;
                        Client* oc = NULL;
                        for (int k = 0; k < client_count; k++) {
                            if (clients[k].sock == other) { oc = &clients[k]; break; }
                        }
                        if (!oc) continue;
                        if (c->cap_extended_join) {
                            send_with_tags(other, oc, tag_extra,
                                           ":%s!%s@%s JOIN %s %s :%s\r\n",
                                           nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME,
                                           chan, c->account[0] ? c->account : "*", c->real);
                        } else {
                            send_with_tags(other, oc, tag_extra,
                                           ":%s!%s@%s JOIN %s\r\n",
                                           nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, chan);
                        }
                    }
                }
                if (t && t->topic_set) {
                    send_to(sock, ":%s 332 %s %s :%s\r\n", HOSTNAME, nickbuf, chan, t->topic);
                    send_to(sock, ":%s 333 %s %s %s %lld\r\n",
                            HOSTNAME, nickbuf, chan, t->topic_who, (long long)t->topic_time);
                } else {
                    send_to(sock, ":%s 332 %s %s :MQTT topic - messages published here\r\n",
                            HOSTNAME, nickbuf, chan);
                }
                send_names(sock, c, t, chan);
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "PART") == 0) {
                if (!c) { line = next; continue; }
                char* chan = strtok(NULL, " ");
                if (!chan) {
                    send_to(sock, ":%s 461 %s PART :Not enough parameters\r\n", HOSTNAME, nickbuf);
                    line = next;
                    continue;
                }
                EnterCriticalSection(&cs);
                send_with_msgid(sock, c, ":%s!%s@%s PART %s\r\n",
                                nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, chan);
                for (int i = 0; i < topic_count; i++) {
                    if (_stricmp(topics[i].name, chan) == 0) {
                        for (int j = 0; j < topics[i].sub_count; j++) {
                            SOCKET other = topics[i].subscribers[j].sock;
                            if (other == sock) continue;
                            Client* oc = NULL;
                            for (int k = 0; k < client_count; k++) {
                                if (clients[k].sock == other) { oc = &clients[k]; break; }
                            }
                            if (!oc) continue;
                            send_with_msgid(oc->sock, oc, ":%s!%s@%s PART %s\r\n",
                                            nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, chan);
                        }
                        break;
                    }
                }
                unsubscribe(sock, chan);
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "PRIVMSG") == 0) {
                if (!c || !c->registered) { line = next; continue; }
                char* target = strtok(NULL, " ");
                char* msg = strtok(NULL, "");
                if (!target || !msg) {
                    if (!target)
                        send_to(sock, ":%s 411 %s :No recipient given (PRIVMSG)\r\n", HOSTNAME, nickbuf);
                    else
                        send_to(sock, ":%s 412 %s :No text to send\r\n", HOSTNAME, nickbuf);
                    line = next;
                    continue;
                }
                while (*msg == ' ') msg++;
                if (*msg == ':') msg++;
                EnterCriticalSection(&cs);
                char tag_extra[128] = {0};
                if (c->cap_message_tags) {
                    int mid = InterlockedIncrement(&next_msg_id) - 1;
                    snprintf(tag_extra, sizeof(tag_extra), "msgid=%d", mid);
                }
                if (valid_channel(target)) {
                    publish(c, target, msg);
                    if (c->cap_echo_message) {
                        send_with_tags(sock, c, tag_extra,
                                       ":%s!%s@%s PRIVMSG %s :%s\r\n",
                                       nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, target, msg);
                    }
                } else {
                    Client* target_c = NULL;
                    for (int i = 0; i < client_count; i++) {
                        if (_stricmp(clients[i].nick, target) == 0 && clients[i].sock != sock) {
                            target_c = &clients[i];
                            break;
                        }
                    }
                    if (!target_c) {
                        send_to(sock, ":%s 401 %s %s :No such nick/channel\r\n",
                                HOSTNAME, nickbuf, target);
                    } else {
                        send_with_tags(target_c->sock, target_c, tag_extra,
                                       ":%s!%s@%s PRIVMSG %s :%s\r\n",
                                       nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME,
                                       target, msg);
                        if (target_c->away) {
                            send_to(sock, ":%s 301 %s %s :%s\r\n",
                                    HOSTNAME, nickbuf, target, target_c->away_msg);
                        }
                        if (c->cap_echo_message) {
                            send_with_tags(sock, c, tag_extra,
                                           ":%s!%s@%s PRIVMSG %s :%s\r\n",
                                           nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME,
                                           target, msg);
                        }
                    }
                }
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "NOTICE") == 0) {
                if (!c || !c->registered) { line = next; continue; }
                char* target = strtok(NULL, " ");
                char* msg = strtok(NULL, "");
                if (target && msg) {
                    while (*msg == ' ') msg++;
                    if (*msg == ':') msg++;
                    EnterCriticalSection(&cs);
                    char tag_extra[128] = {0};
                    if (c->cap_message_tags) {
                        int mid = InterlockedIncrement(&next_msg_id) - 1;
                        snprintf(tag_extra, sizeof(tag_extra), "msgid=%d", mid);
                    }
                    if (valid_channel(target)) {
                        for (int i = 0; i < topic_count; i++) {
                            if (_stricmp(topics[i].name, target) != 0) continue;
                            for (int j = 0; j < topics[i].sub_count; j++) {
                                SOCKET other = topics[i].subscribers[j].sock;
                                if (other == sock) continue;
                                Client* oc = NULL;
                                for (int k = 0; k < client_count; k++) {
                                    if (clients[k].sock == other) { oc = &clients[k]; break; }
                                }
                                if (!oc) continue;
                                send_with_tags(other, oc, tag_extra,
                                               ":%s!%s@%s NOTICE %s :%s\r\n",
                                               nickbuf, c->user[0] ? c->user : nickbuf,
                                               HOSTNAME, target, msg);
                            }
                        }
                    } else {
                        for (int i = 0; i < client_count; i++) {
                            if (_stricmp(clients[i].nick, target) == 0 && clients[i].sock != sock) {
                                send_with_tags(clients[i].sock, &clients[i], tag_extra,
                                               ":%s!%s@%s NOTICE %s :%s\r\n",
                                               nickbuf, c->user[0] ? c->user : nickbuf,
                                               HOSTNAME, target, msg);
                                break;
                            }
                        }
                    }
                    LeaveCriticalSection(&cs);
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "TAGMSG") == 0) {
                if (!c || !c->registered) { line = next; continue; }
                char* target = strtok(NULL, " ");
                if (target) {
                    char tag_extra[128] = {0};
                    if (c->cap_message_tags) {
                        int mid = InterlockedIncrement(&next_msg_id) - 1;
                        snprintf(tag_extra, sizeof(tag_extra), "msgid=%d", mid);
                    }
                    if (valid_channel(target)) {
                        for (int i = 0; i < topic_count; i++) {
                            if (_stricmp(topics[i].name, target) != 0) continue;
                            for (int j = 0; j < topics[i].sub_count; j++) {
                                SOCKET other = topics[i].subscribers[j].sock;
                                if (other == sock) continue;
                                Client* oc = NULL;
                                for (int k = 0; k < client_count; k++) {
                                    if (clients[k].sock == other) { oc = &clients[k]; break; }
                                }
                                if (!oc) continue;
                                char client_tags[1024];
                                snprintf(client_tags, sizeof(client_tags), "%s%s%s",
                                         oc->cap_server_time ? "time=" : "",
                                         oc->cap_server_time ? get_timestamp() : "",
                                         tag_extra[0] ? ";" : "");
                                if (oc->cap_message_tags) {
                                    if (client_tags[0]) strcat(client_tags, ";");
                                    strncat(client_tags, tags, sizeof(client_tags) - strlen(client_tags) - 1);
                                }
                                send_with_tags(other, oc, client_tags,
                                               ":%s!%s@%s TAGMSG %s\r\n",
                                               nickbuf, c->user[0] ? c->user : nickbuf,
                                               HOSTNAME, target);
                            }
                        }
                        if (c->cap_echo_message) {
                            send_with_tags(sock, c, tag_extra,
                                           ":%s!%s@%s TAGMSG %s\r\n",
                                           nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, target);
                        }
                    } else {
                        for (int i = 0; i < client_count; i++) {
                            if (_stricmp(clients[i].nick, target) == 0 && clients[i].sock != sock) {
                                send_with_tags(clients[i].sock, &clients[i], tag_extra,
                                               ":%s!%s@%s TAGMSG %s\r\n",
                                               nickbuf, c->user[0] ? c->user : nickbuf,
                                               HOSTNAME, target);
                                break;
                            }
                        }
                        if (c->cap_echo_message) {
                            send_with_tags(sock, c, tag_extra,
                                           ":%s!%s@%s TAGMSG %s\r\n",
                                           nickbuf, c->user[0] ? c->user : nickbuf, HOSTNAME, target);
                        }
                    }
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "LIST") == 0) {
                EnterCriticalSection(&cs);
                send_to(sock, ":%s 321 %s Channel :Users Topic\r\n", HOSTNAME, nickbuf);
                for (int i = 0; i < topic_count; i++) {
                    send_to(sock, ":%s 322 %s %s %d :MQTT topic\r\n",
                            HOSTNAME, nickbuf, topics[i].name, topics[i].sub_count);
                }
                send_to(sock, ":%s 323 %s :End of /LIST\r\n", HOSTNAME, nickbuf);
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "QUIT") == 0) {
                char* reason = strtok(NULL, "");
                if (reason) {
                    while (*reason == ' ' || *reason == ':') reason++;
                } else {
                    reason = "Client quit";
                }
                EnterCriticalSection(&cs);
                for (int i = 0; i < topic_count; i++) {
                    for (int j = 0; j < topics[i].sub_count; j++) {
                        if (topics[i].subscribers[j].sock == sock) continue;
                        Client* oc = NULL;
                        for (int k = 0; k < client_count; k++) {
                            if (clients[k].sock == topics[i].subscribers[j].sock) {
                                oc = &clients[k]; break;
                            }
                        }
                        if (!oc) continue;
                        if (c) {
                            send_with_msgid(topics[i].subscribers[j].sock, oc,
                                            ":%s!%s@%s QUIT :%s\r\n",
                                            c->nick, c->user[0] ? c->user : c->nick,
                                            HOSTNAME, reason);
                        }
                    }
                }
                send_to(sock, "ERROR :Closing connection\r\n");
                remove_client(sock);
                LeaveCriticalSection(&cs);
                return 0;
            }

            if (_stricmp(cmd, "PING") == 0) {
                char* arg = strtok(NULL, "");
                if (arg) {
                    while (*arg == ' ') arg++;
                    send_to(sock, ":%s PONG %s :%s\r\n", HOSTNAME, HOSTNAME, arg);
                } else {
                    send_to(sock, ":%s 409 %s :No origin specified\r\n", HOSTNAME, nick_or_star(c));
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "PONG") == 0) {
                line = next;
                continue;
            }

            if (_stricmp(cmd, "TOPIC") == 0) {
                if (!c || !c->registered) { line = next; continue; }
                char* chan = strtok(NULL, " ");
                char* new_topic = strtok(NULL, "");
                if (!chan) {
                    send_to(sock, ":%s 461 %s TOPIC :Not enough parameters\r\n", HOSTNAME, nickbuf);
                    line = next;
                    continue;
                }
                if (!valid_channel(chan)) {
                    send_to(sock, ":%s 403 %s %s :No such channel\r\n", HOSTNAME, nickbuf, chan);
                    line = next;
                    continue;
                }
                if (new_topic) {
                    while (*new_topic == ' ' || *new_topic == ':') new_topic++;
                    EnterCriticalSection(&cs);
                    Topic* t = get_topic(chan);
                    if (t) {
                        strncpy(t->topic, new_topic, TOPIC_LEN - 1);
                        t->topic[TOPIC_LEN - 1] = '\0';
                        t->topic_set = 1;
                        t->topic_time = time(NULL);
                        strncpy(t->topic_who, nickbuf, NICK_LEN - 1);
                        t->topic_who[NICK_LEN - 1] = '\0';
                        char tag_extra[128] = {0};
                        if (c->cap_message_tags) {
                            int mid = InterlockedIncrement(&next_msg_id) - 1;
                            snprintf(tag_extra, sizeof(tag_extra), "msgid=%d", mid);
                        }
                        for (int j = 0; j < t->sub_count; j++) {
                            SOCKET other = t->subscribers[j].sock;
                            Client* oc = NULL;
                            for (int k = 0; k < client_count; k++) {
                                if (clients[k].sock == other) { oc = &clients[k]; break; }
                            }
                            if (!oc) continue;
                            send_with_tags(other, oc, tag_extra,
                                           ":%s!%s@%s TOPIC %s :%s\r\n",
                                           nickbuf, c->user[0] ? c->user : nickbuf,
                                           HOSTNAME, chan, new_topic);
                        }
                    }
                    LeaveCriticalSection(&cs);
                } else {
                    EnterCriticalSection(&cs);
                    int found = 0;
                    for (int i = 0; i < topic_count; i++) {
                        if (_stricmp(topics[i].name, chan) == 0) {
                            if (topics[i].topic_set) {
                                send_to(sock, ":%s 332 %s %s :%s\r\n", HOSTNAME, nickbuf, chan, topics[i].topic);
                                send_to(sock, ":%s 333 %s %s %s %lld\r\n",
                                        HOSTNAME, nickbuf, chan,
                                        topics[i].topic_who, (long long)topics[i].topic_time);
                            } else {
                                send_to(sock, ":%s 331 %s %s :No topic is set\r\n", HOSTNAME, nickbuf, chan);
                            }
                            found = 1;
                            break;
                        }
                    }
                    if (!found) {
                        send_to(sock, ":%s 403 %s %s :No such channel\r\n", HOSTNAME, nickbuf, chan);
                    }
                    LeaveCriticalSection(&cs);
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "NAMES") == 0) {
                char* chan = strtok(NULL, " ");
                if (!chan) chan = (char*)"";
                EnterCriticalSection(&cs);
                for (int i = 0; i < topic_count; i++) {
                    if (chan[0] == '\0' || _stricmp(topics[i].name, chan) == 0) {
                        send_names(sock, c, &topics[i], topics[i].name);
                    }
                }
                if (chan[0] != '\0') {
                    int found = 0;
                    for (int i = 0; i < topic_count; i++) {
                        if (_stricmp(topics[i].name, chan) == 0) { found = 1; break; }
                    }
                    if (!found) {
                        send_to(sock, ":%s 366 %s %s :End of /NAMES list\r\n", HOSTNAME, nickbuf, chan);
                    }
                }
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "WHO") == 0) {
                char* mask = strtok(NULL, " ");
                char* o_only = strtok(NULL, " ");
                (void)o_only;
                EnterCriticalSection(&cs);
                if (mask && strchr(mask, '#')) {
                    for (int i = 0; i < topic_count; i++) {
                        if (_stricmp(topics[i].name, mask) != 0) continue;
                        for (int j = 0; j < topics[i].sub_count; j++) {
                            Client* wc = NULL;
                            for (int k = 0; k < client_count; k++) {
                                if (clients[k].sock == topics[i].subscribers[j].sock) {
                                    wc = &clients[k]; break;
                                }
                            }
                            if (!wc) continue;
                            char flags[8] = "H";
                            if (wc->away) flags[0] = 'G';
                            send_to(sock, ":%s 352 %s %s %s %s %s %s %s :0 %s\r\n",
                                    HOSTNAME, nickbuf,
                                    topics[i].name,
                                    wc->user[0] ? wc->user : wc->nick,
                                    HOSTNAME, HOSTNAME,
                                    wc->nick, flags,
                                    wc->real[0] ? wc->real : wc->nick);
                        }
                    }
                } else if (mask) {
                    Client* wc = find_client_by_nick(mask);
                    if (wc) {
                        char flags[8] = "H";
                        if (wc->away) flags[0] = 'G';
                        send_to(sock, ":%s 352 %s * %s %s %s %s %s :0 %s\r\n",
                                HOSTNAME, nickbuf,
                                wc->user[0] ? wc->user : wc->nick,
                                HOSTNAME, HOSTNAME,
                                wc->nick, flags,
                                wc->real[0] ? wc->real : wc->nick);
                    }
                } else {
                    for (int i = 0; i < client_count; i++) {
                        char flags[8] = "H";
                        if (clients[i].away) flags[0] = 'G';
                        send_to(sock, ":%s 352 %s * %s %s %s %s %s :0 %s\r\n",
                                HOSTNAME, nickbuf,
                                clients[i].user[0] ? clients[i].user : clients[i].nick,
                                HOSTNAME, HOSTNAME,
                                clients[i].nick, flags,
                                clients[i].real[0] ? clients[i].real : clients[i].nick);
                    }
                }
                send_to(sock, ":%s 315 %s %s :End of WHO list\r\n",
                        HOSTNAME, nickbuf, mask ? mask : "*");
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "WHOIS") == 0) {
                char* target = strtok(NULL, " ");
                if (!target) {
                    send_to(sock, ":%s 431 %s :No nickname given\r\n", HOSTNAME, nickbuf);
                    line = next;
                    continue;
                }
                while (*target == ':' && target[1]) target++;
                // strip server mask if present
                char* comma = strchr(target, ',');
                if (comma) *comma = '\0';

                EnterCriticalSection(&cs);
                Client* target_c = find_client_by_nick(target);
                if (!target_c) {
                    send_to(sock, ":%s 401 %s %s :No such nick/channel\r\n", HOSTNAME, nickbuf, target);
                } else {
                    send_to(sock, ":%s 311 %s %s %s %s * :%s\r\n",
                            HOSTNAME, nickbuf, target,
                            target_c->user[0] ? target_c->user : target,
                            HOSTNAME,
                            target_c->real[0] ? target_c->real : target);
                    send_to(sock, ":%s 312 %s %s %s :MQTT IRC Server\r\n",
                            HOSTNAME, nickbuf, target, HOSTNAME);
                    if (target_c->account[0]) {
                        send_to(sock, ":%s 330 %s %s %s :is logged in as\r\n",
                                HOSTNAME, nickbuf, target, target_c->account);
                    }
                    time_t now = time(NULL);
                    int idle_secs = (int)difftime(now, target_c->idle);
                    int signon_secs = (int)difftime(now, target_c->signon);
                    send_to(sock, ":%s 317 %s %s %d %d :seconds idle, signon time\r\n",
                            HOSTNAME, nickbuf, target, idle_secs, signon_secs);
                    if (target_c->away) {
                        send_to(sock, ":%s 301 %s %s :%s\r\n",
                                HOSTNAME, nickbuf, target, target_c->away_msg);
                    }
                    // Build channel list
                    char chans[512] = {0};
                    int cp = 0;
                    for (int ti = 0; ti < topic_count; ti++) {
                        for (int sj = 0; sj < topics[ti].sub_count; sj++) {
                            if (topics[ti].subscribers[sj].sock == target_c->sock) {
                                if (cp > 0) chans[cp++] = ' ';
                                int cl = (int)sizeof(chans) - cp - 1;
                                if (cl <= 0) break;
                                cp += snprintf(chans + cp, cl, "%s", topics[ti].name);
                                break;
                            }
                        }
                    }
                    if (chans[0]) {
                        send_to(sock, ":%s 319 %s %s :%s\r\n", HOSTNAME, nickbuf, target, chans);
                    }
                    send_to(sock, ":%s 318 %s %s :End of WHOIS list\r\n", HOSTNAME, nickbuf, target);
                }
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "USERHOST") == 0) {
                char* target = strtok(NULL, " ");
                if (target) {
                    EnterCriticalSection(&cs);
                    Client* uc = find_client_by_nick(target);
                    if (uc) {
                        char away_flag = uc->away ? '-' : '+';
                        send_to(sock, ":%s 302 %s :%s%c%s@%s\r\n",
                                HOSTNAME, nickbuf, target, away_flag,
                                uc->user[0] ? uc->user : target, HOSTNAME);
                    }
                    LeaveCriticalSection(&cs);
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "MODE") == 0) {
                char* target = strtok(NULL, " ");
                if (!target) {
                    send_to(sock, ":%s 461 %s MODE :Not enough parameters\r\n", HOSTNAME, nickbuf);
                    line = next;
                    continue;
                }
                if (valid_channel(target)) {
                    send_to(sock, ":%s 324 %s %s +nt\r\n", HOSTNAME, nickbuf, target);
                } else if (_stricmp(target, nickbuf) == 0) {
                    send_to(sock, ":%s 221 %s +\r\n", HOSTNAME, nickbuf);
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "AWAY") == 0) {
                if (!c) { line = next; continue; }
                char* msg = strtok(NULL, "");
                if (msg) {
                    while (*msg == ' ' || *msg == ':') msg++;
                    c->away = 1;
                    strncpy(c->away_msg, msg, sizeof(c->away_msg) - 1);
                    c->away_msg[sizeof(c->away_msg) - 1] = '\0';
                    send_to(sock, ":%s 306 %s :You have been marked as being away\r\n", HOSTNAME, nickbuf);
                } else {
                    c->away = 0;
                    c->away_msg[0] = '\0';
                    send_to(sock, ":%s 305 %s :You are no longer marked as being away\r\n", HOSTNAME, nickbuf);
                }
                line = next;
                continue;
            }

            if (_stricmp(cmd, "LUSERS") == 0) {
                EnterCriticalSection(&cs);
                send_to(sock, ":%s 251 %s :There are %d users and 0 invisible on 1 server\r\n",
                        HOSTNAME, nickbuf, client_count);
                send_to(sock, ":%s 254 %s %d :channels formed\r\n",
                        HOSTNAME, nickbuf, topic_count);
                send_to(sock, ":%s 255 %s :I have %d clients and 1 server\r\n",
                        HOSTNAME, nickbuf, client_count);
                LeaveCriticalSection(&cs);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "MOTD") == 0) {
                send_to(sock, ":%s 375 %s :- MQTT IRC Server Message of the Day -\r\n", HOSTNAME, nickbuf);
                send_to(sock, ":%s 372 %s :- Welcome to MQTT IRC Server\r\n", HOSTNAME, nickbuf);
                send_to(sock, ":%s 372 %s :- Topics are MQTT channels - publish/subscribe\r\n", HOSTNAME, nickbuf);
                send_to(sock, ":%s 376 %s :End of MOTD command\r\n", HOSTNAME, nickbuf);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "VERSION") == 0) {
                send_to(sock, ":%s 351 %s %s-0 %s :IRCv3 MQTT server\r\n",
                        HOSTNAME, nickbuf, VERSION, HOSTNAME);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "ADMIN") == 0) {
                send_to(sock, ":%s 256 %s :Administrative info about MQTT IRC Server\r\n",
                        HOSTNAME, nickbuf);
                send_to(sock, ":%s 257 %s :Server: %s\r\n", HOSTNAME, nickbuf, HOSTNAME);
                send_to(sock, ":%s 258 %s :IRCv3 MQTT bridge\r\n", HOSTNAME, nickbuf);
                send_to(sock, ":%s 259 %s :admin@mqtt-irc.local\r\n", HOSTNAME, nickbuf);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "AUTHENTICATE") == 0) {
                send_to(sock, ":%s 904 %s :SASL authentication failed\r\n", HOSTNAME, nickbuf);
                send_to(sock, ":%s 906 %s :SASL aborted\r\n", HOSTNAME, nickbuf);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "PASS") == 0) {
                // Accept but ignore (no server password required)
                line = next;
                continue;
            }

            if (_stricmp(cmd, "OPER") == 0) {
                send_to(sock, ":%s 491 %s :No O-lines for your host\r\n", HOSTNAME, nickbuf);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "TIME") == 0) {
                time_t now = time(NULL);
                char ts[64];
                struct tm* tm_gmt = gmtime(&now);
                strftime(ts, sizeof(ts), "%a %b %d %Y at %H:%M:%S UTC", tm_gmt);
                send_to(sock, ":%s 391 %s %s :%s\r\n", HOSTNAME, nickbuf, HOSTNAME, ts);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "INFO") == 0) {
                send_to(sock, ":%s 371 %s :MQTT IRC Server - IRCv3 MQTT bridge\r\n", HOSTNAME, nickbuf);
                send_to(sock, ":%s 371 %s :https://github.com/user/mqtt-irc-server\r\n", HOSTNAME, nickbuf);
                send_to(sock, ":%s 374 %s :End of INFO list\r\n", HOSTNAME, nickbuf);
                line = next;
                continue;
            }

            if (_stricmp(cmd, "KILL") == 0) {
                send_to(sock, ":%s 481 %s :Permission denied - You are not an IRC operator\r\n",
                        HOSTNAME, nickbuf);
                line = next;
                continue;
            }

            send_to(sock, ":%s 421 %s %s :Unknown command\r\n", HOSTNAME, nickbuf, cmd);
            line = next;
        }
    }

    EnterCriticalSection(&cs);
    Client* c = find_client(sock);
    if (c) {
        for (int i = 0; i < topic_count; i++) {
            for (int j = 0; j < topics[i].sub_count; j++) {
                if (topics[i].subscribers[j].sock == sock) continue;
                Client* oc = NULL;
                for (int k = 0; k < client_count; k++) {
                    if (clients[k].sock == topics[i].subscribers[j].sock) {
                        oc = &clients[k]; break;
                    }
                }
                if (!oc) continue;
                send_with_msgid(topics[i].subscribers[j].sock, oc,
                                ":%s!%s@%s QUIT :Connection closed\r\n",
                                c->nick, c->user[0] ? c->user : c->nick, HOSTNAME);
            }
        }
    }
    remove_client(sock);
    LeaveCriticalSection(&cs);
    return 0;
}

static BOOL WINAPI console_handler(DWORD sig) {
    if (sig == CTRL_C_EVENT || sig == CTRL_BREAK_EVENT || sig == CTRL_CLOSE_EVENT) {
        running = 0;
        if (server_sock != INVALID_SOCKET)
            closesocket(server_sock);
        return TRUE;
    }
    return FALSE;
}

int main() {
    WSADATA wsa;
    if (WSAStartup(MAKEWORD(2, 2), &wsa) != 0) {
        fprintf(stderr, "WSAStartup failed\n");
        return 1;
    }

    InitializeCriticalSection(&cs);
    SetConsoleCtrlHandler(console_handler, TRUE);

    server_sock = socket(AF_INET, SOCK_STREAM, 0);
    if (server_sock == INVALID_SOCKET) {
        fprintf(stderr, "socket() failed: %d\n", WSAGetLastError());
        WSACleanup();
        return 1;
    }

    int opt = 1;
    setsockopt(server_sock, SOL_SOCKET, SO_REUSEADDR, (const char*)&opt, sizeof(opt));

    struct sockaddr_in addr;
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_port = htons(PORT);

    if (bind(server_sock, (struct sockaddr*)&addr, sizeof(addr)) == SOCKET_ERROR) {
        fprintf(stderr, "bind() failed on port %d: %d\n", PORT, WSAGetLastError());
        closesocket(server_sock);
        DeleteCriticalSection(&cs);
        WSACleanup();
        return 1;
    }

    if (listen(server_sock, SOMAXCONN) == SOCKET_ERROR) {
        fprintf(stderr, "listen() failed: %d\n", WSAGetLastError());
        closesocket(server_sock);
        DeleteCriticalSection(&cs);
        WSACleanup();
        return 1;
    }

    printf("+-------------------------------------------+\n");
    printf("|     MQTT IRC Server (IRCv3)               |\n");
    printf("|     aligned to modern.ircdocs.horse       |\n");
    printf("+-------------------------------------------+\n");
    printf("Listening on port %d\n", PORT);
    printf("Connect: /server 127.0.0.1 %d\n", PORT);
    printf("Press Ctrl+C to stop.\n\n");

    while (running) {
        struct sockaddr_in client_addr;
        int addrlen = sizeof(client_addr);
        SOCKET client = accept(server_sock, (struct sockaddr*)&client_addr, &addrlen);
        if (client == INVALID_SOCKET) {
            if (running) {
                fprintf(stderr, "accept() failed: %d\n", WSAGetLastError());
            }
            continue;
        }

        EnterCriticalSection(&cs);
        if (client_count >= MAX_CLIENTS) {
            LeaveCriticalSection(&cs);
            send_to(client, ":%s NOTICE * :Server full, try again later\r\n", HOSTNAME);
            closesocket(client);
            continue;
        }
        Client* c = &clients[client_count++];
        c->sock = client;
        c->nick[0] = '\0';
        c->user[0] = '\0';
        c->real[0] = '\0';
        c->account[0] = '\0';
        c->addr = client_addr;
        c->cap_echo_message = 0;
        c->cap_server_time = 0;
        c->cap_message_tags = 0;
        c->cap_extended_join = 0;
        c->cap_multi_prefix = 0;
        c->cap_account_tag = 0;
        c->cap_userhost_in_names = 0;
        c->registered = 0;
        c->away = 0;
        c->away_msg[0] = '\0';
        c->signon = 0;
        c->idle = 0;
        LeaveCriticalSection(&cs);

        SOCKET* psock = malloc(sizeof(SOCKET));
        if (!psock) { closesocket(client); continue; }
        *psock = client;

        HANDLE h = CreateThread(NULL, 0, handle_client, psock, 0, NULL);
        if (!h) {
            free(psock);
            closesocket(client);
            EnterCriticalSection(&cs);
            remove_client(client);
            LeaveCriticalSection(&cs);
            continue;
        }
        CloseHandle(h);

        printf("[+] Connection from %s:%d (total: %d)\n",
               inet_ntoa(client_addr.sin_addr), ntohs(client_addr.sin_port), client_count);
    }

    // Cleanup
    EnterCriticalSection(&cs);
    for (int i = 0; i < client_count; i++) {
        closesocket(clients[i].sock);
    }
    client_count = 0;
    topic_count = 0;
    LeaveCriticalSection(&cs);

    closesocket(server_sock);
    DeleteCriticalSection(&cs);
    WSACleanup();
    printf("\nServer stopped.\n");
    return 0;
}
