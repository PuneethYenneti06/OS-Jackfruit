/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Intentionally partial starter:
 *   - command-line shape is defined
 *   - key runtime data structures are defined
 *   - bounded-buffer skeleton is defined
 *   - supervisor / client split is outlined
 *
 * Students are expected to design:
 *   - the control-plane IPC implementation
 *   - container lifecycle and metadata synchronization
 *   - clone + namespace setup for each container
 *   - producer/consumer behavior for log buffering
 *   - signal handling and graceful shutdown
 */

#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <pthread.h>
#include <sched.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/ioctl.h>
#include <sys/mount.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/un.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "monitor_ioctl.h"

#define STACK_SIZE (1024 * 1024)
#define CONTAINER_ID_LEN 32
#define CONTROL_PATH "/tmp/mini_runtime.sock"
#define LOG_DIR "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN 256
#define LOG_CHUNK_SIZE 4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT (40UL << 20)
#define DEFAULT_HARD_LIMIT (64UL << 20)

typedef enum {
    CMD_SUPERVISOR = 0,
    CMD_START,
    CMD_RUN,
    CMD_PS,
    CMD_LOGS,
    CMD_STOP
} command_kind_t;

typedef enum {
    CONTAINER_STARTING = 0,
    CONTAINER_RUNNING,
    CONTAINER_STOPPED,
    CONTAINER_KILLED,
    CONTAINER_EXITED
} container_state_t;

typedef struct container_record {
    char id[CONTAINER_ID_LEN];
    pid_t host_pid;
    time_t started_at;
    container_state_t state;
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int exit_code;
    int exit_signal;
    char log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char container_id[CONTAINER_ID_LEN];
    size_t length;
    char data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t items[LOG_BUFFER_CAPACITY];
    size_t head;
    size_t tail;
    size_t count;
    int shutting_down;
    pthread_mutex_t mutex;
    pthread_cond_t not_empty;
    pthread_cond_t not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char container_id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    unsigned long soft_limit_bytes;
    unsigned long hard_limit_bytes;
    int nice_value;
} control_request_t;

typedef struct {
    int status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char id[CONTAINER_ID_LEN];
    char rootfs[PATH_MAX];
    char command[CHILD_COMMAND_LEN];
    int nice_value;
    int log_write_fd;
} child_config_t;

typedef struct {
    int server_fd;
    int monitor_fd;
    int should_stop;
    pthread_t logger_thread;
    bounded_buffer_t log_buffer;
    pthread_mutex_t metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* Global flag set by SIGCHLD / SIGTERM handlers */
static volatile sig_atomic_t g_stop_flag = 0;
static volatile sig_atomic_t g_child_flag = 0;

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s ps\n"
            "  %s logs <id>\n"
            "  %s stop <id>\n",
            prog, prog, prog, prog, prog, prog);
}

static int parse_mib_flag(const char *flag,
                          const char *value,
                          unsigned long *target_bytes)
{
    char *end = NULL;
    unsigned long mib;

    errno = 0;
    mib = strtoul(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0') {
        fprintf(stderr, "Invalid value for %s: %s\n", flag, value);
        return -1;
    }

    if (mib > ULONG_MAX / (1UL << 20)) {
        fprintf(stderr, "Value for %s is too large: %s\n", flag, value);
        return -1;
    }

    *target_bytes = mib * (1UL << 20);
    return 0;
}

static int parse_optional_flags(control_request_t *req,
                                int argc,
                                char *argv[],
                                int start_index)
{
    int i;

    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }

        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i + 1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i + 1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }

        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i + 1], &end, 10);
            if (errno != 0 || end == argv[i + 1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i + 1]);
                return -1;
            }
            req->nice_value = (int)nice_value;
            continue;
        }

        fprintf(stderr, "Unknown option: %s\n", argv[i]);
        return -1;
    }

    if (req->soft_limit_bytes > req->hard_limit_bytes) {
        fprintf(stderr, "Invalid limits: soft limit cannot exceed hard limit\n");
        return -1;
    }

    return 0;
}

static const char *state_to_string(container_state_t state)
{
    switch (state) {
    case CONTAINER_STARTING:
        return "starting";
    case CONTAINER_RUNNING:
        return "running";
    case CONTAINER_STOPPED:
        return "stopped";
    case CONTAINER_KILLED:
        return "killed";
    case CONTAINER_EXITED:
        return "exited";
    default:
        return "unknown";
    }
}

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;

    memset(buffer, 0, sizeof(*buffer));

    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0)
        return rc;

    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) {
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    rc = pthread_cond_init(&buffer->not_full, NULL);
    if (rc != 0) {
        pthread_cond_destroy(&buffer->not_empty);
        pthread_mutex_destroy(&buffer->mutex);
        return rc;
    }

    return 0;
}

static void bounded_buffer_destroy(bounded_buffer_t *buffer)
{
    pthread_cond_destroy(&buffer->not_full);
    pthread_cond_destroy(&buffer->not_empty);
    pthread_mutex_destroy(&buffer->mutex);
}

static void bounded_buffer_begin_shutdown(bounded_buffer_t *buffer)
{
    pthread_mutex_lock(&buffer->mutex);
    buffer->shutting_down = 1;
    pthread_cond_broadcast(&buffer->not_empty);
    pthread_cond_broadcast(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
}

int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is full and not shutting down */
    while (buffer->count >= LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    /* If shutting down and buffer is full, reject */
    if (buffer->shutting_down && buffer->count >= LOG_BUFFER_CAPACITY) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    /* Insert at tail */
    memcpy(&buffer->items[buffer->tail], item, sizeof(log_item_t));
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    /* Signal consumers that data is available */
    pthread_cond_signal(&buffer->not_empty);

    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while buffer is empty */
    while (buffer->count == 0) {
        if (buffer->shutting_down) {
            pthread_mutex_unlock(&buffer->mutex);
            return 1; /* Shutdown signal: drain complete */
        }
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);
    }

    /* Extract from head */
    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    /* Wake producers */
    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);

    return 0; /* Item retrieved successfully */
}

void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t item;
    FILE *log_file;
    char log_path[PATH_MAX];
    int rc;

    while (1) {
        rc = bounded_buffer_pop(&ctx->log_buffer, &item);

        if (rc == 1)
            break; /* Shutdown: buffer drained */

        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, item.container_id);
        log_file = fopen(log_path, "a");
        if (log_file == NULL) {
            perror("fopen log file");
            continue;
        }

        fwrite(item.data, 1, item.length, log_file);
        fflush(log_file);
        fclose(log_file);
    }

    return NULL;
}

int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Set hostname in UTS namespace */
    if (sethostname(cfg->id, strlen(cfg->id)) < 0) {
        perror("sethostname");
        return 1;
    }

    /* Change into and chroot to rootfs */
    if (chdir(cfg->rootfs) < 0) {
        perror("chdir rootfs");
        return 1;
    }

    if (chroot(cfg->rootfs) < 0) {
        perror("chroot");
        return 1;
    }

    /* Mount /proc inside container */
    if (mount("proc", "/proc", "proc", MS_NOEXEC | MS_NOSUID | MS_NODEV, NULL) < 0) {
        perror("mount /proc");
        return 1;
    }

    /* Redirect stdout/stderr to the pipe write end */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0 ||
        dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2");
        return 1;
    }
    close(cfg->log_write_fd);

    /* Apply nice value if requested */
    if (cfg->nice_value != 0) {
        if (nice(cfg->nice_value) < 0) {
            perror("nice");
            return 1;
        }
    }

    /* Execute command through /bin/sh */
    execl("/bin/sh", "sh", "-c", cfg->command, NULL);
    perror("execl");
    return 1;
}

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    req.soft_limit_bytes = soft_limit_bytes;
    req.hard_limit_bytes = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;

    return 0;
}

int unregister_from_monitor(int monitor_fd, const char *container_id, pid_t host_pid)
{
    struct monitor_request req;

    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);

    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;

    return 0;
}

/* ---------------------------------------------------------------
 * Signal handlers — set flags only; actual work is in event loop.
 * --------------------------------------------------------------- */
static void handle_sigterm(int sig)
{
    (void)sig;
    g_stop_flag = 1;
}

static void handle_sigchld(int sig)
{
    (void)sig;
    g_child_flag = 1;
}

/* ---------------------------------------------------------------
 * Reap all exited children and update container metadata.
 * --------------------------------------------------------------- */
static void reap_children(supervisor_ctx_t *ctx)
{
    int status;
    pid_t pid;

    while ((pid = waitpid(-1, &status, WNOHANG)) > 0) {
        container_record_t *rec;

        pthread_mutex_lock(&ctx->metadata_lock);
        for (rec = ctx->containers; rec != NULL; rec = rec->next) {
            if (rec->host_pid == pid) {
                if (WIFSIGNALED(status)) {
                    rec->state      = CONTAINER_KILLED;
                    rec->exit_signal = WTERMSIG(status);
                } else {
                    rec->state     = CONTAINER_EXITED;
                    rec->exit_code = WEXITSTATUS(status);
                }

                /* Unregister from kernel monitor */
                if (ctx->monitor_fd >= 0)
                    unregister_from_monitor(ctx->monitor_fd, rec->id, pid);

                break;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);
    }
}

/* ---------------------------------------------------------------
 * Handle CMD_START / CMD_RUN inside the supervisor event loop.
 * Returns the newly created child PID, or -1 on error.
 * --------------------------------------------------------------- */
static pid_t launch_container(supervisor_ctx_t *ctx,
                              const control_request_t *req,
                              control_response_t *resp)
{
    char *stack;
    child_config_t child_cfg;
    char log_path[PATH_MAX];
    pid_t child_pid;

    stack = malloc(STACK_SIZE);
    if (!stack) {
        resp->status = -ENOMEM;
        strncpy(resp->message, "malloc stack failed", sizeof(resp->message) - 1);
        return -1;
    }

    memset(&child_cfg, 0, sizeof(child_cfg));
    strncpy(child_cfg.id,      req->container_id, sizeof(child_cfg.id) - 1);
    strncpy(child_cfg.rootfs,  req->rootfs,        sizeof(child_cfg.rootfs) - 1);
    strncpy(child_cfg.command, req->command,        sizeof(child_cfg.command) - 1);
    child_cfg.nice_value = req->nice_value;

    snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req->container_id);
    child_cfg.log_write_fd = open(log_path, O_CREAT | O_WRONLY | O_APPEND, 0644);
    if (child_cfg.log_write_fd < 0) {
        perror("open log file");
        resp->status = -errno;
        strncpy(resp->message, "failed to open log file", sizeof(resp->message) - 1);
        free(stack);
        return -1;
    }

    child_pid = clone(child_fn, stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      &child_cfg);
    close(child_cfg.log_write_fd);

    if (child_pid < 0) {
        perror("clone");
        resp->status = -errno;
        strncpy(resp->message, "clone failed", sizeof(resp->message) - 1);
        free(stack);
        return -1;
    }

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0 &&
        register_with_monitor(ctx->monitor_fd, req->container_id, child_pid,
                              req->soft_limit_bytes, req->hard_limit_bytes) < 0) {
        perror("register_with_monitor");
        /* Non-fatal: container still runs, just unmonitored */
    }

    /* Add metadata record */
    {
        container_record_t *record = malloc(sizeof(*record));
        if (record) {
            memset(record, 0, sizeof(*record));
            strncpy(record->id, req->container_id, sizeof(record->id) - 1);
            record->host_pid          = child_pid;
            record->state             = CONTAINER_RUNNING;
            record->started_at        = time(NULL);
            record->soft_limit_bytes  = req->soft_limit_bytes;
            record->hard_limit_bytes  = req->hard_limit_bytes;
            snprintf(record->log_path, sizeof(record->log_path),
                     "%s/%s.log", LOG_DIR, req->container_id);

            pthread_mutex_lock(&ctx->metadata_lock);
            record->next    = ctx->containers;
            ctx->containers = record;
            pthread_mutex_unlock(&ctx->metadata_lock);
        }
    }

    resp->status = 0;
    snprintf(resp->message, sizeof(resp->message),
             "Container %s started: pid=%d", req->container_id, child_pid);

    free(stack);
    return child_pid;
}

/* ---------------------------------------------------------------
 * Handle one accepted client connection inside the event loop.
 * --------------------------------------------------------------- */
static void handle_client(supervisor_ctx_t *ctx, int client_fd)
{
    control_request_t req;
    control_response_t resp;
    ssize_t n;

    n = recv(client_fd, &req, sizeof(req), MSG_WAITALL);
    if (n <= 0) {
        if (n < 0)
            perror("recv");
        close(client_fd);
        return;
    }

    memset(&resp, 0, sizeof(resp));

    switch (req.kind) {

    /* ---- START ---- */
    case CMD_START:
        launch_container(ctx, &req, &resp);
        send(client_fd, &resp, sizeof(resp), 0);
        break;

    /* ---- RUN (blocking) ---- */
    case CMD_RUN: {
        pid_t child_pid = launch_container(ctx, &req, &resp);
        send(client_fd, &resp, sizeof(resp), 0);

        if (child_pid > 0) {
            int status;
            waitpid(child_pid, &status, 0);

            /* Update metadata */
            pthread_mutex_lock(&ctx->metadata_lock);
            {
                container_record_t *rec;
                for (rec = ctx->containers; rec != NULL; rec = rec->next) {
                    if (rec->host_pid == child_pid) {
                        if (WIFSIGNALED(status)) {
                            rec->state       = CONTAINER_KILLED;
                            rec->exit_signal = WTERMSIG(status);
                        } else {
                            rec->state     = CONTAINER_EXITED;
                            rec->exit_code = WEXITSTATUS(status);
                        }
                        break;
                    }
                }
            }
            pthread_mutex_unlock(&ctx->metadata_lock);

            unregister_from_monitor(ctx->monitor_fd, req.container_id, child_pid);
        }
        break;
    }

    /* ---- PS ---- */
    case CMD_PS: {
        char buf[CONTROL_MESSAGE_LEN];
        char line[128];
        container_record_t *rec;

        buf[0] = '\0';
        pthread_mutex_lock(&ctx->metadata_lock);
        for (rec = ctx->containers; rec != NULL; rec = rec->next) {
            snprintf(line, sizeof(line), "%-16s %-8d %-12s soft=%luMiB hard=%luMiB\n",
                     rec->id,
                     rec->host_pid,
                     state_to_string(rec->state),
                     rec->soft_limit_bytes >> 20,
                     rec->hard_limit_bytes >> 20);
            strncat(buf, line, sizeof(buf) - strlen(buf) - 1);
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (buf[0] == '\0')
            strncpy(buf, "(no containers)", sizeof(buf) - 1);

        resp.status = 0;
        strncpy(resp.message, buf, sizeof(resp.message) - 1);
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    /* ---- LOGS ---- */
    case CMD_LOGS: {
        char log_path[PATH_MAX];
        FILE *lf;

        snprintf(log_path, sizeof(log_path), "%s/%s.log", LOG_DIR, req.container_id);
        lf = fopen(log_path, "r");
        if (!lf) {
            resp.status = -ENOENT;
            snprintf(resp.message, sizeof(resp.message),
                     "No log for container '%s'", req.container_id);
            send(client_fd, &resp, sizeof(resp), 0);
            break;
        }

        /* Send the log file content in chunks via the response message field */
        resp.status = 0;
        while (fgets(resp.message, sizeof(resp.message), lf) != NULL)
            send(client_fd, &resp, sizeof(resp), 0);

        fclose(lf);

        /* Send a sentinel with an empty message to signal end-of-logs */
        memset(&resp, 0, sizeof(resp));
        resp.status = 1; /* EOF sentinel */
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    /* ---- STOP ---- */
    case CMD_STOP: {
        container_record_t *rec;
        int found = 0;

        pthread_mutex_lock(&ctx->metadata_lock);
        for (rec = ctx->containers; rec != NULL; rec = rec->next) {
            if (strcmp(rec->id, req.container_id) == 0 &&
                rec->state == CONTAINER_RUNNING) {
                kill(rec->host_pid, SIGTERM);
                rec->state = CONTAINER_STOPPED;
                found = 1;
                break;
            }
        }
        pthread_mutex_unlock(&ctx->metadata_lock);

        if (found) {
            resp.status = 0;
            snprintf(resp.message, sizeof(resp.message),
                     "Sent SIGTERM to container '%s'", req.container_id);
        } else {
            resp.status = -ENOENT;
            snprintf(resp.message, sizeof(resp.message),
                     "Container '%s' not found or not running", req.container_id);
        }
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    default:
        resp.status = -EINVAL;
        strncpy(resp.message, "Unknown command", sizeof(resp.message) - 1);
        send(client_fd, &resp, sizeof(resp), 0);
        break;
    }

    close(client_fd);
}

/* ---------------------------------------------------------------
 * Supervisor: long-running process that owns all container state.
 * --------------------------------------------------------------- */
static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    struct sockaddr_un addr;
    struct sigaction sa;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;

    /* --- Signal handling --- */
    memset(&sa, 0, sizeof(sa));
    sa.sa_flags = SA_RESTART;
    sigemptyset(&sa.sa_mask);

    sa.sa_handler = handle_sigterm;
    sigaction(SIGTERM, &sa, NULL);
    sigaction(SIGINT,  &sa, NULL);

    sa.sa_handler = handle_sigchld;
    sa.sa_flags   = SA_RESTART | SA_NOCLDSTOP;
    sigaction(SIGCHLD, &sa, NULL);

    /* Ignore SIGPIPE so broken client connections don't kill supervisor */
    signal(SIGPIPE, SIG_IGN);

    /* --- Init synchronisation primitives --- */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) {
        errno = rc;
        perror("pthread_mutex_init");
        return 1;
    }

    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) {
        errno = rc;
        perror("bounded_buffer_init");
        pthread_mutex_destroy(&ctx.metadata_lock);
        return 1;
    }

    /* --- Open kernel monitor device --- */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0) {
        perror("open /dev/container_monitor");
        /* Non-fatal in environments where the module isn't loaded */
    }

    /* --- Create control socket --- */
    unlink(CONTROL_PATH);
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) {
        perror("socket");
        goto cleanup;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        goto cleanup;
    }

    if (listen(ctx.server_fd, 5) < 0) {
        perror("listen");
        goto cleanup;
    }

    /* --- Create log directory --- */
    mkdir(LOG_DIR, 0755);

    /* --- Start logging thread --- */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) {
        errno = rc;
        perror("pthread_create logger");
        goto cleanup;
    }

    printf("[supervisor] running on %s (rootfs=%s)\n", CONTROL_PATH, rootfs);
    fflush(stdout);

    /* --- Event loop --- */
    while (!g_stop_flag) {
        fd_set rfds;
        struct timeval tv;
        int sel;

        /* Reap any children that have exited */
        if (g_child_flag) {
            g_child_flag = 0;
            reap_children(&ctx);
        }

        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);

        tv.tv_sec  = 1;
        tv.tv_usec = 0;

        sel = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR)
                continue;
            perror("select");
            break;
        }

        if (sel > 0 && FD_ISSET(ctx.server_fd, &rfds)) {
            int client_fd = accept(ctx.server_fd, NULL, NULL);
            if (client_fd < 0) {
                if (errno == EINTR)
                    continue;
                perror("accept");
                continue;
            }
            handle_client(&ctx, client_fd);
        }
    }

    /* --- Orderly shutdown --- */
    printf("[supervisor] shutting down\n");
    fflush(stdout);

    /* SIGTERM all still-running containers */
    {
        container_record_t *rec;
        pthread_mutex_lock(&ctx.metadata_lock);
        for (rec = ctx.containers; rec != NULL; rec = rec->next) {
            if (rec->state == CONTAINER_RUNNING) {
                kill(rec->host_pid, SIGTERM);
                rec->state = CONTAINER_STOPPED;
            }
        }
        pthread_mutex_unlock(&ctx.metadata_lock);
    }

    /* Wait for all container children */
    while (waitpid(-1, NULL, WNOHANG) > 0)
        ;

cleanup:
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    if (ctx.logger_thread)
        pthread_join(ctx.logger_thread, NULL);
    bounded_buffer_destroy(&ctx.log_buffer);

    /* Free metadata list */
    {
        container_record_t *rec = ctx.containers;
        while (rec) {
            container_record_t *next = rec->next;
            free(rec);
            rec = next;
        }
    }

    pthread_mutex_destroy(&ctx.metadata_lock);

    if (ctx.server_fd >= 0)
        close(ctx.server_fd);
    if (ctx.monitor_fd >= 0)
        close(ctx.monitor_fd);
    unlink(CONTROL_PATH);

    return 0;
}

/* ---------------------------------------------------------------
 * Client helpers
 * --------------------------------------------------------------- */
static int send_control_request(const control_request_t *req)
{
    int sock;
    struct sockaddr_un addr;
    control_response_t resp;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    if (send(sock, (void *)req, sizeof(*req), 0) < 0) {
        perror("send");
        close(sock);
        return 1;
    }

    memset(&resp, 0, sizeof(resp));
    if (recv(sock, &resp, sizeof(resp), 0) < 0) {
        perror("recv");
        close(sock);
        return 1;
    }

    if (resp.status < 0)
        fprintf(stderr, "Error: %s\n", resp.message);
    else
        printf("%s\n", resp.message);

    close(sock);
    return resp.status < 0 ? 1 : 0;
}

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n",
                argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs, argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command, argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;

    if (parse_optional_flags(&req, argc, argv, 5) != 0)
        return 1;

    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    control_response_t resp;
    int sock;
    struct sockaddr_un addr;

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;

    if (send(sock, &req, sizeof(req), 0) < 0) {
        perror("send");
        close(sock);
        return 1;
    }

    memset(&resp, 0, sizeof(resp));
    if (recv(sock, &resp, sizeof(resp), 0) < 0) {
        perror("recv");
        close(sock);
        return 1;
    }

    printf("%-16s %-8s %-12s %s\n", "ID", "PID", "STATE", "LIMITS");
    printf("%-16s %-8s %-12s %s\n", "---", "---", "-----", "------");
    printf("%s\n", resp.message);

    close(sock);
    return resp.status < 0 ? 1 : 0;
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    control_response_t resp;
    int sock;
    struct sockaddr_un addr;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }

    sock = socket(AF_UNIX, SOCK_STREAM, 0);
    if (sock < 0) {
        perror("socket");
        return 1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(sock, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("connect");
        close(sock);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    if (send(sock, &req, sizeof(req), 0) < 0) {
        perror("send");
        close(sock);
        return 1;
    }

    /* Stream log lines until sentinel (status == 1) */
    while (recv(sock, &resp, sizeof(resp), MSG_WAITALL) > 0) {
        if (resp.status == 1)  /* EOF sentinel */
            break;
        if (resp.status < 0) {
            fprintf(stderr, "Error: %s\n", resp.message);
            close(sock);
            return 1;
        }
        printf("%s", resp.message);
    }

    close(sock);
    return 0;
}

static int cmd_stop(int argc, char *argv[])
{
    control_request_t req;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s stop <id>\n", argv[0]);
        return 1;
    }

    memset(&req, 0, sizeof(req));
    req.kind = CMD_STOP;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);

    return send_control_request(&req);
}

int main(int argc, char *argv[])
{
    if (argc < 2) {
        usage(argv[0]);
        return 1;
    }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }

    if (strcmp(argv[1], "start") == 0)
        return cmd_start(argc, argv);

    if (strcmp(argv[1], "run") == 0)
        return cmd_run(argc, argv);

    if (strcmp(argv[1], "ps") == 0)
        return cmd_ps();

    if (strcmp(argv[1], "logs") == 0)
        return cmd_logs(argc, argv);

    if (strcmp(argv[1], "stop") == 0)
        return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}
