/*
 * engine.c - Supervised Multi-Container Runtime (User Space)
 *
 * Complete implementation:
 *   - UNIX domain socket control-plane IPC
 *   - container lifecycle with clone + namespaces
 *   - bounded-buffer producer/consumer logging
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

#define STACK_SIZE          (1024 * 1024)
#define CONTAINER_ID_LEN    32
#define CONTROL_PATH        "/tmp/mini_runtime.sock"
#define LOG_DIR             "logs"
#define CONTROL_MESSAGE_LEN 256
#define CHILD_COMMAND_LEN   256
#define LOG_CHUNK_SIZE      4096
#define LOG_BUFFER_CAPACITY 16
#define DEFAULT_SOFT_LIMIT  (40UL << 20)
#define DEFAULT_HARD_LIMIT  (64UL << 20)

/* ------------------------------------------------------------------ */
/*  Enums & structs (unchanged from boilerplate)                       */
/* ------------------------------------------------------------------ */

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
    char               id[CONTAINER_ID_LEN];
    pid_t              host_pid;
    time_t             started_at;
    container_state_t  state;
    unsigned long      soft_limit_bytes;
    unsigned long      hard_limit_bytes;
    int                exit_code;
    int                exit_signal;
    int                stop_requested;   /* set before sending stop signal */
    char               log_path[PATH_MAX];
    struct container_record *next;
} container_record_t;

typedef struct {
    char   container_id[CONTAINER_ID_LEN];
    size_t length;
    char   data[LOG_CHUNK_SIZE];
} log_item_t;

typedef struct {
    log_item_t       items[LOG_BUFFER_CAPACITY];
    size_t           head;
    size_t           tail;
    size_t           count;
    int              shutting_down;
    pthread_mutex_t  mutex;
    pthread_cond_t   not_empty;
    pthread_cond_t   not_full;
} bounded_buffer_t;

typedef struct {
    command_kind_t kind;
    char           container_id[CONTAINER_ID_LEN];
    char           rootfs[PATH_MAX];
    char           command[CHILD_COMMAND_LEN];
    unsigned long  soft_limit_bytes;
    unsigned long  hard_limit_bytes;
    int            nice_value;
} control_request_t;

typedef struct {
    int  status;
    char message[CONTROL_MESSAGE_LEN];
} control_response_t;

typedef struct {
    char  id[CONTAINER_ID_LEN];
    char  rootfs[PATH_MAX];
    char  command[CHILD_COMMAND_LEN];
    int   nice_value;
    int   log_write_fd;
} child_config_t;

typedef struct {
    int                server_fd;
    int                monitor_fd;
    int                should_stop;
    pthread_t          logger_thread;
    bounded_buffer_t   log_buffer;
    pthread_mutex_t    metadata_lock;
    container_record_t *containers;
} supervisor_ctx_t;

/* producer thread arg */
typedef struct {
    int               read_fd;
    char              container_id[CONTAINER_ID_LEN];
    bounded_buffer_t *buffer;
} producer_arg_t;

/* ------------------------------------------------------------------ */
/*  Forward declarations                                               */
/* ------------------------------------------------------------------ */
static supervisor_ctx_t *g_ctx = NULL;  /* for signal handlers */

/* ------------------------------------------------------------------ */
/*  Usage / helpers                                                    */
/* ------------------------------------------------------------------ */

static void usage(const char *prog)
{
    fprintf(stderr,
            "Usage:\n"
            "  %s supervisor <base-rootfs>\n"
            "  %s start <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
            "  %s run   <id> <container-rootfs> <command> [--soft-mib N] [--hard-mib N] [--nice N]\n"
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
                                int argc, char *argv[], int start_index)
{
    int i;
    for (i = start_index; i < argc; i += 2) {
        char *end = NULL;
        long  nice_value;

        if (i + 1 >= argc) {
            fprintf(stderr, "Missing value for option: %s\n", argv[i]);
            return -1;
        }
        if (strcmp(argv[i], "--soft-mib") == 0) {
            if (parse_mib_flag("--soft-mib", argv[i+1], &req->soft_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--hard-mib") == 0) {
            if (parse_mib_flag("--hard-mib", argv[i+1], &req->hard_limit_bytes) != 0)
                return -1;
            continue;
        }
        if (strcmp(argv[i], "--nice") == 0) {
            errno = 0;
            nice_value = strtol(argv[i+1], &end, 10);
            if (errno != 0 || end == argv[i+1] || *end != '\0' ||
                nice_value < -20 || nice_value > 19) {
                fprintf(stderr,
                        "Invalid value for --nice (expected -20..19): %s\n",
                        argv[i+1]);
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
    case CONTAINER_STARTING: return "starting";
    case CONTAINER_RUNNING:  return "running";
    case CONTAINER_STOPPED:  return "stopped";
    case CONTAINER_KILLED:   return "killed";
    case CONTAINER_EXITED:   return "exited";
    default:                 return "unknown";
    }
}

/* ------------------------------------------------------------------ */
/*  Bounded buffer                                                     */
/* ------------------------------------------------------------------ */

static int bounded_buffer_init(bounded_buffer_t *buffer)
{
    int rc;
    memset(buffer, 0, sizeof(*buffer));
    rc = pthread_mutex_init(&buffer->mutex, NULL);
    if (rc != 0) return rc;
    rc = pthread_cond_init(&buffer->not_empty, NULL);
    if (rc != 0) { pthread_mutex_destroy(&buffer->mutex); return rc; }
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

/*
 * bounded_buffer_push — producer side.
 * Blocks when buffer is full; wakes consumer on success.
 * Returns 0 on success, -1 if shutting down.
 */
int bounded_buffer_push(bounded_buffer_t *buffer, const log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while full, unless we are shutting down */
    while (buffer->count == LOG_BUFFER_CAPACITY && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_full, &buffer->mutex);

    if (buffer->shutting_down) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    buffer->items[buffer->tail] = *item;
    buffer->tail = (buffer->tail + 1) % LOG_BUFFER_CAPACITY;
    buffer->count++;

    pthread_cond_signal(&buffer->not_empty);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/*
 * bounded_buffer_pop — consumer side.
 * Blocks when buffer is empty; returns -1 when shutdown and empty.
 */
int bounded_buffer_pop(bounded_buffer_t *buffer, log_item_t *item)
{
    pthread_mutex_lock(&buffer->mutex);

    /* Wait while empty, unless shutting down */
    while (buffer->count == 0 && !buffer->shutting_down)
        pthread_cond_wait(&buffer->not_empty, &buffer->mutex);

    /* If still empty after wakeup, we are done */
    if (buffer->count == 0) {
        pthread_mutex_unlock(&buffer->mutex);
        return -1;
    }

    *item = buffer->items[buffer->head];
    buffer->head = (buffer->head + 1) % LOG_BUFFER_CAPACITY;
    buffer->count--;

    pthread_cond_signal(&buffer->not_full);
    pthread_mutex_unlock(&buffer->mutex);
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Logging consumer thread                                            */
/* ------------------------------------------------------------------ */

/*
 * logging_thread — drains the bounded buffer and writes each chunk
 * to the correct per-container log file under logs/.
 */
void *logging_thread(void *arg)
{
    supervisor_ctx_t *ctx = (supervisor_ctx_t *)arg;
    log_item_t        item;

    /* Make sure the logs directory exists */
    mkdir(LOG_DIR, 0755);

    while (bounded_buffer_pop(&ctx->log_buffer, &item) == 0) {
        char path[PATH_MAX];
        int  fd;

        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            write(fd, item.data, item.length);
            close(fd);
        }
    }

    /* Drain anything left after shutdown signal */
    while (1) {
        pthread_mutex_lock(&ctx->log_buffer.mutex);
        if (ctx->log_buffer.count == 0) {
            pthread_mutex_unlock(&ctx->log_buffer.mutex);
            break;
        }
        item = ctx->log_buffer.items[ctx->log_buffer.head];
        ctx->log_buffer.head = (ctx->log_buffer.head + 1) % LOG_BUFFER_CAPACITY;
        ctx->log_buffer.count--;
        pthread_mutex_unlock(&ctx->log_buffer.mutex);

        char path[PATH_MAX];
        int  fd;
        snprintf(path, sizeof(path), "%s/%s.log", LOG_DIR, item.container_id);
        fd = open(path, O_WRONLY | O_CREAT | O_APPEND, 0644);
        if (fd >= 0) {
            write(fd, item.data, item.length);
            close(fd);
        }
    }

    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Producer thread — reads container pipe, pushes to buffer          */
/* ------------------------------------------------------------------ */

void *producer_thread(void *arg)
{
    producer_arg_t *parg = (producer_arg_t *)arg;
    log_item_t      item;
    ssize_t         n;

    memset(&item, 0, sizeof(item));
    strncpy(item.container_id, parg->container_id, CONTAINER_ID_LEN - 1);

    while ((n = read(parg->read_fd, item.data, LOG_CHUNK_SIZE - 1)) > 0) {
        item.length = (size_t)n;
        item.data[n] = '\0';
        bounded_buffer_push(parg->buffer, &item);
        memset(item.data, 0, LOG_CHUNK_SIZE);
    }

    close(parg->read_fd);
    free(parg);
    return NULL;
}

/* ------------------------------------------------------------------ */
/*  Container child entry point                                        */
/* ------------------------------------------------------------------ */

/*
 * child_fn — runs inside the cloned container process.
 *
 * Sets up:
 *   - chroot into assigned rootfs
 *   - /proc mount so ps/top work inside the container
 *   - stdout/stderr redirected through the log pipe
 *   - execv of the requested command
 */
int child_fn(void *arg)
{
    child_config_t *cfg = (child_config_t *)arg;

    /* Apply nice value if requested */
    if (cfg->nice_value != 0)
        nice(cfg->nice_value);

    /* chroot into the container rootfs */
    if (chroot(cfg->rootfs) != 0) {
        perror("chroot");
        return 1;
    }
    if (chdir("/") != 0) {
        perror("chdir /");
        return 1;
    }

    /* Mount /proc so tools like ps work inside */
    mkdir("/proc", 0555);
    if (mount("proc", "/proc", "proc", 0, NULL) != 0) {
        /* Non-fatal — /proc may already exist in some rootfs */
        perror("mount /proc (non-fatal)");
    }

    /* Redirect stdout and stderr into the supervisor log pipe */
    if (dup2(cfg->log_write_fd, STDOUT_FILENO) < 0) {
        perror("dup2 stdout");
        return 1;
    }
    if (dup2(cfg->log_write_fd, STDERR_FILENO) < 0) {
        perror("dup2 stderr");
        return 1;
    }
    close(cfg->log_write_fd);

    /* Execute the requested command */
    char *argv_exec[] = { cfg->command, NULL };
    execv(cfg->command, argv_exec);

    /* If execv returns, it failed */
    perror("execv");
    return 1;
}

/* ------------------------------------------------------------------ */
/*  Monitor helpers                                                    */
/* ------------------------------------------------------------------ */

int register_with_monitor(int monitor_fd,
                          const char *container_id,
                          pid_t host_pid,
                          unsigned long soft_limit_bytes,
                          unsigned long hard_limit_bytes)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid               = host_pid;
    req.soft_limit_bytes  = soft_limit_bytes;
    req.hard_limit_bytes  = hard_limit_bytes;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_REGISTER, &req) < 0)
        return -1;
    return 0;
}

int unregister_from_monitor(int monitor_fd,
                            const char *container_id,
                            pid_t host_pid)
{
    struct monitor_request req;
    memset(&req, 0, sizeof(req));
    req.pid = host_pid;
    strncpy(req.container_id, container_id, sizeof(req.container_id) - 1);
    if (ioctl(monitor_fd, MONITOR_UNREGISTER, &req) < 0)
        return -1;
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Metadata helpers (must hold ctx->metadata_lock)                   */
/* ------------------------------------------------------------------ */

static container_record_t *find_container(supervisor_ctx_t *ctx,
                                          const char *id)
{
    container_record_t *c = ctx->containers;
    while (c) {
        if (strcmp(c->id, id) == 0)
            return c;
        c = c->next;
    }
    return NULL;
}

static void add_container(supervisor_ctx_t *ctx, container_record_t *rec)
{
    rec->next       = ctx->containers;
    ctx->containers = rec;
}

/* ------------------------------------------------------------------ */
/*  SIGCHLD handler — reaps children, updates metadata                */
/* ------------------------------------------------------------------ */

static void sigchld_handler(int sig)
{
    (void)sig;
    int   wstatus;
    pid_t pid;

    while ((pid = waitpid(-1, &wstatus, WNOHANG)) > 0) {
        if (!g_ctx) continue;

        pthread_mutex_lock(&g_ctx->metadata_lock);
        container_record_t *c = g_ctx->containers;
        while (c) {
            if (c->host_pid == pid) {
                if (WIFEXITED(wstatus)) {
                    c->exit_code = WEXITSTATUS(wstatus);
                    c->state     = CONTAINER_EXITED;
                } else if (WIFSIGNALED(wstatus)) {
                    c->exit_signal = WTERMSIG(wstatus);
                    if (c->stop_requested)
                        c->state = CONTAINER_STOPPED;
                    else if (c->exit_signal == SIGKILL)
                        c->state = CONTAINER_KILLED;  /* hard-limit kill */
                    else
                        c->state = CONTAINER_EXITED;
                }
                /* Unregister from kernel monitor */
                if (g_ctx->monitor_fd >= 0)
                    unregister_from_monitor(g_ctx->monitor_fd,
                                            c->id, c->host_pid);
                break;
            }
            c = c->next;
        }
        pthread_mutex_unlock(&g_ctx->metadata_lock);
    }
}

static void sigterm_handler(int sig)
{
    (void)sig;
    if (g_ctx)
        g_ctx->should_stop = 1;
}

/* ------------------------------------------------------------------ */
/*  Launch a container (called from supervisor event loop)            */
/* ------------------------------------------------------------------ */

static container_record_t *launch_container(supervisor_ctx_t *ctx,
                                            const control_request_t *req)
{
    int pipefd[2];
    if (pipe(pipefd) != 0) {
        perror("pipe");
        return NULL;
    }

    /* Allocate stack for clone */
    char *stack = malloc(STACK_SIZE);
    if (!stack) {
        close(pipefd[0]);
        close(pipefd[1]);
        return NULL;
    }

    /* Build child config on the heap so child can read it after clone */
    child_config_t *cfg = malloc(sizeof(child_config_t));
    if (!cfg) {
        free(stack);
        close(pipefd[0]);
        close(pipefd[1]);
        return NULL;
    }
    memset(cfg, 0, sizeof(*cfg));
    strncpy(cfg->id,      req->container_id, CONTAINER_ID_LEN - 1);
    strncpy(cfg->rootfs,  req->rootfs,        PATH_MAX - 1);
    strncpy(cfg->command, req->command,       CHILD_COMMAND_LEN - 1);
    cfg->nice_value    = req->nice_value;
    cfg->log_write_fd  = pipefd[1];

    /* Ensure logs directory exists */
    mkdir(LOG_DIR, 0755);

    pid_t pid = clone(child_fn,
                      stack + STACK_SIZE,
                      CLONE_NEWPID | CLONE_NEWUTS | CLONE_NEWNS | SIGCHLD,
                      cfg);

    /* Parent closes write end of pipe */
    close(pipefd[1]);

    if (pid < 0) {
        perror("clone");
        free(cfg);
        free(stack);
        close(pipefd[0]);
        return NULL;
    }

    /* Start producer thread for this container's pipe */
    producer_arg_t *parg = malloc(sizeof(producer_arg_t));
    if (parg) {
        parg->read_fd = pipefd[0];
        strncpy(parg->container_id, req->container_id, CONTAINER_ID_LEN - 1);
        parg->buffer  = &ctx->log_buffer;
        pthread_t pt;
        pthread_create(&pt, NULL, producer_thread, parg);
        pthread_detach(pt);
    } else {
        close(pipefd[0]);
    }

    /* Build container metadata record */
    container_record_t *rec = calloc(1, sizeof(container_record_t));
    if (!rec) {
        free(cfg);
        free(stack);
        return NULL;
    }
    strncpy(rec->id, req->container_id, CONTAINER_ID_LEN - 1);
    rec->host_pid         = pid;
    rec->started_at       = time(NULL);
    rec->state            = CONTAINER_RUNNING;
    rec->soft_limit_bytes = req->soft_limit_bytes;
    rec->hard_limit_bytes = req->hard_limit_bytes;
    rec->exit_code        = 0;
    rec->exit_signal      = 0;
    rec->stop_requested   = 0;
    snprintf(rec->log_path, PATH_MAX, "%s/%s.log", LOG_DIR, req->container_id);

    /* Register with kernel monitor */
    if (ctx->monitor_fd >= 0)
        register_with_monitor(ctx->monitor_fd,
                               req->container_id, pid,
                               req->soft_limit_bytes,
                               req->hard_limit_bytes);

    /* stack and cfg intentionally leaked — they are live for the child's
       lifetime.  A production runtime would track and free them in SIGCHLD. */
    (void)stack;

    return rec;
}

/* ------------------------------------------------------------------ */
/*  Supervisor event loop                                              */
/* ------------------------------------------------------------------ */

static int run_supervisor(const char *rootfs)
{
    supervisor_ctx_t ctx;
    int rc;

    memset(&ctx, 0, sizeof(ctx));
    ctx.server_fd  = -1;
    ctx.monitor_fd = -1;
    g_ctx = &ctx;

    /* Initialise metadata lock */
    rc = pthread_mutex_init(&ctx.metadata_lock, NULL);
    if (rc != 0) { errno = rc; perror("pthread_mutex_init"); return 1; }

    /* Initialise bounded log buffer */
    rc = bounded_buffer_init(&ctx.log_buffer);
    if (rc != 0) { errno = rc; perror("bounded_buffer_init");
                   pthread_mutex_destroy(&ctx.metadata_lock); return 1; }

    /* ---- 1) Open kernel monitor device (optional — skip if not loaded) --- */
    ctx.monitor_fd = open("/dev/container_monitor", O_RDWR);
    if (ctx.monitor_fd < 0)
        fprintf(stderr, "[supervisor] Warning: kernel monitor not available (%s)\n",
                strerror(errno));

    /* ---- 2) Create UNIX domain socket ------------------------------------ */
    ctx.server_fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (ctx.server_fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    unlink(CONTROL_PATH);   /* remove stale socket if any */

    if (bind(ctx.server_fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        perror("bind");
        close(ctx.server_fd);
        return 1;
    }
    if (listen(ctx.server_fd, 10) < 0) {
        perror("listen");
        close(ctx.server_fd);
        unlink(CONTROL_PATH);
        return 1;
    }

    /* ---- 3) Signal handling ---------------------------------------------- */
    struct sigaction sa_chld = { .sa_handler = sigchld_handler,
                                 .sa_flags   = SA_RESTART | SA_NOCLDSTOP };
    sigemptyset(&sa_chld.sa_mask);
    sigaction(SIGCHLD, &sa_chld, NULL);

    struct sigaction sa_term = { .sa_handler = sigterm_handler };
    sigemptyset(&sa_term.sa_mask);
    sigaction(SIGTERM, &sa_term, NULL);
    sigaction(SIGINT,  &sa_term, NULL);

    /* ---- 4) Spawn logger thread ------------------------------------------ */
    rc = pthread_create(&ctx.logger_thread, NULL, logging_thread, &ctx);
    if (rc != 0) { errno = rc; perror("pthread_create logger"); return 1; }

    fprintf(stderr, "[supervisor] Started. rootfs=%s  socket=%s\n",
            rootfs, CONTROL_PATH);

    /* ---- 5) Event loop ---------------------------------------------------- */
    while (!ctx.should_stop) {
        /* Use select() so SIGINT/SIGTERM can interrupt accept */
        fd_set rfds;
        FD_ZERO(&rfds);
        FD_SET(ctx.server_fd, &rfds);
        struct timeval tv = { .tv_sec = 1, .tv_usec = 0 };

        int sel = select(ctx.server_fd + 1, &rfds, NULL, NULL, &tv);
        if (sel < 0) {
            if (errno == EINTR) continue;
            perror("select");
            break;
        }
        if (sel == 0) continue;   /* timeout — check should_stop */

        int client_fd = accept(ctx.server_fd, NULL, NULL);
        if (client_fd < 0) {
            if (errno == EINTR) continue;
            perror("accept");
            continue;
        }

        /* Read the control request */
        control_request_t  req;
        control_response_t resp;
        memset(&resp, 0, sizeof(resp));

        ssize_t n = read(client_fd, &req, sizeof(req));
        if (n != (ssize_t)sizeof(req)) {
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "bad request");
            write(client_fd, &resp, sizeof(resp));
            close(client_fd);
            continue;
        }

        /* Dispatch */
        switch (req.kind) {

        case CMD_START:
        case CMD_RUN: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *existing = find_container(&ctx, req.container_id);
            if (existing && existing->state == CONTAINER_RUNNING) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "container %s already running", req.container_id);
                write(client_fd, &resp, sizeof(resp));
                break;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            container_record_t *rec = launch_container(&ctx, &req);
            if (!rec) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "failed to launch container %s", req.container_id);
            } else {
                pthread_mutex_lock(&ctx.metadata_lock);
                add_container(&ctx, rec);
                pthread_mutex_unlock(&ctx.metadata_lock);

                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message),
                         "started container %s pid=%d", req.container_id,
                         rec->host_pid);

                if (req.kind == CMD_RUN) {
                    /* For run: send initial response then wait */
                    write(client_fd, &resp, sizeof(resp));

                    int wstatus = 0;
                    waitpid(rec->host_pid, &wstatus, 0);

                    pthread_mutex_lock(&ctx.metadata_lock);
                    if (WIFEXITED(wstatus)) {
                        rec->exit_code = WEXITSTATUS(wstatus);
                        rec->state     = CONTAINER_EXITED;
                    } else if (WIFSIGNALED(wstatus)) {
                        rec->exit_signal = WTERMSIG(wstatus);
                        rec->state = rec->stop_requested ?
                                     CONTAINER_STOPPED : CONTAINER_KILLED;
                    }
                    pthread_mutex_unlock(&ctx.metadata_lock);

                    memset(&resp, 0, sizeof(resp));
                    resp.status = rec->exit_code;
                    snprintf(resp.message, sizeof(resp.message),
                             "container %s exited code=%d signal=%d",
                             req.container_id, rec->exit_code, rec->exit_signal);
                }
            }
            write(client_fd, &resp, sizeof(resp));
            break;
        }

        case CMD_PS: {
            /* Build a multi-line response in message buffer */
            char buf[4096];
            int  off = 0;
            off += snprintf(buf + off, sizeof(buf) - off,
                            "%-16s %-8s %-10s %-20s %-8s %-8s\n",
                            "ID", "PID", "STATE", "STARTED",
                            "SOFT_MiB", "HARD_MiB");

            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *c = ctx.containers;
            while (c && off < (int)sizeof(buf) - 1) {
                char tmbuf[20];
                struct tm *tm_info = localtime(&c->started_at);
                strftime(tmbuf, sizeof(tmbuf), "%Y-%m-%d %H:%M:%S", tm_info);
                off += snprintf(buf + off, sizeof(buf) - off,
                                "%-16s %-8d %-10s %-20s %-8lu %-8lu\n",
                                c->id, c->host_pid,
                                state_to_string(c->state),
                                tmbuf,
                                c->soft_limit_bytes >> 20,
                                c->hard_limit_bytes >> 20);
                c = c->next;
            }
            pthread_mutex_unlock(&ctx.metadata_lock);

            resp.status = 0;
            /* Write raw table directly so it fits */
            write(client_fd, buf, off);
            close(client_fd);
            client_fd = -1;
            break;
        }

        case CMD_LOGS: {
            char path[PATH_MAX];
            snprintf(path, sizeof(path), "%s/%s.log",
                     LOG_DIR, req.container_id);
            int lfd = open(path, O_RDONLY);
            if (lfd < 0) {
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "no log for container %s", req.container_id);
                write(client_fd, &resp, sizeof(resp));
            } else {
                char chunk[4096];
                ssize_t nr;
                while ((nr = read(lfd, chunk, sizeof(chunk))) > 0)
                    write(client_fd, chunk, nr);
                close(lfd);
            }
            close(client_fd);
            client_fd = -1;
            break;
        }

        case CMD_STOP: {
            pthread_mutex_lock(&ctx.metadata_lock);
            container_record_t *c = find_container(&ctx, req.container_id);
            if (!c || c->state != CONTAINER_RUNNING) {
                pthread_mutex_unlock(&ctx.metadata_lock);
                resp.status = -1;
                snprintf(resp.message, sizeof(resp.message),
                         "container %s not running", req.container_id);
            } else {
                c->stop_requested = 1;
                pid_t pid = c->host_pid;
                pthread_mutex_unlock(&ctx.metadata_lock);

                kill(pid, SIGTERM);
                /* Give it 2 s to exit gracefully, then SIGKILL */
                usleep(2000000);
                kill(pid, SIGKILL);

                resp.status = 0;
                snprintf(resp.message, sizeof(resp.message),
                         "sent stop to container %s", req.container_id);
            }
            write(client_fd, &resp, sizeof(resp));
            break;
        }

        default:
            resp.status = -1;
            snprintf(resp.message, sizeof(resp.message), "unknown command");
            write(client_fd, &resp, sizeof(resp));
            break;
        }

        if (client_fd >= 0)
            close(client_fd);
    }

    /* ---- Orderly shutdown ------------------------------------------------- */
    fprintf(stderr, "[supervisor] Shutting down...\n");

    /* Stop all running containers */
    pthread_mutex_lock(&ctx.metadata_lock);
    container_record_t *c = ctx.containers;
    while (c) {
        if (c->state == CONTAINER_RUNNING) {
            c->stop_requested = 1;
            kill(c->host_pid, SIGTERM);
        }
        c = c->next;
    }
    pthread_mutex_unlock(&ctx.metadata_lock);
    sleep(2);

    /* Shutdown logging pipeline */
    bounded_buffer_begin_shutdown(&ctx.log_buffer);
    pthread_join(ctx.logger_thread, NULL);

    /* Free metadata list */
    pthread_mutex_lock(&ctx.metadata_lock);
    c = ctx.containers;
    while (c) {
        container_record_t *next = c->next;
        free(c);
        c = next;
    }
    ctx.containers = NULL;
    pthread_mutex_unlock(&ctx.metadata_lock);

    /* Cleanup resources */
    bounded_buffer_destroy(&ctx.log_buffer);
    pthread_mutex_destroy(&ctx.metadata_lock);
    if (ctx.monitor_fd >= 0) close(ctx.monitor_fd);
    if (ctx.server_fd  >= 0) close(ctx.server_fd);
    unlink(CONTROL_PATH);

    fprintf(stderr, "[supervisor] Clean exit.\n");
    return 0;
}

/* ------------------------------------------------------------------ */
/*  Client-side: send control request to supervisor                   */
/* ------------------------------------------------------------------ */

static int send_control_request(const control_request_t *req)
{
    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) { perror("socket"); return 1; }

    struct sockaddr_un addr;
    memset(&addr, 0, sizeof(addr));
    addr.sun_family = AF_UNIX;
    strncpy(addr.sun_path, CONTROL_PATH, sizeof(addr.sun_path) - 1);

    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) < 0) {
        fprintf(stderr, "Cannot connect to supervisor at %s: %s\n"
                        "Is the supervisor running? (sudo ./engine supervisor <rootfs>)\n",
                CONTROL_PATH, strerror(errno));
        close(fd);
        return 1;
    }

    if (write(fd, req, sizeof(*req)) != (ssize_t)sizeof(*req)) {
        perror("write");
        close(fd);
        return 1;
    }

    /* For PS and LOGS the server sends raw text — just print it */
    if (req->kind == CMD_PS || req->kind == CMD_LOGS) {
        char buf[4096];
        ssize_t n;
        while ((n = read(fd, buf, sizeof(buf))) > 0)
            fwrite(buf, 1, n, stdout);
        close(fd);
        return 0;
    }

    /* For other commands expect a control_response_t */
    control_response_t resp;
    ssize_t n = read(fd, &resp, sizeof(resp));
    close(fd);

    if (n == (ssize_t)sizeof(resp)) {
        printf("%s\n", resp.message);
        return resp.status == 0 ? 0 : 1;
    }

    fprintf(stderr, "Incomplete response from supervisor\n");
    return 1;
}

/* ------------------------------------------------------------------ */
/*  CLI command wrappers                                               */
/* ------------------------------------------------------------------ */

static int cmd_start(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s start <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_START;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_run(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 5) {
        fprintf(stderr,
                "Usage: %s run <id> <container-rootfs> <command>"
                " [--soft-mib N] [--hard-mib N] [--nice N]\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_RUN;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    strncpy(req.rootfs,       argv[3], sizeof(req.rootfs) - 1);
    strncpy(req.command,      argv[4], sizeof(req.command) - 1);
    req.soft_limit_bytes = DEFAULT_SOFT_LIMIT;
    req.hard_limit_bytes = DEFAULT_HARD_LIMIT;
    if (parse_optional_flags(&req, argc, argv, 5) != 0) return 1;
    return send_control_request(&req);
}

static int cmd_ps(void)
{
    control_request_t req;
    memset(&req, 0, sizeof(req));
    req.kind = CMD_PS;
    return send_control_request(&req);
}

static int cmd_logs(int argc, char *argv[])
{
    control_request_t req;
    if (argc < 3) {
        fprintf(stderr, "Usage: %s logs <id>\n", argv[0]);
        return 1;
    }
    memset(&req, 0, sizeof(req));
    req.kind = CMD_LOGS;
    strncpy(req.container_id, argv[2], sizeof(req.container_id) - 1);
    return send_control_request(&req);
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

/* ------------------------------------------------------------------ */
/*  main                                                               */
/* ------------------------------------------------------------------ */

int main(int argc, char *argv[])
{
    if (argc < 2) { usage(argv[0]); return 1; }

    if (strcmp(argv[1], "supervisor") == 0) {
        if (argc < 3) {
            fprintf(stderr, "Usage: %s supervisor <base-rootfs>\n", argv[0]);
            return 1;
        }
        return run_supervisor(argv[2]);
    }
    if (strcmp(argv[1], "start") == 0) return cmd_start(argc, argv);
    if (strcmp(argv[1], "run")   == 0) return cmd_run(argc, argv);
    if (strcmp(argv[1], "ps")    == 0) return cmd_ps();
    if (strcmp(argv[1], "logs")  == 0) return cmd_logs(argc, argv);
    if (strcmp(argv[1], "stop")  == 0) return cmd_stop(argc, argv);

    usage(argv[0]);
    return 1;
}