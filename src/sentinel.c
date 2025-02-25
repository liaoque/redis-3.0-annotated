/* Redis Sentinel implementation
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "server.h"
#include "hiredis.h"
#ifdef USE_OPENSSL
#include "openssl/ssl.h"
#include "hiredis_ssl.h"
#endif
#include "async.h"

#include <ctype.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <sys/wait.h>
#include <fcntl.h>

extern char **environ;

#ifdef USE_OPENSSL
extern SSL_CTX *redis_tls_ctx;
extern SSL_CTX *redis_tls_client_ctx;
#endif

// sentinel 的默认端口号
#define REDIS_SENTINEL_PORT 26379

/* ======================== Sentinel global state =========================== */

/* Address object, used to describe an ip:port pair. */
/* 地址对象，用于保存 IP 地址和端口 */
typedef struct sentinelAddr {
    char *hostname;         /* Hostname OR address, as specified */
    char *ip;               /* Always a resolved address */
    int port;
} sentinelAddr;

/* A Sentinel Redis Instance object is monitoring. */
/* 每个被监视的 Redis 实例都会创建一个 sentinelRedisInstance 结构
 * 而每个结构的 flags 值会是以下常量的一个或多个的并 */
// 实例是一个主服务器
#define SRI_MASTER  (1<<0)
// 实例是一个从服务器
#define SRI_SLAVE   (1<<1)
// 实例是一个 Sentinel
#define SRI_SENTINEL (1<<2)
// 实例已断线
#define SRI_S_DOWN (1<<3)   /* Subjectively down (no quorum). */
// 实例已处于 SDOWN 状态
#define SRI_O_DOWN (1<<4)   /* Objectively down (confirmed by others). */
// 实例已处于 ODOWN 状态
#define SRI_MASTER_DOWN (1<<5) /* A Sentinel with this flag set thinks that
                                   its master is down. */
                                   // 正在对主服务器进行故障迁移
#define SRI_FAILOVER_IN_PROGRESS (1<<6) /* Failover is in progress forthis master. */


                                   // 实例是被选中的新主服务器（目前仍是从服务器）
#define SRI_PROMOTED (1<<7)            /* Slave selected for promotion. */
// 向从服务器发送 SLAVEOF 命令，让它们转向复制新主服务器
#define SRI_RECONF_SENT (1<<8)     /* SLAVEOF <newmaster> sent. */
// 从服务器正在与新主服务器进行同步
#define SRI_RECONF_INPROG (1<<9)   /* Slave synchronization in progress. */
// 从服务器与新主服务器同步完毕，开始复制新主服务器
#define SRI_RECONF_DONE (1<<10)     /* Slave synchronized with new master. */
//对主服务器强制执行故障迁移操作
#define SRI_FORCE_FAILOVER (1<<11)  /* Force failover with master up. */
// 已经对返回 -BUSY 的服务器发送 SCRIPT KILL 命令
#define SRI_SCRIPT_KILL_SENT (1<<12) /* SCRIPT KILL already sent on -BUSY */

/* Note: times are in milliseconds. */
/* 各种时间常量，以毫秒为单位 */
// 发送 INFO 命令的间隔
#define SENTINEL_INFO_PERIOD 10000
// 发送 PING 命令的间隔
#define SENTINEL_PING_PERIOD 1000
// 发送 ASK 命令的间隔
#define SENTINEL_ASK_PERIOD 1000
// 发送 PUBLISH 命令的间隔
#define SENTINEL_PUBLISH_PERIOD 2000
// 默认的判断服务器已下线的时长
#define SENTINEL_DEFAULT_DOWN_AFTER 30000
// 默认的信息频道
#define SENTINEL_HELLO_CHANNEL "__sentinel__:hello"
// 默认的 TILT 触发时长
#define SENTINEL_TILT_TRIGGER 2000
// 默认的 TILT 环境时长（要多久才能退出 TITL 模式）
#define SENTINEL_TILT_PERIOD (SENTINEL_PING_PERIOD*30)
// 默认从服务器优先级
#define SENTINEL_DEFAULT_SLAVE_PRIORITY 100
#define SENTINEL_SLAVE_RECONF_TIMEOUT 10000
// 默认的同时对新主服务器进行复制的从服务器个数
#define SENTINEL_DEFAULT_PARALLEL_SYNCS 1
// 默认的最少重连接间隔
#define SENTINEL_MIN_LINK_RECONNECT_PERIOD 15000
// 默认的故障迁移执行时长
#define SENTINEL_DEFAULT_FAILOVER_TIMEOUT (60*3*1000)
// 默认的最大积压命令数量
#define SENTINEL_MAX_PENDING_COMMANDS 100
// 默认的选举超时时长
#define SENTINEL_ELECTION_TIMEOUT 10000
#define SENTINEL_MAX_DESYNC 1000
#define SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG 1
#define SENTINEL_DEFAULT_RESOLVE_HOSTNAMES 0
#define SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES 0

/* Failover machine different states. */
/* 故障转移时的状态 */
// 没在执行故障迁移
#define SENTINEL_FAILOVER_STATE_NONE 0  /* No failover in progress. */
// 正在等待开始故障迁移
#define SENTINEL_FAILOVER_STATE_WAIT_START 1  /* Wait for failover_start_time*/
// 正在挑选作为新主服务器的从服务器
#define SENTINEL_FAILOVER_STATE_SELECT_SLAVE 2 /* Select slave to promote */
// 向被选中的从服务器发送 SLAVEOF no one
#define SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE 3 /* Slave -> Master */
// 等待从服务器转变成主服务器
#define SENTINEL_FAILOVER_STATE_WAIT_PROMOTION 4 /* Wait slave to change role */
// 向已下线主服务器的其他从服务器发送 SLAVEOF 命令
// 让它们复制新的主服务器
#define SENTINEL_FAILOVER_STATE_RECONF_SLAVES 5 /* SLAVEOF newmaster */
// 监视被升级的从服务器
#define SENTINEL_FAILOVER_STATE_UPDATE_CONFIG 6 /* Monitor promoted slave. */

/* 主从服务器之间的连接状态 */
// 连接正常
#define SENTINEL_MASTER_LINK_STATUS_UP 0
// 连接断开
#define SENTINEL_MASTER_LINK_STATUS_DOWN 1

/* Generic flags that can be used with different functions.
 * They use higher bits to avoid colliding with the function specific
 * flags. */
/* 可以用于多个函数的通用标识。
 * 使用高位来避免与一般标识冲突。 */
// 没有标识
#define SENTINEL_NO_FLAGS 0
// 生成事件
#define SENTINEL_GENERATE_EVENT (1<<16)
// 领头
#define SENTINEL_LEADER (1<<17)
// 观察者
#define SENTINEL_OBSERVER (1<<18)

/* Script execution flags and limits. */
/* 脚本执行状态和限制 */
// 脚本目前没有被执行
#define SENTINEL_SCRIPT_NONE 0
// 脚本正在执行
#define SENTINEL_SCRIPT_RUNNING 1
// 脚本队列保存脚本数量的最大值
#define SENTINEL_SCRIPT_MAX_QUEUE 256
// 同一时间可执行脚本的最大数量
#define SENTINEL_SCRIPT_MAX_RUNNING 16
// 脚本的最大执行时长
#define SENTINEL_SCRIPT_MAX_RUNTIME 60000 /* 60 seconds max exec time. */
// 脚本最大重试数量
#define SENTINEL_SCRIPT_MAX_RETRY 10
// 脚本重试之前的延迟时间
#define SENTINEL_SCRIPT_RETRY_DELAY 30000 /* 30 seconds between retries. */

/* SENTINEL SIMULATE-FAILURE command flags. */
#define SENTINEL_SIMFAILURE_NONE 0
#define SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION (1<<0)
#define SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION (1<<1)

/* The link to a sentinelRedisInstance. When we have the same set of Sentinels
 * monitoring many masters, we have different instances representing the
 * same Sentinels, one per master, and we need to share the hiredis connections
 * among them. Otherwise if 5 Sentinels are monitoring 100 masters we create
 * 500 outgoing connections instead of 5.
 *
 * So this structure represents a reference counted link in terms of the two
 * hiredis connections for commands and Pub/Sub, and the fields needed for
 * failure detection, since the ping/pong time are now local to the link: if
 * the link is available, the instance is available. This way we don't just
 * have 5 connections instead of 500, we also send 5 pings instead of 500.
 *
 * Links are shared only for Sentinels: master and slave instances have
 * a link with refcount = 1, always. */
typedef struct instanceLink {
    int refcount;          /* Number of sentinelRedisInstance owners. */
    int disconnected;      /* Non-zero if we need to reconnect cc or pc. */
    int pending_commands;  /* Number of commands sent waiting for a reply. */
    redisAsyncContext *cc; /* Hiredis context for commands. */

    // 用于执行 SUBSCRIBE 命令、接收频道信息的异步连接
    // 仅在实例为主服务器时使用
    redisAsyncContext *pc; /* Hiredis context for Pub / Sub. */

    // 已发送但尚未回复的命令数量
//    int pending_commands;   /* Number of commands sent waiting for a reply. */

    // cc 连接的创建时间
    mstime_t cc_conn_time; /* cc connection time. */

    // pc 连接的创建时间
    mstime_t pc_conn_time; /* pc connection time. */

    // 最后一次从这个实例接收信息的时间
    mstime_t pc_last_activity; /* Last time we received any message. */

    // 实例最后一次返回正确的 PING 命令回复的时间
    mstime_t last_avail_time; /* Last time the instance replied to ping with
                                 a reply we consider valid. */


    mstime_t act_ping_time;   /* Time at which the last pending ping (no pong
                                 received after it) was sent. This field is
                                 set to 0 when a pong is received, and set again
                                 to the current time if the value is 0 and a new
                                 ping is sent. */

    // 实例最后一次发送 PING 命令的时间
    mstime_t last_ping_time;  /* Time at which we sent the last ping. This is
                                 only used to avoid sending too many pings
                                 during failure. Idle time is computed using
                                 the act_ping_time field. */

    // 实例最后一次返回 PING 命令的时间，无论内容正确与否
    mstime_t last_pong_time;  /* Last time the instance replied to ping,
                                 whatever the reply was. That's used to check
                                 if the link is idle and must be reconnected. */
    mstime_t last_reconn_time;  /* Last reconnection attempt performed when
                                   the link was down. */
} instanceLink;


// Sentinel 会为每个被监视的 Redis 实例创建相应的 sentinelRedisInstance 实例
// （被监视的实例可以是主服务器、从服务器、或者其他 Sentinel ）
typedef struct sentinelRedisInstance {
    // 标识值，记录了实例的类型，以及该实例的当前状态
    int flags;      /* See SRI_... defines */

    // 实例的名字
    // 主服务器的名字由用户在配置文件中设置
    // 从服务器以及 Sentinel 的名字由 Sentinel 自动设置
    // 格式为 ip:port ，例如 "127.0.0.1:26379"
    char *name;     /* Master name from the point of view of this sentinel. */

    // 实例的运行 ID
    char *runid;    /* Run ID of this instance, or unique ID if is a Sentinel.*/

    // 配置纪元，用于实现故障转移
    uint64_t config_epoch;  /* Configuration epoch. */

    // 实例的地址
    sentinelAddr *addr; /* Master host. */
    instanceLink *link; /* Link to the instance, may be shared for Sentinels. */

    // 最后一次向频道发送问候信息的时间
    // 只在当前实例为 sentinel 时使用
    mstime_t last_pub_time;   /* Last time we sent hello via Pub/Sub. */

    // 最后一次接收到这个 sentinel 发来的问候信息的时间
    // 只在当前实例为 sentinel 时使用
    mstime_t last_hello_time; /* Only used if SRI_SENTINEL is set. Last time
                                 we received a hello from this Sentinel
                                 via Pub/Sub. */

    // 最后一次回复 SENTINEL is-master-down-by-addr 命令的时间
    // 只在当前实例为 sentinel 时使用
    mstime_t last_master_down_reply_time; /* Time of last reply to
                                             SENTINEL is-master-down command. */

    // 实例被判断为 SDOWN 状态的时间
    mstime_t s_down_since_time; /* Subjectively down since time. */

    // 实例被判断为 ODOWN 状态的时间
    mstime_t o_down_since_time; /* Objectively down since time. */

    // SENTINEL down-after-milliseconds 选项所设定的值
    // 实例无响应多少毫秒之后才会被判断为主观下线（subjectively down）
    mstime_t down_after_period; /* Consider it down after that period. */

    // 从实例获取 INFO 命令的回复的时间
    mstime_t info_refresh;  /* Time at which we received INFO output from it. */
    dict *renamed_commands;     /* Commands renamed in this instance:
                                   Sentinel will use the alternative commands
                                   mapped on this table to send things like
                                   SLAVEOF, CONFING, INFO, ... */

    /* Role and the first time we observed it.
     * This is useful in order to delay replacing what the instance reports
     * with our own configuration. We need to always wait some time in order
     * to give a chance to the leader to report the new configuration before
     * we do silly things. */
    // 实例的角色
    int role_reported;
    // 角色的更新时间
    mstime_t role_reported_time;

    // 最后一次从服务器的主服务器地址变更的时间
    mstime_t slave_conf_change_time; /* Last time slave master addr changed. */

    /* Master specific. */
    /* 主服务器实例特有的属性 -------------------------------------------------------------*/

    // 其他同样监控这个主服务器的所有 sentinel
    dict *sentinels;    /* Other sentinels monitoring the same master. */

    // 如果这个实例代表的是一个主服务器
    // 那么这个字典保存着主服务器属下的从服务器
    // 字典的键是从服务器的名字，字典的值是从服务器对应的 sentinelRedisInstance 结构
    dict *slaves;       /* Slaves for this master instance. */

    // SENTINEL monitor <master-name> <IP> <port> <quorum> 选项中的 quorum 参数
    // 判断这个实例为客观下线（objectively down）所需的支持投票数量
    unsigned int quorum;/* Number of sentinels that need to agree on failure. */
    // SENTINEL parallel-syncs <master-name> <number> 选项的值
    // 在执行故障转移操作时，可以同时对新的主服务器进行同步的从服务器数量
    int parallel_syncs; /* How many slaves to reconfigure at same time. */


    // 连接主服务器和从服务器所需的密码
    char *auth_pass;    /* Password to use for AUTH against master & replica. */
    char *auth_user;    /* Username for ACLs AUTH against master & replica. */

    /* Slave specific. */
    /* 从服务器实例特有的属性 -------------------------------------------------------------*/

    // 主从服务器连接断开的时间
    mstime_t master_link_down_time; /* Slave replication link down time. */

    // 从服务器优先级
    int slave_priority; /* Slave priority according to its INFO output. */


    int replica_announced; /* Replica announcing according to its INFO output. */
    // 执行故障转移操作时，从服务器发送 SLAVEOF <new-master> 命令的时间
    mstime_t slave_reconf_sent_time; /* Time at which we sent SLAVE OF <new> */

    // 主服务器的实例（在本实例为从服务器时使用）
    struct sentinelRedisInstance *master; /* Master instance if it's slave. */

    // INFO 命令的回复中记录的主服务器 IP
    char *slave_master_host;    /* Master host as reported by INFO */

    // INFO 命令的回复中记录的主服务器端口号
    int slave_master_port;      /* Master port as reported by INFO */

    // INFO 命令的回复中记录的主从服务器连接状态
    int slave_master_link_status; /* Master link status as reported by INFO */

    // 从服务器的复制偏移量
    unsigned long long slave_repl_offset; /* Slave replication offset. */
    /* Failover */
    /* 故障转移相关属性 -------------------------------------------------------------------*/


    // 如果这是一个主服务器实例，那么 leader 将是负责进行故障转移的 Sentinel 的运行 ID 。
    // 如果这是一个 Sentinel 实例，那么 leader 就是被选举出来的领头 Sentinel 。
    // 这个域只在 Sentinel 实例的 flags 属性的 SRI_MASTER_DOWN 标志处于打开状态时才有效。
    char *leader;       /* If this is a master instance, this is the runid of
                           the Sentinel that should perform the failover. If
                           this is a Sentinel, this is the runid of the Sentinel
                           that this Sentinel voted as leader. */
    // 领头的纪元
    uint64_t leader_epoch; /* Epoch of the 'leader' field. */
    // 当前执行中的故障转移的纪元
    uint64_t failover_epoch; /* Epoch of the currently started failover. */
    // 故障转移操作的当前状态
    int failover_state; /* See SENTINEL_FAILOVER_STATE_* defines. */

    // 状态改变的时间
    mstime_t failover_state_change_time;

    // 最后一次进行故障迁移的时间
    mstime_t failover_start_time;   /* Last failover attempt start time. */

    // SENTINEL failover-timeout <master-name> <ms> 选项的值
    // 刷新故障迁移状态的最大时限
    mstime_t failover_timeout;      /* Max time to refresh failover state. */

    mstime_t failover_delay_logged; /* For what failover_start_time value we
                                       logged the failover delay. */
    // 指向被提升为新主服务器的从服务器的指针
    struct sentinelRedisInstance *promoted_slave; /* Promoted slave instance. */

    /* Scripts executed to notify admin or reconfigure clients: when they
     * are set to NULL no script is executed. */
    // 一个文件路径，保存着 WARNING 级别的事件发生时执行的，
    // 用于通知管理员的脚本的地址
    char *notification_script;
    // 一个文件路径，保存着故障转移执行之前、之后、或者被中止时，
    // 需要执行的脚本的地址
    char *client_reconfig_script;
    sds info; /* cached INFO output */
} sentinelRedisInstance;

/* Main state. */
/* Sentinel 的状态结构 */
struct sentinelState {
    char myid[CONFIG_RUN_ID_SIZE+1]; /* This sentinel ID. */

    // 当前纪元
    uint64_t current_epoch;         /* Current epoch. */
    // 保存了所有被这个 sentinel 监视的主服务器
    // 字典的键是主服务器的名字
    // 字典的值则是一个指向 sentinelRedisInstance 结构的指针
    dict *masters;      /* Dictionary of master sentinelRedisInstances.
                           Key is the instance name, value is the
                           sentinelRedisInstance structure pointer. */

    // 是否进入了 TILT 模式？
    int tilt;           /* Are we in TILT mode? */

    // 目前正在执行的脚本的数量
    int running_scripts;    /* Number of scripts in execution right now. */

    // 进入 TILT 模式的时间
    mstime_t tilt_start_time;       /* When TITL started. */

    // 最后一次执行时间处理器的时间
    mstime_t previous_time;         /* Last time we ran the time handler. */

    // 一个 FIFO 队列，包含了所有需要执行的用户脚本
    list *scripts_queue;            /* Queue of user scripts to execute. */
    char *announce_ip;  /* IP addr that is gossiped to other sentinels if
                           not NULL. */
    int announce_port;  /* Port that is gossiped to other sentinels if
                           non zero. */
    unsigned long simfailure_flags; /* Failures simulation. */
    int deny_scripts_reconfig; /* Allow SENTINEL SET ... to change script
                                  paths at runtime? */
    char *sentinel_auth_pass;    /* Password to use for AUTH against other sentinel */
    char *sentinel_auth_user;    /* Username for ACLs AUTH against other sentinel. */
    int resolve_hostnames;       /* Support use of hostnames, assuming DNS is well configured. */
    int announce_hostnames;      /* Announce hostnames instead of IPs when we have them. */
} sentinel;

/* A script execution job. */
// 脚本运行状态
typedef struct sentinelScriptJob {

    // 标志，记录了脚本是否运行
    int flags;              /* Script job flags: SENTINEL_SCRIPT_* */

    // 该脚本的已尝试执行次数
    int retry_num;          /* Number of times we tried to execute it. */

    // 要传给脚本的参数
    char **argv;            /* Arguments to call the script. */

    // 开始运行脚本的时间
    mstime_t start_time;    /* Script execution time if the script is running,
                               otherwise 0 if we are allowed to retry the
                               execution at any time. If the script is not
                               running and it's not 0, it means: do not run
                               before the specified time. */

    // 脚本由子进程执行，该属性记录子进程的 pid
    pid_t pid;              /* Script execution pid. */
} sentinelScriptJob;

/* ======================= hiredis ae.c adapters =============================
 * Note: this implementation is taken from hiredis/adapters/ae.h, however
 * we have our modified copy for Sentinel in order to use our allocator
 * and to have full control over how the adapter works. */

// 客户端适配器（adapter）结构
typedef struct redisAeEvents {

    // 客户端连接上下文
    redisAsyncContext *context;

    // 服务器的事件循环
    aeEventLoop *loop;

    // 套接字
    int fd;

    // 记录读事件以及写事件是否就绪
    int reading, writing;

} redisAeEvents;

// 读事件处理器
static void redisAeReadEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    // 从连接中进行读取
    redisAsyncHandleRead(e->context);
}

// 写事件处理器
static void redisAeWriteEvent(aeEventLoop *el, int fd, void *privdata, int mask) {
    ((void)el); ((void)fd); ((void)mask);

    redisAeEvents *e = (redisAeEvents*)privdata;
    // 从连接中进行写入
    redisAsyncHandleWrite(e->context);
}

// 将读事件处理器安装到事件循环中
static void redisAeAddRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    // 如果读事件处理器未安装，那么进行安装
    if (!e->reading) {
        e->reading = 1;
        aeCreateFileEvent(loop,e->fd,AE_READABLE,redisAeReadEvent,e);
    }
}

// 从事件循环中删除读事件处理器
static void redisAeDelRead(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    // 仅在读事件处理器已安装的情况下进行删除
    if (e->reading) {
        e->reading = 0;
        aeDeleteFileEvent(loop,e->fd,AE_READABLE);
    }
}

// 将写事件处理器安装到事件循环中
static void redisAeAddWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (!e->writing) {
        e->writing = 1;
        aeCreateFileEvent(loop,e->fd,AE_WRITABLE,redisAeWriteEvent,e);
    }
}

// 从事件循环中删除写事件处理器
static void redisAeDelWrite(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    aeEventLoop *loop = e->loop;
    if (e->writing) {
        e->writing = 0;
        aeDeleteFileEvent(loop,e->fd,AE_WRITABLE);
    }
}

// 清理事件
static void redisAeCleanup(void *privdata) {
    redisAeEvents *e = (redisAeEvents*)privdata;
    redisAeDelRead(privdata);
    redisAeDelWrite(privdata);
    zfree(e);
}

// 为上下文 ae 和事件循环 loop 创建 hiredis 适配器
// 并设置相关的异步处理函数
static int redisAeAttach(aeEventLoop *loop, redisAsyncContext *ac) {
    redisContext *c = &(ac->c);
    redisAeEvents *e;

    /* Nothing should be attached when something is already attached */
    if (ac->ev.data != NULL)
        return C_ERR;

    /* Create container for context and r/w events */
    // 创建适配器
    e = (redisAeEvents*)zmalloc(sizeof(*e));
    e->context = ac;
    e->loop = loop;
    e->fd = c->fd;
    e->reading = e->writing = 0;

    /* Register functions to start/stop listening for events */
    // 设置异步调用函数
    ac->ev.addRead = redisAeAddRead;
    ac->ev.delRead = redisAeDelRead;
    ac->ev.addWrite = redisAeAddWrite;
    ac->ev.delWrite = redisAeDelWrite;
    ac->ev.cleanup = redisAeCleanup;
    ac->ev.data = e;

    return C_OK;
}

/* ============================= Prototypes ================================= */

void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status);
void sentinelDisconnectCallback(const redisAsyncContext *c, int status);
void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata);
sentinelRedisInstance *sentinelGetMasterByName(char *name);
char *sentinelGetSubjectiveLeader(sentinelRedisInstance *master);
char *sentinelGetObjectiveLeader(sentinelRedisInstance *master);
int yesnotoi(char *s);
void instanceLinkConnectionError(const redisAsyncContext *c);
const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri);
void sentinelAbortFailover(sentinelRedisInstance *ri);
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri, const char *fmt, ...);
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master);
void sentinelScheduleScriptExecution(char *path, ...);
void sentinelStartFailover(sentinelRedisInstance *master);
void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata);
int sentinelSendSlaveOf(sentinelRedisInstance *ri, const sentinelAddr *addr);
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch);
void sentinelFlushConfig(void);
void sentinelGenerateInitialMonitorEvents(void);
int sentinelSendPing(sentinelRedisInstance *ri);
int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master);
sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *ip, int port, char *runid);
void sentinelSimFailureCrash(void);

/* ========================= Dictionary types =============================== */

uint64_t dictSdsHash(const void *key);
uint64_t dictSdsCaseHash(const void *key);
int dictSdsKeyCompare(void *privdata, const void *key1, const void *key2);
int dictSdsKeyCaseCompare(void *privdata, const void *key1, const void *key2);
void releaseSentinelRedisInstance(sentinelRedisInstance *ri);

void dictInstancesValDestructor (void *privdata, void *obj) {
    UNUSED(privdata);
    releaseSentinelRedisInstance(obj);
}

/* Instance name (sds) -> instance (sentinelRedisInstance pointer)
 *
 * also used for: sentinelRedisInstance->sentinels dictionary that maps
 * sentinels ip:port to last seen time in Pub/Sub hello message. */
// 这个字典类型有两个作用：
// 1） 将实例名字映射到一个 sentinelRedisInstance 指针
// 2） 将 sentinelRedisInstance 指针映射到一个字典，
//     字典的键是 Sentinel 的 ip:port 地址，
//     字典的值是该 Sentinel 最后一次向频道发送信息的时间
dictType instancesDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* key destructor */
        dictInstancesValDestructor,/* val destructor */
        NULL                       /* allow to expand */
};

/* Instance runid (sds) -> votes (long casted to void*)
 *
 * This is useful into sentinelGetObjectiveLeader() function in order to
 * count the votes and understand who is the leader. */
// 将一个运行 ID 映射到一个 cast 成 void* 类型的 long 值的投票数量上
// 用于统计客观 leader sentinel
dictType leaderVotesDictType = {
        dictSdsHash,               /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCompare,         /* key compare */
        NULL,                      /* key destructor */
        NULL,                      /* val destructor */
        NULL                       /* allow to expand */
};

/* Instance renamed commands table. */
dictType renamedCommandsDictType = {
        dictSdsCaseHash,           /* hash function */
        NULL,                      /* key dup */
        NULL,                      /* val dup */
        dictSdsKeyCaseCompare,     /* key compare */
        dictSdsDestructor,         /* key destructor */
        dictSdsDestructor,         /* val destructor */
        NULL                       /* allow to expand */
};

/* =========================== Initialization =============================== */

void sentinelCommand(client *c);
void sentinelInfoCommand(client *c);
void sentinelSetCommand(client *c);
void sentinelPublishCommand(client *c);
void sentinelRoleCommand(client *c);
void sentinelConfigGetCommand(client *c);
void sentinelConfigSetCommand(client *c);

struct redisCommand sentinelcmds[] = {
        {"ping",pingCommand,1,"fast @connection",0,NULL,0,0,0,0,0},
        {"sentinel",sentinelCommand,-2,"admin",0,NULL,0,0,0,0,0},
        {"subscribe",subscribeCommand,-2,"pub-sub",0,NULL,0,0,0,0,0},
        {"unsubscribe",unsubscribeCommand,-1,"pub-sub",0,NULL,0,0,0,0,0},
        {"psubscribe",psubscribeCommand,-2,"pub-sub",0,NULL,0,0,0,0,0},
        {"punsubscribe",punsubscribeCommand,-1,"pub-sub",0,NULL,0,0,0,0,0},
        {"publish",sentinelPublishCommand,3,"pub-sub fast",0,NULL,0,0,0,0,0},
        {"info",sentinelInfoCommand,-1,"random @dangerous",0,NULL,0,0,0,0,0},
        {"role",sentinelRoleCommand,1,"fast read-only @dangerous",0,NULL,0,0,0,0,0},
        {"client",clientCommand,-2,"admin random @connection",0,NULL,0,0,0,0,0},
        {"shutdown",shutdownCommand,-1,"admin",0,NULL,0,0,0,0,0},
        {"auth",authCommand,-2,"no-auth fast @connection",0,NULL,0,0,0,0,0},
        {"hello",helloCommand,-1,"no-auth fast @connection",0,NULL,0,0,0,0,0},
        {"acl",aclCommand,-2,"admin",0,NULL,0,0,0,0,0,0},
        {"command",commandCommand,-1, "random @connection", 0,NULL,0,0,0,0,0,0}
};

/* this array is used for sentinel config lookup, which need to be loaded
 * before monitoring masters config to avoid dependency issues */
const char *preMonitorCfgName[] = {
        "announce-ip",
        "announce-port",
        "deny-scripts-reconfig",
        "sentinel-user",
        "sentinel-pass",
        "current-epoch",
        "myid",
        "resolve-hostnames",
        "announce-hostnames"
};

/* This function overwrites a few normal Redis config default with Sentinel
 * specific defaults. */
void initSentinelConfig(void) {
    server.port = REDIS_SENTINEL_PORT;
    server.protected_mode = 0; /* Sentinel must be exposed. */
}

void freeSentinelLoadQueueEntry(void *item);

/* Perform the Sentinel mode initialization. */
// 以 Sentinel 模式初始化服务器
void initSentinel(void) {
    unsigned int j;

    /* Remove usual Redis commands from the command table, then just add
     * the SENTINEL command. */
    // 清空 Redis 服务器的命令表（该表用于普通模式）
    dictEmpty(server.commands,NULL);
    dictEmpty(server.orig_commands,NULL);
    ACLClearCommandID();
    // 将 SENTINEL 模式所用的命令添加进命令表
    for (j = 0; j < sizeof(sentinelcmds)/sizeof(sentinelcmds[0]); j++) {
        int retval;
        struct redisCommand *cmd = sentinelcmds+j;
        cmd->id = ACLGetCommandID(cmd->name); /* Assign the ID used for ACL. */
        retval = dictAdd(server.commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);
        retval = dictAdd(server.orig_commands, sdsnew(cmd->name), cmd);
        serverAssert(retval == DICT_OK);

        /* Translate the command string flags description into an actual
         * set of flags. */
        if (populateCommandTableParseFlags(cmd,cmd->sflags) == C_ERR)
            serverPanic("Unsupported command flag");
    }

    /* Initialize various data structures. */
    /* 初始化 Sentinel 的状态 */
    // 初始化纪元
    sentinel.current_epoch = 0;

    // 初始化保存主服务器信息的字典
    sentinel.masters = dictCreate(&instancesDictType,NULL);

    // 初始化 TILT 模式的相关选项
    sentinel.tilt = 0;
    sentinel.tilt_start_time = 0;
    sentinel.previous_time = mstime();
    // 初始化脚本相关选项
    sentinel.running_scripts = 0;
    sentinel.scripts_queue = listCreate();
    sentinel.announce_ip = NULL;
    sentinel.announce_port = 0;
    sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
    sentinel.deny_scripts_reconfig = SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG;
    sentinel.sentinel_auth_pass = NULL;
    sentinel.sentinel_auth_user = NULL;
    sentinel.resolve_hostnames = SENTINEL_DEFAULT_RESOLVE_HOSTNAMES;
    sentinel.announce_hostnames = SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES;
    memset(sentinel.myid,0,sizeof(sentinel.myid));
    server.sentinel_config = NULL;
}

/* This function is for checking whether sentinel config file has been set,
 * also checking whether we have write permissions. */
void sentinelCheckConfigFile(void) {
    if (server.configfile == NULL) {
        serverLog(LL_WARNING,
                  "Sentinel needs config file on disk to save state. Exiting...");
        exit(1);
    } else if (access(server.configfile,W_OK) == -1) {
        serverLog(LL_WARNING,
                  "Sentinel config file %s is not writable: %s. Exiting...",
                  server.configfile,strerror(errno));
        exit(1);
    }
}

/* This function gets called when the server is in Sentinel mode, started,
 * loaded the configuration, and is ready for normal operations. */
// 这个函数在 Sentinel 准备就绪，可以执行操作时执行
void sentinelIsRunning(void) {
    int j;

    /* If this Sentinel has yet no ID set in the configuration file, we
     * pick a random one and persist the config on disk. From now on this
     * will be this Sentinel ID across restarts. */
    for (j = 0; j < CONFIG_RUN_ID_SIZE; j++)
        if (sentinel.myid[j] != 0) break;

        if (j == CONFIG_RUN_ID_SIZE) {
            /* Pick ID and persist the config. */
            getRandomHexChars(sentinel.myid,CONFIG_RUN_ID_SIZE);
            sentinelFlushConfig();
        }

        /* Log its ID to make debugging of issues simpler. */
        serverLog(LL_WARNING,"Sentinel ID is %s", sentinel.myid);

        /* We want to generate a +monitor event for every configured master
         * at startup. */
        sentinelGenerateInitialMonitorEvents();
}

/* ============================== sentinelAddr ============================== */

/* Create a sentinelAddr object and return it on success.
 *
 * 创建一个 sentinel 地址对象，并在创建成功时返回该对象。
 *
 * On error NULL is returned and errno is set to:
 *
 * 函数在出错时返回 NULL ，并将 errnor 设为以下值：
 *
 *  ENOENT: Can't resolve the hostname.
 *          不能解释 hostname
 *
 *  EINVAL: Invalid port number.
 *          端口号不正确
 */
sentinelAddr *createSentinelAddr(char *hostname, int port) {
    char ip[NET_IP_STR_LEN];
    sentinelAddr *sa;

    // 检查端口号
    if (port < 0 || port > 65535) {
        errno = EINVAL;
        return NULL;
    }

    // 检查并创建地址
    if (anetResolve(NULL,hostname,ip,sizeof(ip),
                    sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR) {
        errno = ENOENT;
        return NULL;
    }
    // 创建并返回地址结构
    sa = zmalloc(sizeof(*sa));
    sa->hostname = sdsnew(hostname);
    sa->ip = sdsnew(ip);
    sa->port = port;
    return sa;
}

/* Return a duplicate of the source address. */
// 复制并返回给定地址的一个副本
sentinelAddr *dupSentinelAddr(sentinelAddr *src) {
    sentinelAddr *sa;

    sa = zmalloc(sizeof(*sa));
    sa->hostname = sdsnew(src->hostname);
    sa->ip = sdsnew(src->ip);
    sa->port = src->port;
    return sa;
}

/* Free a Sentinel address. Can't fail. */
// 释放 Sentinel 地址
void releaseSentinelAddr(sentinelAddr *sa) {
    sdsfree(sa->hostname);
    sdsfree(sa->ip);
    zfree(sa);
}

/* Return non-zero if two addresses are equal. */
// 如果两个地址相同，那么返回 0
int sentinelAddrIsEqual(sentinelAddr *a, sentinelAddr *b) {
    return a->port == b->port && !strcasecmp(a->ip,b->ip);
}

/* Return non-zero if a hostname matches an address. */
int sentinelAddrEqualsHostname(sentinelAddr *a, char *hostname) {
    char ip[NET_IP_STR_LEN];

    /* We always resolve the hostname and compare it to the address */
    if (anetResolve(NULL, hostname, ip, sizeof(ip),
                    sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR)
        return 0;
    return !strcasecmp(a->ip, ip);
}

const char *announceSentinelAddr(const sentinelAddr *a) {
    return sentinel.announce_hostnames ? a->hostname : a->ip;
}

/* Return an allocated sds with hostname/address:port. IPv6
 * addresses are bracketed the same way anetFormatAddr() does.
 */
sds announceSentinelAddrAndPort(const sentinelAddr *a) {
    const char *addr = announceSentinelAddr(a);
    if (strchr(addr, ':') != NULL)
        return sdscatprintf(sdsempty(), "[%s]:%d", addr, a->port);
    else
        return sdscatprintf(sdsempty(), "%s:%d", addr, a->port);
}

/* =========================== Events notification ========================== */

/* Send an event to log, pub/sub, user notification script.
 *
 * 将事件发送到日志、频道，以及用户提醒脚本。
 *
 * 'level' is the log level for logging. Only REDIS_WARNING events will trigger
 * the execution of the user notification script.
 *
 * level 是日志的级别。只有 REDIS_WARNING 级别的日志会触发用户提醒脚本。
 *
 * 'type' is the message type, also used as a pub/sub channel name.
 *
 * type 是信息的类型，也用作频道的名字。
 *
 * 'ri', is the redis instance target of this event if applicable, and is
 * used to obtain the path of the notification script to execute.
 *
 * ri 是引发事件的 Redis 实例，它可以用来获取可执行的用户脚本。
 *
 * The remaining arguments are printf-alike.
 *
 * 剩下的都是类似于传给 printf 函数的参数。
 *
 * If the format specifier starts with the two characters "%@" then ri is
 * not NULL, and the message is prefixed with an instance identifier in the
 * following format:
 *
 * 如果格式指定以 "%@" 两个字符开头，并且 ri 不为空，
 * 那么信息将使用以下实例标识符为开头：
 *
 *  <instance type> <instance name> <ip> <port>
 *
 *  If the instance type is not master, than the additional string is
 *  added to specify the originating master:
 *
 *  如果实例的类型不是主服务器，那么以下内容会被追加到信息的后面，
 *  用于指定目标主服务器：
 *
 *  @ <master name> <master ip> <master port>
 *
 *  Any other specifier after "%@" is processed by printf itself.
 *
 * "%@" 之后的其他指派器（specifier）都和 printf 函数所使用的指派器一样。
 */
void sentinelEvent(int level, char *type, sentinelRedisInstance *ri,
                   const char *fmt, ...) {
    va_list ap;
    // 日志字符串
    char msg[LOG_MAX_LEN];
    robj *channel, *payload;

    /* Handle %@ */
    // 处理 %@
    if (fmt[0] == '%' && fmt[1] == '@') {
        // 如果 ri 实例是主服务器，那么 master 就是 NULL
        // 否则 ri 就是一个从服务器或者 sentinel ，而 master 就是该实例的主服务器
        //
        // sentinelRedisInstance *master = NULL;
        // if (~(ri->flags & SRI_MASTER))
        //     master = ri->master;
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                NULL : ri->master;

        if (master) {
            // ri 不是主服务器
            snprintf(msg, sizeof(msg), "%s %s %s %d @ %s %s %d",
                     // 打印 ri 的类型
                     // 打印 ri 的名字、IP 和端口号
                     // 打印 ri 的主服务器的名字、 IP 和端口号
                     sentinelRedisInstanceTypeStr(ri),
                     ri->name, announceSentinelAddr(ri->addr), ri->addr->port,
                     master->name, announceSentinelAddr(master->addr), master->addr->port);
        } else {
            // ri 是主服务器
            // 打印 ri 的类型
            // 打印 ri 的名字、IP 和端口号
            snprintf(msg, sizeof(msg), "%s %s %s %d",
                     sentinelRedisInstanceTypeStr(ri),
                     ri->name, announceSentinelAddr(ri->addr), ri->addr->port);
        }
        // 跳过已处理的 "%@" 字符
        fmt += 2;
    } else {
        msg[0] = '\0';
    }

    /* Use vsprintf for the rest of the formatting if any. */
    // 打印之后的内容，格式和平常的 printf 一样
    if (fmt[0] != '\0') {
        va_start(ap, fmt);
        vsnprintf(msg+strlen(msg), sizeof(msg)-strlen(msg), fmt, ap);
        va_end(ap);
    }

    /* Log the message if the log level allows it to be logged. */
    // 如果日志的级别足够高的话，那么记录到日志中
    if (level >= server.verbosity)
        serverLog(level,"%s %s",type,msg);

    /* Publish the message via Pub/Sub if it's not a debugging one. */
    // 如果日志不是 DEBUG 日志，那么将它发送到频道中
    if (level != LL_DEBUG) {
        // 频道
        channel = createStringObject(type,strlen(type));
        // 内容
        payload = createStringObject(msg,strlen(msg));
        // 发送信息
        pubsubPublishMessage(channel,payload);
        decrRefCount(channel);
        decrRefCount(payload);
    }

    /* Call the notification script if applicable. */
    // 如果有需要的话，调用提醒脚本
    if (level == LL_WARNING && ri != NULL) {
        sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ?
                ri : ri->master;
        if (master && master->notification_script) {
            sentinelScheduleScriptExecution(master->notification_script,
                                            type,msg,NULL);
        }
    }
}

/* This function is called only at startup and is used to generate a
 * +monitor event for every configured master. The same events are also
 * generated when a master to monitor is added at runtime via the
 * SENTINEL MONITOR command. */
// 在 Sentinel 启动时执行，用于创建并生成 +monitor 事件
void sentinelGenerateInitialMonitorEvents(void) {
    dictIterator *di;
    dictEntry *de;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);
        sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
    }
    dictReleaseIterator(di);
}

/* ============================ script execution ============================ */

/* Release a script job structure and all the associated data. */
// 释放一个脚本任务结构，以及该任务的相关数据。
void sentinelReleaseScriptJob(sentinelScriptJob *sj) {
    int j = 0;

    while(sj->argv[j]) sdsfree(sj->argv[j++]);
    zfree(sj->argv);
    zfree(sj);
}

// 将给定参数和脚本放入队列
#define SENTINEL_SCRIPT_MAX_ARGS 16
void sentinelScheduleScriptExecution(char *path, ...) {
    va_list ap;
    char *argv[SENTINEL_SCRIPT_MAX_ARGS+1];
    int argc = 1;
    sentinelScriptJob *sj;

    // 生成参数
    va_start(ap, path);
    while(argc < SENTINEL_SCRIPT_MAX_ARGS) {
        argv[argc] = va_arg(ap,char*);
        if (!argv[argc]) break;
        argv[argc] = sdsnew(argv[argc]); /* Copy the string. */
        argc++;
    }
    va_end(ap);
    argv[0] = sdsnew(path);

    // 初始化脚本结构
    sj = zmalloc(sizeof(*sj));
    sj->flags = SENTINEL_SCRIPT_NONE;
    sj->retry_num = 0;
    sj->argv = zmalloc(sizeof(char*)*(argc+1));
    sj->start_time = 0;
    sj->pid = 0;
    memcpy(sj->argv,argv,sizeof(char*)*(argc+1));

    // 添加到等待执行脚本队列的末尾， FIFO
    listAddNodeTail(sentinel.scripts_queue,sj);

    /* Remove the oldest non running script if we already hit the limit. */
    // 如果入队的脚本数量太多，那么移除最旧的未执行脚本
    if (listLength(sentinel.scripts_queue) > SENTINEL_SCRIPT_MAX_QUEUE) {
        listNode *ln;
        listIter li;

        listRewind(sentinel.scripts_queue,&li);
        while ((ln = listNext(&li)) != NULL) {
            sj = ln->value;

            // 不删除正在运行的脚本
            if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;
            /* The first node is the oldest as we add on tail. */
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
            break;
        }
        serverAssert(listLength(sentinel.scripts_queue) <=
        SENTINEL_SCRIPT_MAX_QUEUE);
    }
}

/* Lookup a script in the scripts queue via pid, and returns the list node
 * (so that we can easily remove it from the queue if needed). */
// 根据 pid ，查找正在运行中的脚本
listNode *sentinelGetScriptListNodeByPid(pid_t pid) {
    listNode *ln;
    listIter li;

    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        if ((sj->flags & SENTINEL_SCRIPT_RUNNING) && sj->pid == pid)
            return ln;
    }
    return NULL;
}

/* Run pending scripts if we are not already at max number of running
 * scripts. */
// 运行等待执行的脚本
void sentinelRunPendingScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    /* Find jobs that are not running and run them, from the top to the
     * tail of the queue, so we run older jobs first. */
    // 如果运行的脚本数量未超过最大值，
    // 那么从 FIFO 队列中取出未运行的脚本，并运行该脚本
    listRewind(sentinel.scripts_queue,&li);
    while (sentinel.running_scripts < SENTINEL_SCRIPT_MAX_RUNNING &&
    (ln = listNext(&li)) != NULL)
    {
        sentinelScriptJob *sj = ln->value;
        pid_t pid;

        /* Skip if already running. */
        // 跳过已运行脚本
        if (sj->flags & SENTINEL_SCRIPT_RUNNING) continue;

        /* Skip if it's a retry, but not enough time has elapsed. */
        // 这是一个重试脚本，但它刚刚执行完，稍后再重试
        if (sj->start_time && sj->start_time > now) continue;

        // 打开运行标记
        sj->flags |= SENTINEL_SCRIPT_RUNNING;
        // 记录开始时间
        sj->start_time = mstime();
        // 增加重试计数器
        sj->retry_num++;

        // 创建子进程
        pid = fork();

        if (pid == -1) {

            // 创建子进程失败
            /* Parent (fork error).
             * We report fork errors as signal 99, in order to unify the
             * reporting with other kind of errors. */
            sentinelEvent(LL_WARNING,"-script-error",NULL,
                          "%s %d %d", sj->argv[0], 99, 0);
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
        } else if (pid == 0) {
            // 子进程执行脚本
            /* Child */
            tlsCleanup();
            execve(sj->argv[0],sj->argv,environ);
            /* If we are here an error occurred. */
            _exit(2); /* Don't retry execution. */
        } else {

            // 父进程

            // 增加运行脚本计数器
            sentinel.running_scripts++;

            // 记录 pid
            sj->pid = pid;
            // 发送脚本运行信号
            sentinelEvent(LL_DEBUG,"+script-child",NULL,"%ld",(long)pid);
        }
    }
}

/* How much to delay the execution of a script that we need to retry after
 * an error?
 *
 * We double the retry delay for every further retry we do. So for instance
 * if RETRY_DELAY is set to 30 seconds and the max number of retries is 10
 * starting from the second attempt to execute the script the delays are:
 * 30 sec, 60 sec, 2 min, 4 min, 8 min, 16 min, 32 min, 64 min, 128 min. */
// 计算重试脚本前的延迟时间
mstime_t sentinelScriptRetryDelay(int retry_num) {
    mstime_t delay = SENTINEL_SCRIPT_RETRY_DELAY;

    while (retry_num-- > 1) delay *= 2;
    return delay;
}

/* Check for scripts that terminated, and remove them from the queue if the
 * script terminated successfully. If instead the script was terminated by
 * a signal, or returned exit code "1", it is scheduled to run again if
 * the max number of retries did not already elapsed. */
// 检查脚本的退出状态，并在脚本成功退出时，将脚本从队列中删除。
// 如果脚本被信号终结，或者返回退出代码 1 ，那么只要该脚本的重试次数未超过限制
// 那么该脚本就会被调度，并等待重试
void sentinelCollectTerminatedScripts(void) {
    int statloc;
    pid_t pid;

    // 获取子进程信号
    while ((pid = waitpid(-1, &statloc, WNOHANG)) > 0) {
        int exitcode = WEXITSTATUS(statloc);
        int bysignal = 0;
        listNode *ln;
        sentinelScriptJob *sj;

        // 发送脚本终结信号
        if (WIFSIGNALED(statloc)) bysignal = WTERMSIG(statloc);
        sentinelEvent(LL_DEBUG,"-script-child",NULL,"%ld %d %d",
                      (long)pid, exitcode, bysignal);

        // 在队列中安 pid 查找脚本
        ln = sentinelGetScriptListNodeByPid(pid);
        if (ln == NULL) {
            serverLog(LL_WARNING,"waitpid() returned a pid (%ld) we can't find in our scripts execution queue!", (long)pid);
            continue;
        }
        sj = ln->value;

        /* If the script was terminated by a signal or returns an
         * exit code of "1" (that means: please retry), we reschedule it
         * if the max number of retries is not already reached. */
        if ((bysignal || exitcode == 1) &&
        sj->retry_num != SENTINEL_SCRIPT_MAX_RETRY)
        {
            // 重试脚本
            sj->flags &= ~SENTINEL_SCRIPT_RUNNING;
            sj->pid = 0;
            sj->start_time = mstime() +
                    sentinelScriptRetryDelay(sj->retry_num);
        } else {
            /* Otherwise let's remove the script, but log the event if the
             * execution did not terminated in the best of the ways. */
            // 发送脚本执行错误事件
            if (bysignal || exitcode != 0) {
                sentinelEvent(LL_WARNING,"-script-error",NULL,
                              "%s %d %d", sj->argv[0], bysignal, exitcode);
            }
            // 将脚本从队列中删除
            listDelNode(sentinel.scripts_queue,ln);
            sentinelReleaseScriptJob(sj);
        }
        sentinel.running_scripts--;
    }
}

/* Kill scripts in timeout, they'll be collected by the
 * sentinelCollectTerminatedScripts() function. */
// 杀死超时脚本，这些脚本会被 sentinelCollectTerminatedScripts 函数回收处理
void sentinelKillTimedoutScripts(void) {
    listNode *ln;
    listIter li;
    mstime_t now = mstime();

    // 遍历队列中的所有脚本
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;

        // 选出那些正在执行，并且执行时间超过限制的脚本
        if (sj->flags & SENTINEL_SCRIPT_RUNNING &&
        (now - sj->start_time) > SENTINEL_SCRIPT_MAX_RUNTIME)
        {
            // 发送脚本超时事件
            sentinelEvent(LL_WARNING,"-script-timeout",NULL,"%s %ld",
                          sj->argv[0], (long)sj->pid);
            // 杀死脚本进程
            kill(sj->pid,SIGKILL);
        }
    }
}

/* Implements SENTINEL PENDING-SCRIPTS command. */
// 打印脚本队列中所有脚本的状态
void sentinelPendingScriptsCommand(client *c) {
    listNode *ln;
    listIter li;

    addReplyArrayLen(c,listLength(sentinel.scripts_queue));
    listRewind(sentinel.scripts_queue,&li);
    while ((ln = listNext(&li)) != NULL) {
        sentinelScriptJob *sj = ln->value;
        int j = 0;

        addReplyMapLen(c,5);

        addReplyBulkCString(c,"argv");
        while (sj->argv[j]) j++;
        addReplyArrayLen(c,j);
        j = 0;
        while (sj->argv[j]) addReplyBulkCString(c,sj->argv[j++]);

        addReplyBulkCString(c,"flags");
        addReplyBulkCString(c,
                            (sj->flags & SENTINEL_SCRIPT_RUNNING) ? "running" : "scheduled");

        addReplyBulkCString(c,"pid");
        addReplyBulkLongLong(c,sj->pid);

        if (sj->flags & SENTINEL_SCRIPT_RUNNING) {
            addReplyBulkCString(c,"run-time");
            addReplyBulkLongLong(c,mstime() - sj->start_time);
        } else {
            mstime_t delay = sj->start_time ? (sj->start_time-mstime()) : 0;
            if (delay < 0) delay = 0;
            addReplyBulkCString(c,"run-delay");
            addReplyBulkLongLong(c,delay);
        }

        addReplyBulkCString(c,"retry-num");
        addReplyBulkLongLong(c,sj->retry_num);
    }
}

/* This function calls, if any, the client reconfiguration script with the
 * following parameters:
 *
 * 当该函数执行时，使用以下格式的参数调用客户端重配置脚本
 *
 * <master-name> <role> <state> <from-ip> <from-port> <to-ip> <to-port>
 *
 * It is called every time a failover is performed.
 *
 * 这个函数在每次执行故障迁移时都会执行一次。
 *
 * <state> is currently always "failover".
 * <role> is either "leader" or "observer".
 *
 * <state> 总是 "failover" ，而 <role> 可以是 "leader" 或者 "observer"
 *
 * from/to fields are respectively master -> promoted slave addresses for
 * "start" and "end". */
void sentinelCallClientReconfScript(sentinelRedisInstance *master, int role, char *state, sentinelAddr *from, sentinelAddr *to) {
    char fromport[32], toport[32];

    if (master->client_reconfig_script == NULL) return;
    ll2string(fromport,sizeof(fromport),from->port);
    ll2string(toport,sizeof(toport),to->port);
    // 将给定参数和脚本放进度列，等待执行
    sentinelScheduleScriptExecution(master->client_reconfig_script,
                                    master->name,
                                    (role == SENTINEL_LEADER) ? "leader" : "observer",
                                    state, announceSentinelAddr(from), fromport,
                                    announceSentinelAddr(to), toport, NULL);
}

/* =============================== instanceLink ============================= */

/* Create a not yet connected link object. */
instanceLink *createInstanceLink(void) {
    instanceLink *link = zmalloc(sizeof(*link));

    link->refcount = 1;
    link->disconnected = 1;
    link->pending_commands = 0;
    link->cc = NULL;
    link->pc = NULL;
    link->cc_conn_time = 0;
    link->pc_conn_time = 0;
    link->last_reconn_time = 0;
    link->pc_last_activity = 0;
    /* We set the act_ping_time to "now" even if we actually don't have yet
     * a connection with the node, nor we sent a ping.
     * This is useful to detect a timeout in case we'll not be able to connect
     * with the node at all. */
    link->act_ping_time = mstime();
    link->last_ping_time = 0;
    link->last_avail_time = mstime();
    link->last_pong_time = mstime();
    return link;
}

/* Disconnect a hiredis connection in the context of an instance link. */
void instanceLinkCloseConnection(instanceLink *link, redisAsyncContext *c) {
    if (c == NULL) return;

    if (link->cc == c) {
        link->cc = NULL;
        link->pending_commands = 0;
    }
    if (link->pc == c) link->pc = NULL;
    c->data = NULL;
    link->disconnected = 1;
    redisAsyncFree(c);
}

/* Decrement the refcount of a link object, if it drops to zero, actually
 * free it and return NULL. Otherwise don't do anything and return the pointer
 * to the object.
 *
 * If we are not going to free the link and ri is not NULL, we rebind all the
 * pending requests in link->cc (hiredis connection for commands) to a
 * callback that will just ignore them. This is useful to avoid processing
 * replies for an instance that no longer exists. */
instanceLink *releaseInstanceLink(instanceLink *link, sentinelRedisInstance *ri)
{
    serverAssert(link->refcount > 0);
    link->refcount--;
    if (link->refcount != 0) {
        if (ri && ri->link->cc) {
            /* This instance may have pending callbacks in the hiredis async
             * context, having as 'privdata' the instance that we are going to
             * free. Let's rewrite the callback list, directly exploiting
             * hiredis internal data structures, in order to bind them with
             * a callback that will ignore the reply at all. */
            redisCallback *cb;
            redisCallbackList *callbacks = &link->cc->replies;

            cb = callbacks->head;
            while(cb) {
                if (cb->privdata == ri) {
                    cb->fn = sentinelDiscardReplyCallback;
                    cb->privdata = NULL; /* Not strictly needed. */
                }
                cb = cb->next;
            }
        }
        return link; /* Other active users. */
    }

    instanceLinkCloseConnection(link,link->cc);
    instanceLinkCloseConnection(link,link->pc);
    zfree(link);
    return NULL;
}

/* This function will attempt to share the instance link we already have
 * for the same Sentinel in the context of a different master, with the
 * instance we are passing as argument.
 *
 * This way multiple Sentinel objects that refer all to the same physical
 * Sentinel instance but in the context of different masters will use
 * a single connection, will send a single PING per second for failure
 * detection and so forth.
 *
 * Return C_OK if a matching Sentinel was found in the context of a
 * different master and sharing was performed. Otherwise C_ERR
 * is returned. */
int sentinelTryConnectionSharing(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;

    if (ri->runid == NULL) return C_ERR; /* No way to identify it. */
    if (ri->link->refcount > 1) return C_ERR; /* Already shared. */

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        /* We want to share with the same physical Sentinel referenced
         * in other masters, so skip our master. */
        if (master == ri->master) continue;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL,0,ri->runid);
        if (match == NULL) continue; /* No match. */
        if (match == ri) continue; /* Should never happen but... safer. */

        /* We identified a matching Sentinel, great! Let's free our link
         * and use the one of the matching Sentinel. */
        releaseInstanceLink(ri->link,NULL);
        ri->link = match->link;
        match->link->refcount++;
        dictReleaseIterator(di);
        return C_OK;
    }
    dictReleaseIterator(di);
    return C_ERR;
}

/* Drop all connections to other sentinels. Returns the number of connections
 * dropped.*/
int sentinelDropConnections(void) {
    dictIterator *di;
    dictEntry *de;
    int dropped = 0;

    di = dictGetIterator(sentinel.masters);
    while ((de = dictNext(di)) != NULL) {
        dictIterator *sdi;
        dictEntry *sde;

        sentinelRedisInstance *ri = dictGetVal(de);
        sdi = dictGetIterator(ri->sentinels);
        while ((sde = dictNext(sdi)) != NULL) {
            sentinelRedisInstance *si = dictGetVal(sde);
            if (!si->link->disconnected) {
                instanceLinkCloseConnection(si->link, si->link->pc);
                instanceLinkCloseConnection(si->link, si->link->cc);
                dropped++;
            }
        }
        dictReleaseIterator(sdi);
    }
    dictReleaseIterator(di);

    return dropped;
}

/* When we detect a Sentinel to switch address (reporting a different IP/port
 * pair in Hello messages), let's update all the matching Sentinels in the
 * context of other masters as well and disconnect the links, so that everybody
 * will be updated.
 *
 * Return the number of updated Sentinel addresses. */
int sentinelUpdateSentinelAddressInAllMasters(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_SENTINEL);
    dictIterator *di;
    dictEntry *de;
    int reconfigured = 0;

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *master = dictGetVal(de), *match;
        match = getSentinelRedisInstanceByAddrAndRunID(master->sentinels,
                                                       NULL,0,ri->runid);
        /* If there is no match, this master does not know about this
         * Sentinel, try with the next one. */
        if (match == NULL) continue;

        /* Disconnect the old links if connected. */
        if (match->link->cc != NULL)
            instanceLinkCloseConnection(match->link,match->link->cc);
        if (match->link->pc != NULL)
            instanceLinkCloseConnection(match->link,match->link->pc);

        if (match == ri) continue; /* Address already updated for it. */

        /* Update the address of the matching Sentinel by copying the address
         * of the Sentinel object that received the address update. */
        releaseSentinelAddr(match->addr);
        match->addr = dupSentinelAddr(ri->addr);
        reconfigured++;
    }
    dictReleaseIterator(di);
    if (reconfigured)
        sentinelEvent(LL_NOTICE,"+sentinel-address-update", ri,
                      "%@ %d additional matching instances", reconfigured);
    return reconfigured;
}

/* This function is called when a hiredis connection reported an error.
 * We set it to NULL and mark the link as disconnected so that it will be
 * reconnected again.
 *
 * Note: we don't free the hiredis context as hiredis will do it for us
 * for async connections. */
void instanceLinkConnectionError(const redisAsyncContext *c) {
    instanceLink *link = c->data;
    int pubsub;

    if (!link) return;

    pubsub = (link->pc == c);
    if (pubsub)
        link->pc = NULL;
    else
        link->cc = NULL;
    link->disconnected = 1;
}

/* Hiredis connection established / disconnected callbacks. We need them
 * just to cleanup our link state. */
void sentinelLinkEstablishedCallback(const redisAsyncContext *c, int status) {
    if (status != C_OK) instanceLinkConnectionError(c);
}

void sentinelDisconnectCallback(const redisAsyncContext *c, int status) {
    UNUSED(status);
    instanceLinkConnectionError(c);
}

/* ========================== sentinelRedisInstance ========================= */

/* Create a redis instance, the following fields must be populated by the
 * caller if needed:
 *
 * 创建一个 Redis 实例，在有需要时，以下两个域需要从调用者提取：
 *
 * runid: set to NULL but will be populated once INFO output is received.
 *        设置为 NULL ，并在接收到 INFO 命令的回复时设置
 *
 * info_refresh: is set to 0 to mean that we never received INFO so far.
 *               如果这个值为 0 ，那么表示我们未收到过 INFO 信息。
 *
 * If SRI_MASTER is set into initial flags the instance is added to
 * sentinel.masters table.
 *
 * 如果 flags 参数为 SRI_MASTER ，
 * 那么这个实例会被添加到 sentinel.masters 表。
 *
 * if SRI_SLAVE or SRI_SENTINEL is set then 'master' must be not NULL and the
 * instance is added into master->slaves or master->sentinels table.
 *
 * 如果 flags 为 SRI_SLAVE 或者 SRI_SENTINEL ，
 * 那么 master 参数不能为 NULL ，
 * SRI_SLAVE 类型的实例会被添加到 master->slaves 表中，
 * 而 SRI_SENTINEL 类型的实例则会被添加到 master->sentinels 表中。
 *
 * If the instance is a slave or sentinel, the name parameter is ignored and
 * is created automatically as hostname:port.
 *
 * 如果实例是从服务器或者 sentinel ，那么 name 参数会被自动忽略，
 * 实例的名字会被自动设置为 hostname:port 。
 *
 * The function fails if hostname can't be resolved or port is out of range.
 * When this happens NULL is returned and errno is set accordingly to the
 * createSentinelAddr() function.
 *
 * 当 hostname 不能被解释，或者超出范围时，函数将失败。
 * 函数将返回 NULL ，并设置 errno 变量，
 * 具体的出错值请参考 createSentinelAddr() 函数。
 * The function may also fail and return NULL with errno set to EBUSY if
 * a master with the same name, a slave with the same address, or a sentinel
 * with the same ID already exists.
 * 当相同名字的主服务器或者从服务器已经存在时，函数返回 NULL ，
 * 并将 errno 设为 EBUSY 。
 */

sentinelRedisInstance *createSentinelRedisInstance(char *name, int flags, char *hostname, int port, int quorum, sentinelRedisInstance *master) {
    sentinelRedisInstance *ri;
    sentinelAddr *addr;
    dict *table = NULL;
    sds sdsname;

    serverAssert(flags & (SRI_MASTER|SRI_SLAVE|SRI_SENTINEL));
    serverAssert((flags & SRI_MASTER) || master != NULL);

    /* Check address validity. */
    // 保存 IP 地址和端口号到 addr
    addr = createSentinelAddr(hostname,port);
    if (addr == NULL) return NULL;

    /* For slaves use ip/host:port as name. */
    // 如果实例是从服务器或者 sentinel ，那么使用 ip:port 格式为实例设置名字
    if (flags & SRI_SLAVE)
        sdsname = announceSentinelAddrAndPort(addr);
    else
        sdsname = sdsnew(name);

    /* Make sure the entry is not duplicated. This may happen when the same
     * name for a master is used multiple times inside the configuration or
     * if we try to add multiple times a slave or sentinel with same ip/port
     * to a master. */
    // 配置文件中添加了重复的主服务器配置
    // 或者尝试添加一个相同 ip 或者端口号的从服务器或者 sentinel 时
    // 就可能出现重复添加同一个实例的情况
    // 为了避免这种现象，程序在添加新实例之前，需要先检查实例是否已存在
    // 只有不存在的实例会被添加

    // 选择要添加的表
    // 注意主服务会被添加到 sentinel.masters 表
    // 而从服务器和 sentinel 则会被添加到 master 所属的 slaves 表和 sentinels 表中
    if (flags & SRI_MASTER) table = sentinel.masters;
    else if (flags & SRI_SLAVE) table = master->slaves;
    else if (flags & SRI_SENTINEL) table = master->sentinels;
    if (dictFind(table,sdsname)) {
        releaseSentinelAddr(addr);

        // 实例已存在，函数直接返回

        sdsfree(sdsname);
        errno = EBUSY;
        return NULL;
    }

    /* Create the instance object. */
    // 创建实例对象
    ri = zmalloc(sizeof(*ri));
    /* Note that all the instances are started in the disconnected state,
     * the event loop will take care of connecting them. */
    // 所有连接都已断线为起始状态，sentinel 会在需要时自动为它创建连接
    ri->flags = flags;
    ri->name = sdsname;
    ri->runid = NULL;
    ri->config_epoch = 0;
    ri->addr = addr;
    ri->link = createInstanceLink();
    ri->last_pub_time = mstime();
    ri->last_hello_time = mstime();
    ri->last_master_down_reply_time = mstime();
    ri->s_down_since_time = 0;
    ri->o_down_since_time = 0;
    ri->down_after_period = master ? master->down_after_period :
            SENTINEL_DEFAULT_DOWN_AFTER;
    ri->master_link_down_time = 0;
    ri->auth_pass = NULL;
    ri->auth_user = NULL;
    ri->slave_priority = SENTINEL_DEFAULT_SLAVE_PRIORITY;
    ri->replica_announced = 1;
    ri->slave_reconf_sent_time = 0;
    ri->slave_master_host = NULL;
    ri->slave_master_port = 0;
    ri->slave_master_link_status = SENTINEL_MASTER_LINK_STATUS_DOWN;
    ri->slave_repl_offset = 0;
    ri->sentinels = dictCreate(&instancesDictType,NULL);
    ri->quorum = quorum;
    ri->parallel_syncs = SENTINEL_DEFAULT_PARALLEL_SYNCS;
    ri->master = master;
    ri->slaves = dictCreate(&instancesDictType,NULL);
    ri->info_refresh = 0;
    ri->renamed_commands = dictCreate(&renamedCommandsDictType,NULL);

    /* Failover state. */
    ri->leader = NULL;
    ri->leader_epoch = 0;
    ri->failover_epoch = 0;
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = 0;
    ri->failover_start_time = 0;
    ri->failover_timeout = SENTINEL_DEFAULT_FAILOVER_TIMEOUT;
    ri->failover_delay_logged = 0;
    ri->promoted_slave = NULL;
    ri->notification_script = NULL;
    ri->client_reconfig_script = NULL;
    ri->info = NULL;

    /* Role */
    ri->role_reported = ri->flags & (SRI_MASTER|SRI_SLAVE);
    ri->role_reported_time = mstime();
    ri->slave_conf_change_time = mstime();

    /* Add into the right table. */
    // 将实例添加到适当的表中
    dictAdd(table, ri->name, ri);
    // 返回实例
    return ri;
}

/* Release this instance and all its slaves, sentinels, hiredis connections.
 *
 * 释放一个实例，以及它的所有从服务器、sentinel ，以及 hiredis 连接。
 *
 * This function does not take care of unlinking the instance from the main
 * masters table (if it is a master) or from its master sentinels/slaves table
 * if it is a slave or sentinel.
 *
 * 如果这个实例是一个从服务器或者 sentinel ，
 * 那么这个函数也会从该实例所属的主服务器表中删除这个从服务器/sentinel 。
 */
void releaseSentinelRedisInstance(sentinelRedisInstance *ri) {
    /* Release all its slaves or sentinels if any. */
    // 释放（可能有的）sentinel 和 slave
    dictRelease(ri->sentinels);
    dictRelease(ri->slaves);

    /* Disconnect the instance. */
    // 释放连接
    releaseInstanceLink(ri->link,ri);

    /* Free other resources. */
    // 释放其他资源
    sdsfree(ri->name);
    sdsfree(ri->runid);
    sdsfree(ri->notification_script);
    sdsfree(ri->client_reconfig_script);
    sdsfree(ri->slave_master_host);
    sdsfree(ri->leader);
    sdsfree(ri->auth_pass);
    sdsfree(ri->auth_user);
    sdsfree(ri->info);
    releaseSentinelAddr(ri->addr);
    dictRelease(ri->renamed_commands);

    /* Clear state into the master if needed. */
    // 清除故障转移带来的状态
    if ((ri->flags & SRI_SLAVE) && (ri->flags & SRI_PROMOTED) && ri->master)
        ri->master->promoted_slave = NULL;

    zfree(ri);
}

/* Lookup a slave in a master Redis instance, by ip and port. */
// 根据 IP 和端口号，查找主服务器实例的从服务器
sentinelRedisInstance *sentinelRedisInstanceLookupSlave(
        sentinelRedisInstance *ri, char *slave_addr, int port)
        {
    sds key;
    sentinelRedisInstance *slave;
    sentinelAddr *addr;

    serverAssert(ri->flags & SRI_MASTER);

    /* We need to handle a slave_addr that is potentially a hostname.
     * If that is the case, depending on configuration we either resolve
     * it and use the IP addres or fail.
     */
    addr = createSentinelAddr(slave_addr, port);
    if (!addr) return NULL;
    key = announceSentinelAddrAndPort(addr);
    releaseSentinelAddr(addr);

    slave = dictFetchValue(ri->slaves,key);
    sdsfree(key);
    return slave;
        }

        /* Return the name of the type of the instance as a string. */
        // 以字符串形式返回实例的类型
        const char *sentinelRedisInstanceTypeStr(sentinelRedisInstance *ri) {
            if (ri->flags & SRI_MASTER) return "master";
            else if (ri->flags & SRI_SLAVE) return "slave";
            else if (ri->flags & SRI_SENTINEL) return "sentinel";
            else return "unknown";
        }

        /* This function remove the Sentinel with the specified ID from the
         * specified master.
         *
         * If "runid" is NULL the function returns ASAP.
         *
         * This function is useful because on Sentinels address switch, we want to
         * remove our old entry and add a new one for the same ID but with the new
         * address.
         *
         * The function returns 1 if the matching Sentinel was removed, otherwise
         * 0 if there was no Sentinel with this ID. */
        int removeMatchingSentinelFromMaster(sentinelRedisInstance *master, char *runid) {
            dictIterator *di;
            dictEntry *de;
            int removed = 0;

            if (runid == NULL) return 0;

            di = dictGetSafeIterator(master->sentinels);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);

                // 运行 ID 相同，或者 IP 和端口号相同，那么移除该实例
                if (ri->runid && strcmp(ri->runid,runid) == 0) {
                    dictDelete(master->sentinels,ri->name);
                    removed++;
                }
            }
            dictReleaseIterator(di);
            return removed;
        }

        /* Search an instance with the same runid, ip and port into a dictionary
         * of instances. Return NULL if not found, otherwise return the instance
         * pointer.
         * 在给定的实例中查找具有相同 runid 、ip 、port 的实例，
         * 没找到则返回 NULL 。
         * runid or addr can be NULL. In such a case the search is performed only
         * by the non-NULL field.
         * runid 或者 ip 都可以为 NULL ，在这种情况下，函数只检查非空域。
         */
        sentinelRedisInstance *getSentinelRedisInstanceByAddrAndRunID(dict *instances, char *addr, int port, char *runid) {
            dictIterator *di;
            dictEntry *de;
            sentinelRedisInstance *instance = NULL;
            sentinelAddr *ri_addr = NULL;

            serverAssert(addr || runid);   /* User must pass at least one search param. */

            // 遍历所有输入实例

            if (addr != NULL) {
                /* Resolve addr, we use the IP as a key even if a hostname is used */
                ri_addr = createSentinelAddr(addr, port);
                if (!ri_addr) return NULL;
            }
            di = dictGetIterator(instances);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);

                // runid 不相同，忽略该实例
                if (runid && !ri->runid) continue;
                // 检查 ip 和端口号是否相同
                if ((runid == NULL || strcmp(ri->runid, runid) == 0) &&
                (addr == NULL || (strcmp(ri->addr->ip, ri_addr->ip) == 0 &&
                ri->addr->port == port)))
                {
                    instance = ri;
                    break;
                }
            }
            dictReleaseIterator(di);
            if (ri_addr != NULL)
                releaseSentinelAddr(ri_addr);

            return instance;
        }

        // 根据名字查找主服务器
        /* Master lookup by name */
        sentinelRedisInstance *sentinelGetMasterByName(char *name) {
            sentinelRedisInstance *ri;
            sds sdsname = sdsnew(name);

            ri = dictFetchValue(sentinel.masters,sdsname);
            sdsfree(sdsname);
            return ri;
        }

        /* Add the specified flags to all the instances in the specified dictionary. */
        // 为输入的所有实例打开指定的 flags
        void sentinelAddFlagsToDictOfRedisInstances(dict *instances, int flags) {
            dictIterator *di;
            dictEntry *de;

            di = dictGetIterator(instances);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);
                ri->flags |= flags;
            }
            dictReleaseIterator(di);
        }

        /* Remove the specified flags to all the instances in the specified
         * dictionary. */
        // 从字典中移除所有实例的给定 flags
        void sentinelDelFlagsToDictOfRedisInstances(dict *instances, int flags) {
            dictIterator *di;
            dictEntry *de;

            // 遍历所有实例
            di = dictGetIterator(instances);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);
                // 移除 flags
                ri->flags &= ~flags;
            }
            dictReleaseIterator(di);
        }

        /* Reset the state of a monitored master:
         *
         * 重置主服务区的监控状态
         *
         * 1) Remove all slaves.
         *    移除主服务器的所有从服务器
         * 2) Remove all sentinels.
         *    移除主服务器的所有 sentinel
         * 3) Remove most of the flags resulting from runtime operations.
         * 4) Reset timers to their default value. For example after a reset it will be
         *    possible to failover again the same master ASAP, without waiting the
         *    failover timeout delay.
         *    重置计时器为默认值
         * 5) In the process of doing this undo the failover if in progress.
         *    如果故障转移正在执行的话，那么取消该它
         * 6) Disconnect the connections with the master (will reconnect automatically).
         *    断开 sentinel 与主服务器的连接（之后会自动重连）
         */

#define SENTINEL_RESET_NO_SENTINELS (1<<0)
        void sentinelResetMaster(sentinelRedisInstance *ri, int flags) {
            serverAssert(ri->flags & SRI_MASTER);
            dictRelease(ri->slaves);
            ri->slaves = dictCreate(&instancesDictType,NULL);
            if (!(flags & SENTINEL_RESET_NO_SENTINELS)) {
                dictRelease(ri->sentinels);
                ri->sentinels = dictCreate(&instancesDictType,NULL);
            }
            instanceLinkCloseConnection(ri->link,ri->link->cc);
            instanceLinkCloseConnection(ri->link,ri->link->pc);


            // 设置标识为断线的主服务器
            ri->flags &= SRI_MASTER;
            if (ri->leader) {
                sdsfree(ri->leader);
                ri->leader = NULL;
            }
            ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
            ri->failover_state_change_time = 0;
            ri->failover_start_time = 0; /* We can failover again ASAP. */
            ri->promoted_slave = NULL;
            sdsfree(ri->runid);
            sdsfree(ri->slave_master_host);
            ri->runid = NULL;
            ri->slave_master_host = NULL;
            ri->link->act_ping_time = mstime();
            ri->link->last_ping_time = 0;
            ri->link->last_avail_time = mstime();
            ri->link->last_pong_time = mstime();
            ri->role_reported_time = mstime();
            ri->role_reported = SRI_MASTER;
            // 发送主服务器重置事件
            if (flags & SENTINEL_GENERATE_EVENT)
                sentinelEvent(LL_WARNING,"+reset-master",ri,"%@");
        }

        /* Call sentinelResetMaster() on every master with a name matching the specified
         * pattern. */
        // 重置所有符合给定模式的主服务器
        int sentinelResetMastersByPattern(char *pattern, int flags) {
            dictIterator *di;
            dictEntry *de;
            int reset = 0;

            di = dictGetIterator(sentinel.masters);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);

                if (ri->name) {
                    if (stringmatch(pattern,ri->name,0)) {
                        sentinelResetMaster(ri,flags);
                        reset++;
                    }
                }
            }
            dictReleaseIterator(di);
            return reset;
        }

        /* Reset the specified master with sentinelResetMaster(), and also change
         * the ip:port address, but take the name of the instance unmodified.
         *
         * 将 master 实例的 IP 和端口号修改成给定的 ip 和 port ，
         * 但保留 master 原来的名字。
         *
         * This is used to handle the +switch-master event.
         *
         * 这个函数用于处理 +switch-master 事件
         *
         * The function returns C_ERR if the address can't be resolved for some
         * reason. Otherwise C_OK is returned.
         *
         * 函数在无法解释地址时返回 REDIS_ERR ，否则返回 REDIS_OK 。
         */
        int sentinelResetMasterAndChangeAddress(sentinelRedisInstance *master, char *hostname, int port) {
            sentinelAddr *oldaddr, *newaddr;
            sentinelAddr **slaves = NULL;
            int numslaves = 0, j;
            dictIterator *di;
            dictEntry *de;

            // 根据 ip 和 port 参数，创建地址结构
            newaddr = createSentinelAddr(hostname,port);
            if (newaddr == NULL) return C_ERR;

            /* There can be only 0 or 1 slave that has the newaddr.
             * and It can add old master 1 more slave.
             * so It allocates dictSize(master->slaves) + 1          */
            // 创建一个包含原主服务器所有从服务器实例的数组
            // 用于在重置地址之后进行检查
            // 新主服务器（原主服务器的其中一个从服务器）的地址不会包含在这个数组中
            slaves = zmalloc(sizeof(sentinelAddr*)*(dictSize(master->slaves) + 1));

            /* Don't include the one having the address we are switching to. */
            di = dictGetIterator(master->slaves);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *slave = dictGetVal(de);

                // 跳过新主服务器
                if (sentinelAddrIsEqual(slave->addr,newaddr)) continue;



                // 将从服务器保存到数组中
                slaves[numslaves++] = dupSentinelAddr(slave->addr);
            }
            dictReleaseIterator(di);

            /* If we are switching to a different address, include the old address
             * as a slave as well, so that we'll be able to sense / reconfigure
             * the old master. */
            // 如果新地址和 master 的地址不相同，
            // 将 master 的地址也作为从服务器地址添加到保存了所有从服务器地址的数组中
            // （这样等于将下线主服务器设置为新主服务器的从服务器）
            if (!sentinelAddrIsEqual(newaddr,master->addr)) {
                slaves[numslaves++] = dupSentinelAddr(master->addr);
            }

            /* Reset and switch address. */
            // 重置 master 实例结构
            sentinelResetMaster(master,SENTINEL_RESET_NO_SENTINELS);
            oldaddr = master->addr;
            // 为 master 实例设置新的地址
            master->addr = newaddr;
            master->o_down_since_time = 0;
            master->s_down_since_time = 0;

            /* Add slaves back. */
            // 为实例加回之前保存的所有从服务器
            for (j = 0; j < numslaves; j++) {
                sentinelRedisInstance *slave;

                slave = createSentinelRedisInstance(NULL,SRI_SLAVE,slaves[j]->hostname,
                                                    slaves[j]->port, master->quorum, master);
                releaseSentinelAddr(slaves[j]);
                if (slave) sentinelEvent(LL_NOTICE,"+slave",slave,"%@");
            }
            zfree(slaves);

            /* Release the old address at the end so we are safe even if the function
             * gets the master->addr->ip and master->addr->port as arguments. */
            // 释放旧地址
            releaseSentinelAddr(oldaddr);
            sentinelFlushConfig();
            return C_OK;
        }

        /* Return non-zero if there was no SDOWN or ODOWN error associated to this
         * instance in the latest 'ms' milliseconds. */
        // 如果实例在给定 ms 中没有出现过 SDOWN 或者 ODOWN 状态
        // 那么函数返回一个非零值
        int sentinelRedisInstanceNoDownFor(sentinelRedisInstance *ri, mstime_t ms) {
            mstime_t most_recent;

            most_recent = ri->s_down_since_time;
            if (ri->o_down_since_time > most_recent)
                most_recent = ri->o_down_since_time;
            return most_recent == 0 || (mstime() - most_recent) > ms;
        }

        /* Return the current master address, that is, its address or the address
         * of the promoted slave if already operational. */
        // 返回当前主服务器的地址
        // 如果 Sentinel 正在对主服务器进行故障迁移，那么返回新主服务器的地址
        sentinelAddr *sentinelGetCurrentMasterAddress(sentinelRedisInstance *master) {
            /* If we are failing over the master, and the state is already
             * SENTINEL_FAILOVER_STATE_RECONF_SLAVES or greater, it means that we
             * already have the new configuration epoch in the master, and the
             * slave acknowledged the configuration switch. Advertise the new
             * address. */
            if ((master->flags & SRI_FAILOVER_IN_PROGRESS) &&
            master->promoted_slave &&
            master->failover_state >= SENTINEL_FAILOVER_STATE_RECONF_SLAVES)
            {
                return master->promoted_slave->addr;
            } else {
                return master->addr;
            }
        }

        /* This function sets the down_after_period field value in 'master' to all
         * the slaves and sentinel instances connected to this master. */
        void sentinelPropagateDownAfterPeriod(sentinelRedisInstance *master) {
            dictIterator *di;
            dictEntry *de;
            int j;
            dict *d[] = {master->slaves, master->sentinels, NULL};

            for (j = 0; d[j]; j++) {
                di = dictGetIterator(d[j]);
                while((de = dictNext(di)) != NULL) {
                    sentinelRedisInstance *ri = dictGetVal(de);
                    ri->down_after_period = master->down_after_period;
                }
                dictReleaseIterator(di);
            }
        }

        /* This function is used in order to send commands to Redis instances: the
         * commands we send from Sentinel may be renamed, a common case is a master
         * with CONFIG and SLAVEOF commands renamed for security concerns. In that
         * case we check the ri->renamed_command table (or if the instance is a slave,
         * we check the one of the master), and map the command that we should send
         * to the set of renamed commands. However, if the command was not renamed,
         * we just return "command" itself. */
        char *sentinelInstanceMapCommand(sentinelRedisInstance *ri, char *command) {
            sds sc = sdsnew(command);
            if (ri->master) ri = ri->master;
            char *retval = dictFetchValue(ri->renamed_commands, sc);
            sdsfree(sc);
            return retval ? retval : command;
        }

        /* ============================ Config handling ============================= */

        /* Generalise handling create instance error. Use SRI_MASTER, SRI_SLAVE or
         * SRI_SENTINEL as a role value. */
        // Sentinel 配置文件分析器
        const char *sentinelCheckCreateInstanceErrors(int role) {
            switch(errno) {
                case EBUSY:
                    switch (role) {
                        case SRI_MASTER:
                            return "Duplicate master name.";
                            case SRI_SLAVE:
                                return "Duplicate hostname and port for replica.";
                                case SRI_SENTINEL:
                                    return "Duplicate runid for sentinel.";
                                    default:
                                        serverAssert(0);
                                        break;
                    }
                    break;
                case ENOENT:
                    return "Can't resolve instance hostname.";
                    case EINVAL:
                        return "Invalid port number.";
                        default:
                            return "Unknown Error for creating instances.";
            }
        }

        /* init function for server.sentinel_config */
        void initializeSentinelConfig() {
            server.sentinel_config = zmalloc(sizeof(struct sentinelConfig));
            server.sentinel_config->monitor_cfg = listCreate();
            server.sentinel_config->pre_monitor_cfg = listCreate();
            server.sentinel_config->post_monitor_cfg = listCreate();
            listSetFreeMethod(server.sentinel_config->monitor_cfg,freeSentinelLoadQueueEntry);
            listSetFreeMethod(server.sentinel_config->pre_monitor_cfg,freeSentinelLoadQueueEntry);
            listSetFreeMethod(server.sentinel_config->post_monitor_cfg,freeSentinelLoadQueueEntry);
        }

        /* destroy function for server.sentinel_config */
        void freeSentinelConfig() {
            /* release these three config queues since we will not use it anymore */
            listRelease(server.sentinel_config->pre_monitor_cfg);
            listRelease(server.sentinel_config->monitor_cfg);
            listRelease(server.sentinel_config->post_monitor_cfg);
            zfree(server.sentinel_config);
            server.sentinel_config = NULL;
        }

        /* Search config name in pre monitor config name array, return 1 if found,
         * 0 if not found. */
        int searchPreMonitorCfgName(const char *name) {
            for (unsigned int i = 0; i < sizeof(preMonitorCfgName)/sizeof(preMonitorCfgName[0]); i++) {
                if (!strcasecmp(preMonitorCfgName[i],name)) return 1;
            }
            return 0;
        }

        /* free method for sentinelLoadQueueEntry when release the list */
        void freeSentinelLoadQueueEntry(void *item) {
            struct sentinelLoadQueueEntry *entry = item;
            sdsfreesplitres(entry->argv,entry->argc);
            sdsfree(entry->line);
            zfree(entry);
        }

        /* This function is used for queuing sentinel configuration, the main
         * purpose of this function is to delay parsing the sentinel config option
         * in order to avoid the order dependent issue from the config. */
        void queueSentinelConfig(sds *argv, int argc, int linenum, sds line) {
            int i;
            struct sentinelLoadQueueEntry *entry;

            /* initialize sentinel_config for the first call */
            if (server.sentinel_config == NULL) initializeSentinelConfig();

            entry = zmalloc(sizeof(struct sentinelLoadQueueEntry));
            entry->argv = zmalloc(sizeof(char*)*argc);
            entry->argc = argc;
            entry->linenum = linenum;
            entry->line = sdsdup(line);
            for (i = 0; i < argc; i++) {
                entry->argv[i] = sdsdup(argv[i]);
            }
            /*  Separate config lines with pre monitor config, monitor config and
             *  post monitor config, in order to parsing config dependencies
             *  correctly. */
            if (!strcasecmp(argv[0],"monitor")) {
                listAddNodeTail(server.sentinel_config->monitor_cfg,entry);
            } else if (searchPreMonitorCfgName(argv[0])) {
                listAddNodeTail(server.sentinel_config->pre_monitor_cfg,entry);
            } else{
                listAddNodeTail(server.sentinel_config->post_monitor_cfg,entry);
            }
        }

        /* This function is used for loading the sentinel configuration from
         * pre_monitor_cfg, monitor_cfg and post_monitor_cfg list */
        void loadSentinelConfigFromQueue(void) {
            const char *err = NULL;
            listIter li;
            listNode *ln;
            int linenum = 0;
            sds line = NULL;

            /* if there is no sentinel_config entry, we can return immediately */
            if (server.sentinel_config == NULL) return;

            /* loading from pre monitor config queue first to avoid dependency issues */
            listRewind(server.sentinel_config->pre_monitor_cfg,&li);
            while((ln = listNext(&li))) {
                struct sentinelLoadQueueEntry *entry = ln->value;
                err = sentinelHandleConfiguration(entry->argv,entry->argc);
                if (err) {
                    linenum = entry->linenum;
                    line = entry->line;
                    goto loaderr;
                }
            }

            /* loading from monitor config queue */
            listRewind(server.sentinel_config->monitor_cfg,&li);
            while((ln = listNext(&li))) {
                struct sentinelLoadQueueEntry *entry = ln->value;
                err = sentinelHandleConfiguration(entry->argv,entry->argc);
                if (err) {
                    linenum = entry->linenum;
                    line = entry->line;
                    goto loaderr;
                }
            }

            /* loading from the post monitor config queue */
            listRewind(server.sentinel_config->post_monitor_cfg,&li);
            while((ln = listNext(&li))) {
                struct sentinelLoadQueueEntry *entry = ln->value;
                err = sentinelHandleConfiguration(entry->argv,entry->argc);
                if (err) {
                    linenum = entry->linenum;
                    line = entry->line;
                    goto loaderr;
                }
            }

            /* free sentinel_config when config loading is finished */
            freeSentinelConfig();
            return;

            loaderr:
            fprintf(stderr, "\n*** FATAL CONFIG FILE ERROR (Redis %s) ***\n",
                    REDIS_VERSION);
            fprintf(stderr, "Reading the configuration file, at line %d\n", linenum);
            fprintf(stderr, ">>> '%s'\n", line);
            fprintf(stderr, "%s\n", err);
            exit(1);
        }

        const char *sentinelHandleConfiguration(char **argv, int argc) {

            sentinelRedisInstance *ri;

            // SENTINEL monitor 选项
            if (!strcasecmp(argv[0],"monitor") && argc == 5) {
                /* monitor <name> <host> <port> <quorum> */

                // 读入 quorum 参数
                int quorum = atoi(argv[4]);

                // 检查 quorum 参数必须大于 0
                if (quorum <= 0) return "Quorum must be 1 or greater.";

                // 创建主服务器实例
                if (createSentinelRedisInstance(argv[1],SRI_MASTER,argv[2],
                                                atoi(argv[3]),quorum,NULL) == NULL)
                {
                    return sentinelCheckCreateInstanceErrors(SRI_MASTER);
                }

                // SENTINEL down-after-milliseconds 选项
            } else if (!strcasecmp(argv[0],"down-after-milliseconds") && argc == 3) {

                /* down-after-milliseconds <name> <milliseconds> */

                // 查找主服务器
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";

                // 设置选项
                ri->down_after_period = atoi(argv[2]);
                if (ri->down_after_period <= 0)
                    return "negative or zero time parameter.";

                sentinelPropagateDownAfterPeriod(ri);

                // SENTINEL failover-timeout 选项
            } else if (!strcasecmp(argv[0],"failover-timeout") && argc == 3) {

                /* failover-timeout <name> <milliseconds> */

                // 查找主服务器
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";

                // 设置选项
                ri->failover_timeout = atoi(argv[2]);
                if (ri->failover_timeout <= 0)
                    return "negative or zero time parameter.";

                // Sentinel parallel-syncs 选项
            } else if (!strcasecmp(argv[0],"parallel-syncs") && argc == 3) {
                /* parallel-syncs <name> <milliseconds> */

                // 查找主服务器
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";

                // 设置选项
                ri->parallel_syncs = atoi(argv[2]);


                // SENTINEL notification-script 选项
            } else if (!strcasecmp(argv[0],"notification-script") && argc == 3) {
                /* notification-script <name> <path> */

                // 查找主服务器
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";

                // 检查给定路径所指向的文件是否存在，以及是否可执行
                if (access(argv[2],X_OK) == -1)
                    return "Notification script seems non existing or non executable.";

                // 设置选项
                ri->notification_script = sdsnew(argv[2]);


                // SENTINEL client-reconfig-script 选项
            } else if (!strcasecmp(argv[0],"client-reconfig-script") && argc == 3) {
                /* client-reconfig-script <name> <path> */

                // 查找主服务器
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";
                // 检查给定路径所指向的文件是否存在，以及是否可执行
                if (access(argv[2],X_OK) == -1)
                    return "Client reconfiguration script seems non existing or "
                           "non executable.";

                // 设置选项
                ri->client_reconfig_script = sdsnew(argv[2]);


                // 设置 SENTINEL auth-pass 选项
            } else if (!strcasecmp(argv[0],"auth-pass") && argc == 3) {
                /* auth-pass <name> <password> */

                // 查找主服务器
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";

                // 设置选项
                ri->auth_pass = sdsnew(argv[2]);



            } else if (!strcasecmp(argv[0],"auth-user") && argc == 3) {
                /* auth-user <name> <username> */
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";
                ri->auth_user = sdsnew(argv[2]);
            } else if (!strcasecmp(argv[0],"current-epoch") && argc == 2) {
                /* current-epoch <epoch> */
                unsigned long long current_epoch = strtoull(argv[1],NULL,10);
                if (current_epoch > sentinel.current_epoch)
                    sentinel.current_epoch = current_epoch;
            } else if (!strcasecmp(argv[0],"myid") && argc == 2) {
                if (strlen(argv[1]) != CONFIG_RUN_ID_SIZE)
                    return "Malformed Sentinel id in myid option.";
                memcpy(sentinel.myid,argv[1],CONFIG_RUN_ID_SIZE);
            } else if (!strcasecmp(argv[0],"config-epoch") && argc == 3) {
                /* config-epoch <name> <epoch> */
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";
                ri->config_epoch = strtoull(argv[2],NULL,10);
                /* The following update of current_epoch is not really useful as
                 * now the current epoch is persisted on the config file, but
                 * we leave this check here for redundancy. */
                if (ri->config_epoch > sentinel.current_epoch)
                    sentinel.current_epoch = ri->config_epoch;


                // SENTINEL config-epoch 选项
            } else if (!strcasecmp(argv[0],"leader-epoch") && argc == 3) {
                /* leader-epoch <name> <epoch> */
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";
                ri->leader_epoch = strtoull(argv[2],NULL,10);


                // SENTINEL known-slave 选项
            } else if ((!strcasecmp(argv[0],"known-slave") ||
            !strcasecmp(argv[0],"known-replica")) && argc == 4)
            {
                sentinelRedisInstance *slave;

                /* known-replica <name> <ip> <port> */
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";
                if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,argv[2],
                                                         atoi(argv[3]), ri->quorum, ri)) == NULL)
                {
                    return sentinelCheckCreateInstanceErrors(SRI_SLAVE);
                }
            } else if (!strcasecmp(argv[0],"known-sentinel") &&
            (argc == 4 || argc == 5)) {
                sentinelRedisInstance *si;

                if (argc == 5) { /* Ignore the old form without runid. */
                    /* known-sentinel <name> <ip> <port> [runid] */
                    ri = sentinelGetMasterByName(argv[1]);
                    if (!ri) return "No such master with specified name.";
                    if ((si = createSentinelRedisInstance(argv[4],SRI_SENTINEL,argv[2],
                                                          atoi(argv[3]), ri->quorum, ri)) == NULL)
                    {
                        return sentinelCheckCreateInstanceErrors(SRI_SENTINEL);
                    }
                    si->runid = sdsnew(argv[4]);
                    sentinelTryConnectionSharing(si);
                }
            } else if (!strcasecmp(argv[0],"rename-command") && argc == 4) {
                /* rename-command <name> <command> <renamed-command> */
                ri = sentinelGetMasterByName(argv[1]);
                if (!ri) return "No such master with specified name.";
                sds oldcmd = sdsnew(argv[2]);
                sds newcmd = sdsnew(argv[3]);
                if (dictAdd(ri->renamed_commands,oldcmd,newcmd) != DICT_OK) {
                    sdsfree(oldcmd);
                    sdsfree(newcmd);
                    return "Same command renamed multiple times with rename-command.";
                }
            } else if (!strcasecmp(argv[0],"announce-ip") && argc == 2) {
                /* announce-ip <ip-address> */
                if (strlen(argv[1]))
                    sentinel.announce_ip = sdsnew(argv[1]);
            } else if (!strcasecmp(argv[0],"announce-port") && argc == 2) {
                /* announce-port <port> */
                sentinel.announce_port = atoi(argv[1]);
            } else if (!strcasecmp(argv[0],"deny-scripts-reconfig") && argc == 2) {
                /* deny-scripts-reconfig <yes|no> */
                if ((sentinel.deny_scripts_reconfig = yesnotoi(argv[1])) == -1) {
                    return "Please specify yes or no for the "
                           "deny-scripts-reconfig options.";
                }
            } else if (!strcasecmp(argv[0],"sentinel-user") && argc == 2) {
                /* sentinel-user <user-name> */
                if (strlen(argv[1]))
                    sentinel.sentinel_auth_user = sdsnew(argv[1]);
            } else if (!strcasecmp(argv[0],"sentinel-pass") && argc == 2) {
                /* sentinel-pass <password> */
                if (strlen(argv[1]))
                    sentinel.sentinel_auth_pass = sdsnew(argv[1]);
            } else if (!strcasecmp(argv[0],"resolve-hostnames") && argc == 2) {
                /* resolve-hostnames <yes|no> */
                if ((sentinel.resolve_hostnames = yesnotoi(argv[1])) == -1) {
                    return "Please specify yes or no for the resolve-hostnames option.";
                }
            } else if (!strcasecmp(argv[0],"announce-hostnames") && argc == 2) {
                /* announce-hostnames <yes|no> */
                if ((sentinel.announce_hostnames = yesnotoi(argv[1])) == -1) {
                    return "Please specify yes or no for the announce-hostnames option.";
                }
            } else {
                return "Unrecognized sentinel configuration statement.";
            }
            return NULL;
        }

        /* Implements CONFIG REWRITE for "sentinel" option.
         * This is used not just to rewrite the configuration given by the user
         * (the configured masters) but also in order to retain the state of
         * Sentinel across restarts: config epoch of masters, associated slaves
         * and sentinel instances, and so forth. */
        // CONFIG REWIRTE 命令中和 sentinel 选项有关的部分
        // 这个函数不仅用于用户执行 CONFIG REWRITE 的时候，
        // 也用于保存 Sentinel 状态，以备 Sentinel 重启时载入状态使用
        void rewriteConfigSentinelOption(struct rewriteConfigState *state) {
            dictIterator *di, *di2;
            dictEntry *de;
            sds line;

            /* sentinel unique ID. */
            line = sdscatprintf(sdsempty(), "sentinel myid %s", sentinel.myid);
            rewriteConfigRewriteLine(state,"sentinel myid",line,1);

            /* sentinel deny-scripts-reconfig. */
            line = sdscatprintf(sdsempty(), "sentinel deny-scripts-reconfig %s",
                                sentinel.deny_scripts_reconfig ? "yes" : "no");
            rewriteConfigRewriteLine(state,"sentinel deny-scripts-reconfig",line,
                                     sentinel.deny_scripts_reconfig != SENTINEL_DEFAULT_DENY_SCRIPTS_RECONFIG);

            /* sentinel resolve-hostnames.
             * This must be included early in the file so it is already in effect
             * when reading the file.
             */
            line = sdscatprintf(sdsempty(), "sentinel resolve-hostnames %s",
                                sentinel.resolve_hostnames ? "yes" : "no");
            rewriteConfigRewriteLine(state,"sentinel resolve-hostnames",line,
                                     sentinel.resolve_hostnames != SENTINEL_DEFAULT_RESOLVE_HOSTNAMES);

            /* sentinel announce-hostnames. */
            line = sdscatprintf(sdsempty(), "sentinel announce-hostnames %s",
                                sentinel.announce_hostnames ? "yes" : "no");
            rewriteConfigRewriteLine(state,"sentinel announce-hostnames",line,
                                     sentinel.announce_hostnames != SENTINEL_DEFAULT_ANNOUNCE_HOSTNAMES);

            /* For every master emit a "sentinel monitor" config entry. */
            di = dictGetIterator(sentinel.masters);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *master, *ri;
                sentinelAddr *master_addr;

                /* sentinel monitor */
                master = dictGetVal(de);
                master_addr = sentinelGetCurrentMasterAddress(master);
                line = sdscatprintf(sdsempty(),"sentinel monitor %s %s %d %d",
                                    master->name, announceSentinelAddr(master_addr), master_addr->port,
                                    master->quorum);
                rewriteConfigRewriteLine(state,"sentinel monitor",line,1);
                /* rewriteConfigMarkAsProcessed is handled after the loop */

                /* sentinel down-after-milliseconds */
                if (master->down_after_period != SENTINEL_DEFAULT_DOWN_AFTER) {
                    line = sdscatprintf(sdsempty(),
                                        "sentinel down-after-milliseconds %s %ld",
                                        master->name, (long) master->down_after_period);
                    rewriteConfigRewriteLine(state,"sentinel down-after-milliseconds",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }

                /* sentinel failover-timeout */
                if (master->failover_timeout != SENTINEL_DEFAULT_FAILOVER_TIMEOUT) {
                    line = sdscatprintf(sdsempty(),
                                        "sentinel failover-timeout %s %ld",
                                        master->name, (long) master->failover_timeout);
                    rewriteConfigRewriteLine(state,"sentinel failover-timeout",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */

                }

                /* sentinel parallel-syncs */
                if (master->parallel_syncs != SENTINEL_DEFAULT_PARALLEL_SYNCS) {
                    line = sdscatprintf(sdsempty(),
                                        "sentinel parallel-syncs %s %d",
                                        master->name, master->parallel_syncs);
                    rewriteConfigRewriteLine(state,"sentinel parallel-syncs",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }

                /* sentinel notification-script */
                if (master->notification_script) {
                    line = sdscatprintf(sdsempty(),
                                        "sentinel notification-script %s %s",
                                        master->name, master->notification_script);
                    rewriteConfigRewriteLine(state,"sentinel notification-script",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }

                /* sentinel client-reconfig-script */
                if (master->client_reconfig_script) {
                    line = sdscatprintf(sdsempty(),
                                        "sentinel client-reconfig-script %s %s",
                                        master->name, master->client_reconfig_script);
                    rewriteConfigRewriteLine(state,"sentinel client-reconfig-script",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }

                /* sentinel auth-pass & auth-user */
                if (master->auth_pass) {
                    line = sdscatprintf(sdsempty(),
                                        "sentinel auth-pass %s %s",
                                        master->name, master->auth_pass);
                    rewriteConfigRewriteLine(state,"sentinel auth-pass",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }

                if (master->auth_user) {
                    line = sdscatprintf(sdsempty(),
                                        "sentinel auth-user %s %s",
                                        master->name, master->auth_user);
                    rewriteConfigRewriteLine(state,"sentinel auth-user",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }

                /* sentinel config-epoch */
                line = sdscatprintf(sdsempty(),
                                    "sentinel config-epoch %s %llu",
                                    master->name, (unsigned long long) master->config_epoch);
                rewriteConfigRewriteLine(state,"sentinel config-epoch",line,1);
                /* rewriteConfigMarkAsProcessed is handled after the loop */


                /* sentinel leader-epoch */
                line = sdscatprintf(sdsempty(),
                                    "sentinel leader-epoch %s %llu",
                                    master->name, (unsigned long long) master->leader_epoch);
                rewriteConfigRewriteLine(state,"sentinel leader-epoch",line,1);
                /* rewriteConfigMarkAsProcessed is handled after the loop */

                /* sentinel known-slave */
                di2 = dictGetIterator(master->slaves);
                while((de = dictNext(di2)) != NULL) {
                    sentinelAddr *slave_addr;

                    ri = dictGetVal(de);
                    slave_addr = ri->addr;

                    /* If master_addr (obtained using sentinelGetCurrentMasterAddress()
                     * so it may be the address of the promoted slave) is equal to this
                     * slave's address, a failover is in progress and the slave was
                     * already successfully promoted. So as the address of this slave
                     * we use the old master address instead. */
                    if (sentinelAddrIsEqual(slave_addr,master_addr))
                        slave_addr = master->addr;
                    line = sdscatprintf(sdsempty(),
                                        "sentinel known-replica %s %s %d",
                                        master->name, announceSentinelAddr(slave_addr), slave_addr->port);
                    rewriteConfigRewriteLine(state,"sentinel known-replica",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }
                dictReleaseIterator(di2);

                /* sentinel known-sentinel */
                di2 = dictGetIterator(master->sentinels);
                while((de = dictNext(di2)) != NULL) {
                    ri = dictGetVal(de);
                    if (ri->runid == NULL) continue;
                    line = sdscatprintf(sdsempty(),
                                        "sentinel known-sentinel %s %s %d %s",
                                        master->name, announceSentinelAddr(ri->addr), ri->addr->port, ri->runid);
                    rewriteConfigRewriteLine(state,"sentinel known-sentinel",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }
                dictReleaseIterator(di2);

                /* sentinel rename-command */
                di2 = dictGetIterator(master->renamed_commands);
                while((de = dictNext(di2)) != NULL) {
                    sds oldname = dictGetKey(de);
                    sds newname = dictGetVal(de);
                    line = sdscatprintf(sdsempty(),
                                        "sentinel rename-command %s %s %s",
                                        master->name, oldname, newname);
                    rewriteConfigRewriteLine(state,"sentinel rename-command",line,1);
                    /* rewriteConfigMarkAsProcessed is handled after the loop */
                }
                dictReleaseIterator(di2);
            }

            /* sentinel current-epoch is a global state valid for all the masters. */
            line = sdscatprintf(sdsempty(),
                                "sentinel current-epoch %llu", (unsigned long long) sentinel.current_epoch);
            rewriteConfigRewriteLine(state,"sentinel current-epoch",line,1);

            /* sentinel announce-ip. */
            if (sentinel.announce_ip) {
                line = sdsnew("sentinel announce-ip ");
                line = sdscatrepr(line, sentinel.announce_ip, sdslen(sentinel.announce_ip));
                rewriteConfigRewriteLine(state,"sentinel announce-ip",line,1);
            } else {
                rewriteConfigMarkAsProcessed(state,"sentinel announce-ip");
            }

            /* sentinel announce-port. */
            if (sentinel.announce_port) {
                line = sdscatprintf(sdsempty(),"sentinel announce-port %d",
                                    sentinel.announce_port);
                rewriteConfigRewriteLine(state,"sentinel announce-port",line,1);
            } else {
                rewriteConfigMarkAsProcessed(state,"sentinel announce-port");
            }

            /* sentinel sentinel-user. */
            if (sentinel.sentinel_auth_user) {
                line = sdscatprintf(sdsempty(), "sentinel sentinel-user %s", sentinel.sentinel_auth_user);
                rewriteConfigRewriteLine(state,"sentinel sentinel-user",line,1);
            } else {
                rewriteConfigMarkAsProcessed(state,"sentinel sentinel-user");
            }

            /* sentinel sentinel-pass. */
            if (sentinel.sentinel_auth_pass) {
                line = sdscatprintf(sdsempty(), "sentinel sentinel-pass %s", sentinel.sentinel_auth_pass);
                rewriteConfigRewriteLine(state,"sentinel sentinel-pass",line,1);
            } else {
                rewriteConfigMarkAsProcessed(state,"sentinel sentinel-pass");
            }

            dictReleaseIterator(di);

            /* NOTE: the purpose here is in case due to the state change, the config rewrite
             does not handle the configs, however, previously the config was set in the config file,
             rewriteConfigMarkAsProcessed should be put here to mark it as processed in order to
             delete the old config entry.
            */
            rewriteConfigMarkAsProcessed(state,"sentinel monitor");
            rewriteConfigMarkAsProcessed(state,"sentinel down-after-milliseconds");
            rewriteConfigMarkAsProcessed(state,"sentinel failover-timeout");
            rewriteConfigMarkAsProcessed(state,"sentinel parallel-syncs");
            rewriteConfigMarkAsProcessed(state,"sentinel notification-script");
            rewriteConfigMarkAsProcessed(state,"sentinel client-reconfig-script");
            rewriteConfigMarkAsProcessed(state,"sentinel auth-pass");
            rewriteConfigMarkAsProcessed(state,"sentinel auth-user");
            rewriteConfigMarkAsProcessed(state,"sentinel config-epoch");
            rewriteConfigMarkAsProcessed(state,"sentinel leader-epoch");
            rewriteConfigMarkAsProcessed(state,"sentinel known-replica");
            rewriteConfigMarkAsProcessed(state,"sentinel known-sentinel");
            rewriteConfigMarkAsProcessed(state,"sentinel rename-command");
        }

        /* This function uses the config rewriting Redis engine in order to persist
         * the state of the Sentinel in the current configuration file.
         *
         * 使用 CONFIG REWRITE 功能，将当前 Sentinel 的状态持久化到配置文件里面。
         *
         * Before returning the function calls fsync() against the generated
         * configuration file to make sure changes are committed to disk.
         *
         * 在函数返回之前，程序会调用一次 fsync() ，确保文件已经被保存到磁盘里面。
         *
         * On failure the function logs a warning on the Redis log.
         *
         * 如果保存失败，那么打印一条警告日志。
         */
        void sentinelFlushConfig(void) {
            int fd = -1;
            int saved_hz = server.hz;
            int rewrite_status;

            server.hz = CONFIG_DEFAULT_HZ;
            rewrite_status = rewriteConfig(server.configfile, 0);
            server.hz = saved_hz;

            if (rewrite_status == -1) goto werr;
            if ((fd = open(server.configfile,O_RDONLY)) == -1) goto werr;
            if (fsync(fd) == -1) goto werr;
            if (close(fd) == EOF) goto werr;
            return;

            werr:
            serverLog(LL_WARNING,"WARNING: Sentinel was not able to save the new configuration on disk!!!: %s", strerror(errno));
            if (fd != -1) close(fd);
        }

        /* ====================== hiredis connection handling ======================= */

        /* Send the AUTH command with the specified master password if needed.
         * Note that for slaves the password set for the master is used.
         *
         * In case this Sentinel requires a password as well, via the "requirepass"
         * configuration directive, we assume we should use the local password in
         * order to authenticate when connecting with the other Sentinels as well.
         * So basically all the Sentinels share the same password and use it to
         * authenticate reciprocally.
         *
         * 如果 sentinel 设置了 auth-pass 选项，那么向主服务器或者从服务器发送验证密码。
         * 注意从服务器使用的是主服务器的密码。
         * We don't check at all if the command was successfully transmitted
         * to the instance as if it fails Sentinel will detect the instance down,
         * will disconnect and reconnect the link and so forth.
         *
         * 函数不检查命令是否被成功发送，因为如果目标服务器掉线了的话， sentinel 会识别到，
         * 并对它进行重连接，然后又重新发送 AUTH 命令。
         */
        void sentinelSendAuthIfNeeded(sentinelRedisInstance *ri, redisAsyncContext *c) {
            char *auth_pass = NULL;
            char *auth_user = NULL;

            // 如果 ri 是主服务器，那么使用实例自己的密码
            // 如果 ri 是从服务器，那么使用主服务器的密码
            if (ri->flags & SRI_MASTER) {
                auth_pass = ri->auth_pass;
                auth_user = ri->auth_user;
            } else if (ri->flags & SRI_SLAVE) {
                auth_pass = ri->master->auth_pass;
                auth_user = ri->master->auth_user;
            } else if (ri->flags & SRI_SENTINEL) {
                /* If sentinel_auth_user is NULL, AUTH will use default user
                   with sentinel_auth_pass to authenticate */
                if (sentinel.sentinel_auth_pass) {
                    auth_pass = sentinel.sentinel_auth_pass;
                    auth_user = sentinel.sentinel_auth_user;
                } else {
                    /* Compatibility with old configs. requirepass is used
                     * for both incoming and outgoing authentication. */
                    auth_pass = server.requirepass;
                    auth_user = NULL;
                }
            }


            // 发送 AUTH 命令
            if (auth_pass && auth_user == NULL) {
                if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s",
                                      sentinelInstanceMapCommand(ri,"AUTH"),
                                      auth_pass) == C_OK) ri->link->pending_commands++;
            } else if (auth_pass && auth_user) {
                /* If we also have an username, use the ACL-style AUTH command
                 * with two arguments, username and password. */
                if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri, "%s %s %s",
                                      sentinelInstanceMapCommand(ri,"AUTH"),
                                      auth_user, auth_pass) == C_OK) ri->link->pending_commands++;
            }
        }

        /* Use CLIENT SETNAME to name the connection in the Redis instance as
         * sentinel-<first_8_chars_of_runid>-<connection_type>
         * The connection type is "cmd" or "pubsub" as specified by 'type'.
         *
         * This makes it possible to list all the sentinel instances connected
         * to a Redis server with CLIENT LIST, grepping for a specific name format. */
        // 使用 CLIENT SETNAME 命令，为给定的客户端设置名字。
        void sentinelSetClientName(sentinelRedisInstance *ri, redisAsyncContext *c, char *type) {
            char name[64];

            snprintf(name,sizeof(name),"sentinel-%.8s-%s",sentinel.myid,type);
            if (redisAsyncCommand(c, sentinelDiscardReplyCallback, ri,
                                  "%s SETNAME %s",
                                  sentinelInstanceMapCommand(ri,"CLIENT"),
                                  name) == C_OK)
            {
                ri->link->pending_commands++;
            }
        }

        static int instanceLinkNegotiateTLS(redisAsyncContext *context) {
#ifndef USE_OPENSSL
            (void) context;
#else
            if (!redis_tls_ctx) return C_ERR;
            SSL *ssl = SSL_new(redis_tls_client_ctx ? redis_tls_client_ctx : redis_tls_ctx);
            if (!ssl) return C_ERR;

            if (redisInitiateSSL(&context->c, ssl) == REDIS_ERR) return C_ERR;
#endif
            return C_OK;
        }

        /* Create the async connections for the instance link if the link
         * is disconnected. Note that link->disconnected is true even if just
         * one of the two links (commands and pub/sub) is missing. */
        // 如果 sentinel 与实例处于断线（未连接）状态，那么创建连向实例的异步连接。
        void sentinelReconnectInstance(sentinelRedisInstance *ri) {

            // 示例未断线（已连接），返回
            if (ri->link->disconnected == 0) return;
            if (ri->addr->port == 0) return; /* port == 0 means invalid address. */
            instanceLink *link = ri->link;
            mstime_t now = mstime();

            if (now - ri->link->last_reconn_time < SENTINEL_PING_PERIOD) return;
            ri->link->last_reconn_time = now;

            /* Commands connection. */
            // 对所有实例创建一个用于发送 Redis 命令的连接
            if (link->cc == NULL) {

                // 连接实例
                link->cc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);
                if (link->cc && !link->cc->err) anetCloexec(link->cc->c.fd);
                if (!link->cc) {

                    sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #Failed to establish connection");


                    // 连接出错
                } else if (!link->cc->err && server.tls_replication &&
                (instanceLinkNegotiateTLS(link->cc) == C_ERR)) {
                    sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #Failed to initialize TLS");
                    instanceLinkCloseConnection(link,link->cc);
                } else if (link->cc->err) {
                    sentinelEvent(LL_DEBUG,"-cmd-link-reconnection",ri,"%@ #%s",
                                  link->cc->errstr);
                    instanceLinkCloseConnection(link,link->cc);


                    // 连接成功
                } else {

                    // 设置连接属性
                    link->pending_commands = 0;
                    link->cc_conn_time = mstime();
                    link->cc->data = link;
                    redisAeAttach(server.el,link->cc);

                    // 设置连线 callback
                    redisAsyncSetConnectCallback(link->cc,
                                                 sentinelLinkEstablishedCallback);


                    // 设置断线 callback
                    redisAsyncSetDisconnectCallback(link->cc,
                                                    sentinelDisconnectCallback);

                    // 发送 AUTH 命令，验证身份
                    sentinelSendAuthIfNeeded(ri,link->cc);
                    sentinelSetClientName(ri,link->cc,"cmd");

                    /* Send a PING ASAP when reconnecting. */
                    sentinelSendPing(ri);
                }
            }
            /* Pub / Sub */

            // 对主服务器和从服务器，创建一个用于订阅频道的连接
            if ((ri->flags & (SRI_MASTER|SRI_SLAVE)) && link->pc == NULL) {

                // 连接实例
                link->pc = redisAsyncConnectBind(ri->addr->ip,ri->addr->port,NET_FIRST_BIND_ADDR);
                if (link->pc && !link->pc->err) anetCloexec(link->pc->c.fd);
                if (!link->pc) {
                    sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #Failed to establish connection");
                } else if (!link->pc->err && server.tls_replication &&
                (instanceLinkNegotiateTLS(link->pc) == C_ERR)) {
                    sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #Failed to initialize TLS");


                    // 连接出错
                } else if (link->pc->err) {
                    sentinelEvent(LL_DEBUG,"-pubsub-link-reconnection",ri,"%@ #%s",
                                  link->pc->errstr);
                    instanceLinkCloseConnection(link,link->pc);


                    // 连接成功
                } else {
                    int retval;
                    // 设置连接属性
                    link->pc_conn_time = mstime();
                    link->pc->data = link;
                    redisAeAttach(server.el,link->pc);

                    // 设置连接 callback
                    redisAsyncSetConnectCallback(link->pc,
                                                 sentinelLinkEstablishedCallback);


                    // 设置断线 callback
                    redisAsyncSetDisconnectCallback(link->pc,
                                                    sentinelDisconnectCallback);

                    // 发送 AUTH 命令，验证身份
                    sentinelSendAuthIfNeeded(ri,link->pc);


                    // 为客户但设置名字 "pubsub"
                    sentinelSetClientName(ri,link->pc,"pubsub");
                    /* Now we subscribe to the Sentinels "Hello" channel. */

                    // 发送 SUBSCRIBE __sentinel__:hello 命令，订阅频道
                    retval = redisAsyncCommand(link->pc,
                                               sentinelReceiveHelloMessages, ri, "%s %s",
                                               sentinelInstanceMapCommand(ri,"SUBSCRIBE"),
                                               SENTINEL_HELLO_CHANNEL);

                    // 订阅出错，断开连接
                    if (retval != C_OK) {
                        /* If we can't subscribe, the Pub/Sub connection is useless
                         * and we can simply disconnect it and try again. */
                        instanceLinkCloseConnection(link,link->pc);
                        return;
                    }
                }
            }
            /* Clear the disconnected status only if we have both the connections
             * (or just the commands connection if this is a sentinel instance). */

            // 如果实例是主服务器或者从服务器，那么当 cc 和 pc 两个连接都创建成功时，关闭 DISCONNECTED 标识
            // 如果实例是 Sentinel ，那么当 cc 连接创建成功时，关闭 DISCONNECTED 标识
            if (link->cc && (ri->flags & SRI_SENTINEL || link->pc))
                link->disconnected = 0;
        }

        /* ======================== Redis instances pinging  ======================== */

        /* Return true if master looks "sane", that is:
         * 如果主服务器看上去是合理（sane），那么返回真。判断是否合理的条件如下：
         *
         * 1) It is actually a master in the current configuration.
         *    它在当前配置中的角色为主服务器
         * 2) It reports itself as a master.
         *    它报告自己是一个主服务器
         * 3) It is not SDOWN or ODOWN.
         *    这个主服务器不处于 SDOWN 或者 ODOWN 状态
         * 4) We obtained last INFO no more than two times the INFO period time ago.
         *    主服务器最近一次刷新 INFO 信息距离现在不超过 SENTINEL_INFO_PERIOD 的两倍时间
         */
        int sentinelMasterLooksSane(sentinelRedisInstance *master) {
            return
            master->flags & SRI_MASTER &&
            master->role_reported == SRI_MASTER &&
            (master->flags & (SRI_S_DOWN|SRI_O_DOWN)) == 0 &&
            (mstime() - master->info_refresh) < SENTINEL_INFO_PERIOD*2;
        }

        /* Process the INFO output from masters. */
        // 从主服务器或者从服务器所返回的 INFO 命令的回复中分析相关信息
        // （上面的英文注释错了，这个函数不仅处理主服务器的 INFO 回复，还处理从服务器的 INFO 回复）
        void sentinelRefreshInstanceInfo(sentinelRedisInstance *ri, const char *info) {
            sds *lines;
            int numlines, j;
            int role = 0;

            /* cache full INFO output for instance */
            sdsfree(ri->info);
            ri->info = sdsnew(info);

            /* The following fields must be reset to a given value in the case they
             * are not found at all in the INFO output. */
            // 将该变量重置为 0 ，避免 INFO 回复中无该值的情况
            ri->master_link_down_time = 0;

            /* Process line by line. */
            // 对 INFO 命令的回复进行逐行分析
            lines = sdssplitlen(info,strlen(info),"\r\n",2,&numlines);
            for (j = 0; j < numlines; j++) {
                sentinelRedisInstance *slave;
                sds l = lines[j];

                /* run_id:<40 hex chars>*/
                // 读取并分析 runid
                if (sdslen(l) >= 47 && !memcmp(l,"run_id:",7)) {

                    // 新设置 runid
                    if (ri->runid == NULL) {
                        ri->runid = sdsnewlen(l+7,40);
                    } else {
                        // RUNID 不同，说明服务器已重启
                        if (strncmp(ri->runid,l+7,40) != 0) {
                            sentinelEvent(LL_NOTICE,"+reboot",ri,"%@");
                            // 释放旧 ID ，设置新 ID
                            sdsfree(ri->runid);
                            ri->runid = sdsnewlen(l+7,40);
                        }
                    }
                }

                // 读取从服务器的 ip 和端口号
                /* old versions: slave0:<ip>,<port>,<state>
                 * new versions: slave0:ip=127.0.0.1,port=9999,... */
                if ((ri->flags & SRI_MASTER) &&
                sdslen(l) >= 7 &&
                !memcmp(l,"slave",5) && isdigit(l[5]))
                {
                    char *ip, *port, *end;

                    if (strstr(l,"ip=") == NULL) {
                        /* Old format. */
                        ip = strchr(l,':'); if (!ip) continue;
                        ip++; /* Now ip points to start of ip address. */
                        port = strchr(ip,','); if (!port) continue;
                        *port = '\0'; /* nul term for easy access. */
                        port++; /* Now port points to start of port number. */
                        end = strchr(port,','); if (!end) continue;
                        *end = '\0'; /* nul term for easy access. */
                    } else {
                        /* New format. */
                        ip = strstr(l,"ip="); if (!ip) continue;
                        ip += 3; /* Now ip points to start of ip address. */
                        port = strstr(l,"port="); if (!port) continue;
                        port += 5; /* Now port points to start of port number. */
                        /* Nul term both fields for easy access. */
                        end = strchr(ip,','); if (end) *end = '\0';
                        end = strchr(port,','); if (end) *end = '\0';
                    }

                    /* Check if we already have this slave into our table,
                     * otherwise add it. */
                    // 如果发现有新的从服务器出现，那么为它添加实例
                    if (sentinelRedisInstanceLookupSlave(ri,ip,atoi(port)) == NULL) {
                        if ((slave = createSentinelRedisInstance(NULL,SRI_SLAVE,ip,
                                                                 atoi(port), ri->quorum, ri)) != NULL)
                        {
                            sentinelEvent(LL_NOTICE,"+slave",slave,"%@");
                            sentinelFlushConfig();
                        }
                    }
                }

                /* master_link_down_since_seconds:<seconds> */
                // 读取主从服务器的断线时长
                // 这个只会在实例是从服务器，并且主从连接断开的情况下出现
                if (sdslen(l) >= 32 &&
                !memcmp(l,"master_link_down_since_seconds",30))
                {
                    ri->master_link_down_time = strtoll(l+31,NULL,10)*1000;
                }

                /* role:<role> */
                // 读取实例的角色
                if (sdslen(l) >= 11 && !memcmp(l,"role:master",11)) role = SRI_MASTER;
                else if (sdslen(l) >= 10 && !memcmp(l,"role:slave",10)) role = SRI_SLAVE;

                // 处理从服务器
                if (role == SRI_SLAVE) {

                    /* master_host:<host> */
                    // 读入主服务器的 IP
                    if (sdslen(l) >= 12 && !memcmp(l,"master_host:",12)) {
                        if (ri->slave_master_host == NULL ||
                        strcasecmp(l+12,ri->slave_master_host))
                        {
                            sdsfree(ri->slave_master_host);
                            ri->slave_master_host = sdsnew(l+12);
                            ri->slave_conf_change_time = mstime();
                        }
                    }

                    /* master_port:<port> */
                    // 读入主服务器的端口号
                    if (sdslen(l) >= 12 && !memcmp(l,"master_port:",12)) {
                        int slave_master_port = atoi(l+12);

                        if (ri->slave_master_port != slave_master_port) {
                            ri->slave_master_port = slave_master_port;
                            ri->slave_conf_change_time = mstime();
                        }
                    }

                    /* master_link_status:<status> */
                    // 读入主服务器的状态
                    if (sdslen(l) >= 19 && !memcmp(l,"master_link_status:",19)) {
                        ri->slave_master_link_status =
                                (strcasecmp(l+19,"up") == 0) ?
                                SENTINEL_MASTER_LINK_STATUS_UP :
                        SENTINEL_MASTER_LINK_STATUS_DOWN;
                    }

                    /* slave_priority:<priority> */
                    // 读入从服务器的优先级
                    if (sdslen(l) >= 15 && !memcmp(l,"slave_priority:",15))
                        ri->slave_priority = atoi(l+15);

                    /* slave_repl_offset:<offset> */
                    // 读入从服务器的复制偏移量
                    if (sdslen(l) >= 18 && !memcmp(l,"slave_repl_offset:",18))
                        ri->slave_repl_offset = strtoull(l+18,NULL,10);

                    /* replica_announced:<announcement> */
                    if (sdslen(l) >= 18 && !memcmp(l,"replica_announced:",18))
                        ri->replica_announced = atoi(l+18);
                }
            }
            // 更新刷新 INFO 命令回复的时间
            ri->info_refresh = mstime();
            sdsfreesplitres(lines,numlines);

            /* ---------------------------- Acting half -----------------------------
             * Some things will not happen if sentinel.tilt is true, but some will
             * still be processed.
             *
             * 如果 sentinel 进入了 TILT 模式，那么可能只有一部分动作会被执行
             */

            /* Remember when the role changed. */
            if (role != ri->role_reported) {
                ri->role_reported_time = mstime();
                ri->role_reported = role;
                if (role == SRI_SLAVE) ri->slave_conf_change_time = mstime();
                /* Log the event with +role-change if the new role is coherent or
                 * with -role-change if there is a mismatch with the current config. */
                sentinelEvent(LL_VERBOSE,
                              ((ri->flags & (SRI_MASTER|SRI_SLAVE)) == role) ?
                              "+role-change" : "-role-change",
                              ri, "%@ new reported role is %s",
                              role == SRI_MASTER ? "master" : "slave");
            }

            /* None of the following conditions are processed when in tilt mode, so
             * return asap. */
            // 如果 Sentinel 正处于 TILT 模式，那么它不能执行以下的语句。
            if (sentinel.tilt) return;

            /* Handle master -> slave role switch. */
            // 实例被 Sentinel 标识为主服务器，但根据 INFO 命令的回复
            // 这个实例的身份为从服务器
            if ((ri->flags & SRI_MASTER) && role == SRI_SLAVE) {
                /* Nothing to do, but masters claiming to be slaves are
                 * considered to be unreachable by Sentinel, so eventually
                 * a failover will be triggered. */
                // 如果一个主服务器变为从服务器，那么 Sentinel 将这个主服务器看作是不可用的
            }

            /* Handle slave -> master role switch. */
            // 处理从服务器转变为主服务器的情况
            if ((ri->flags & SRI_SLAVE) && role == SRI_MASTER) {
                /* If this is a promoted slave we can change state to the
                 * failover state machine. */

                // 如果这是被选中升级为新主服务器的从服务器
                // 那么更新相关的故障转移属性
                if ((ri->flags & SRI_PROMOTED) &&
                (ri->master->flags & SRI_FAILOVER_IN_PROGRESS) &&
                (ri->master->failover_state ==
                SENTINEL_FAILOVER_STATE_WAIT_PROMOTION))
                {
                    /* Now that we are sure the slave was reconfigured as a master
                     * set the master configuration epoch to the epoch we won the
                     * election to perform this failover. This will force the other
                     * Sentinels to update their config (assuming there is not
                     * a newer one already available). */
                    // 这是一个被 Sentinel 发送 SLAVEOF no one 之后由从服务器变为主服务器的实例
                    // 将这个新主服务器的配置纪元设置为 Sentinel 赢得领头选举的纪元
                    // 这一操作会强制其他 Sentinel 更新它们自己的配置
                    // （假设没有一个更新的纪元存在的话）
                    // 更新从服务器的主服务器（已下线）的配置纪元
                    ri->master->config_epoch = ri->master->failover_epoch;
                    // 设置从服务器的主服务器（已下线）的故障转移状态
                    // 这个状态会让从服务器开始同步新的主服务器
                    ri->master->failover_state = SENTINEL_FAILOVER_STATE_RECONF_SLAVES;
                    // 更新从服务器的主服务器（已下线）的故障转移状态变更时间
                    ri->master->failover_state_change_time = mstime();
                    // 将当前 Sentinel 状态保存到配置文件里面
                    sentinelFlushConfig();
                    // 发送事件
                    sentinelEvent(LL_WARNING,"+promoted-slave",ri,"%@");
                    if (sentinel.simfailure_flags &
                    SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION)
                        sentinelSimFailureCrash();
                    sentinelEvent(LL_WARNING,"+failover-state-reconf-slaves",
                                  ri->master,"%@");
                    // 执行脚本
                    sentinelCallClientReconfScript(ri->master,SENTINEL_LEADER,
                                                   "start",ri->master->addr,ri->addr);
                    sentinelForceHelloUpdateForMaster(ri->master);


                    // 这个实例由从服务器变为了主服务器，并且没有进入 TILT 模式
                    // （可能是因为重启造成的，或者之前的下线主服务器重新上线了）
                } else {
                    /* A slave turned into a master. We want to force our view and
                     * reconfigure as slave. Wait some time after the change before
                     * going forward, to receive new configs if any. */
                    // 如果一个从服务器变为了主服务器，那么我们会考虑将它变回一个从服务器
                    // 将 PUBLISH 命令的发送时间乘以 4 ，给于一定缓冲时间
                    mstime_t wait_time = SENTINEL_PUBLISH_PERIOD*4;

                    // 如果这个实例的主服务器运作正常
                    // 并且实例在一段时间内没有进入过 SDOWN 状态或者 ODOWN 状态
                    // 并且实例报告它是主服务器的时间已经超过 wait_time
                    if (!(ri->flags & SRI_PROMOTED) &&
                    sentinelMasterLooksSane(ri->master) &&
                    sentinelRedisInstanceNoDownFor(ri,wait_time) &&
                    mstime() - ri->role_reported_time > wait_time)
                    {
                        // 重新将实例设置为从服务器
                        int retval = sentinelSendSlaveOf(ri,ri->master->addr);
                        // 发送事件
                        if (retval == C_OK)
                            sentinelEvent(LL_NOTICE,"+convert-to-slave",ri,"%@");
                    }
                }
            }

            /* Handle slaves replicating to a different master address. */
            // 让从服务器重新复制回正确的主服务器
            if ((ri->flags & SRI_SLAVE) &&
            role == SRI_SLAVE &&
            // 从服务器现在的主服务器地址和 Sentinel 保存的信息不一致
            (ri->slave_master_port != ri->master->addr->port ||
            !sentinelAddrEqualsHostname(ri->master->addr, ri->slave_master_host)))
            {
                mstime_t wait_time = ri->master->failover_timeout;

                /* Make sure the master is sane before reconfiguring this instance
                 * into a slave. */
                // 1) 检查实例的主服务器状态是否正常
                // 2) 检查实例在给定时间内是否进入过 SDOWN 或者 ODOWN 状态
                // 3) 检查实例身份变更的时长是否已经超过了指定时长
                // 如果是的话，执行代码。。。
                if (sentinelMasterLooksSane(ri->master) &&
                sentinelRedisInstanceNoDownFor(ri,wait_time) &&
                mstime() - ri->slave_conf_change_time > wait_time)
                {
                    // 重新将实例指向原本的主服务器
                    int retval = sentinelSendSlaveOf(ri,ri->master->addr);
                    if (retval == C_OK)
                        sentinelEvent(LL_NOTICE,"+fix-slave-config",ri,"%@");
                }
            }

            /* Detect if the slave that is in the process of being reconfigured
             * changed state. */
            // Sentinel 监视的实例为从服务器，并且已经向它发送 SLAVEOF 命令
            if ((ri->flags & SRI_SLAVE) && role == SRI_SLAVE &&
            (ri->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)))
            {
                /* SRI_RECONF_SENT -> SRI_RECONF_INPROG. */
                // 将 SENT 状态改为 INPROG 状态，表示同步正在进行
                if ((ri->flags & SRI_RECONF_SENT) &&
                ri->slave_master_host &&
                sentinelAddrEqualsHostname(ri->master->promoted_slave->addr,
                                           ri->slave_master_host) &&
                                           ri->slave_master_port == ri->master->promoted_slave->addr->port)
                {
                    ri->flags &= ~SRI_RECONF_SENT;
                    ri->flags |= SRI_RECONF_INPROG;
                    sentinelEvent(LL_NOTICE,"+slave-reconf-inprog",ri,"%@");
                }

                /* SRI_RECONF_INPROG -> SRI_RECONF_DONE */
                // 将 INPROG 状态改为 DONE 状态，表示同步已完成
                if ((ri->flags & SRI_RECONF_INPROG) &&
                ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP)
                {
                    ri->flags &= ~SRI_RECONF_INPROG;
                    ri->flags |= SRI_RECONF_DONE;
                    sentinelEvent(LL_NOTICE,"+slave-reconf-done",ri,"%@");
                }
            }
        }

        // 处理 INFO 命令的回复
        void sentinelInfoReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
            sentinelRedisInstance *ri = privdata;
            instanceLink *link = c->data;
            redisReply *r;

            if (!reply || !link) return;
            link->pending_commands--;
            r = reply;

            if (r->type == REDIS_REPLY_STRING)
                sentinelRefreshInstanceInfo(ri,r->str);
        }

        /* Just discard the reply. We use this when we are not monitoring the return
         * value of the command but its effects directly. */
        // 这个回调函数用于处理不需要检查回复的命令（只使用命令的副作用）
        void sentinelDiscardReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
            instanceLink *link = c->data;
            UNUSED(reply);
            UNUSED(privdata);

            if (link) link->pending_commands--;
        }

        // 处理 PING 命令的回复
        void sentinelPingReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
            sentinelRedisInstance *ri = privdata;
            instanceLink *link = c->data;
            redisReply *r;

            if (!reply || !link) return;
            link->pending_commands--;
            r = reply;

            if (r->type == REDIS_REPLY_STATUS ||
            r->type == REDIS_REPLY_ERROR) {
                /* Update the "instance available" field only if this is an
                 * acceptable reply. */
                // 只在实例返回 acceptable 回复时更新 last_avail_time
                if (strncmp(r->str,"PONG",4) == 0 ||
                strncmp(r->str,"LOADING",7) == 0 ||
                strncmp(r->str,"MASTERDOWN",10) == 0)
                {
                    // 实例运作正常
                    link->last_avail_time = mstime();
                    link->act_ping_time = 0; /* Flag the pong as received. */
                } else {
                    // 实例运作不正常

                    /* Send a SCRIPT KILL command if the instance appears to be
                     * down because of a busy script. */
                    // 如果服务器因为执行脚本而进入 BUSY 状态，
                    // 那么尝试通过发送 SCRIPT KILL 来恢复服务器
                    if (strncmp(r->str,"BUSY",4) == 0 &&
                    (ri->flags & SRI_S_DOWN) &&
                    !(ri->flags & SRI_SCRIPT_KILL_SENT))
                    {
                        if (redisAsyncCommand(ri->link->cc,
                                              sentinelDiscardReplyCallback, ri,
                                              "%s KILL",
                                              sentinelInstanceMapCommand(ri,"SCRIPT")) == C_OK)
                        {
                            ri->link->pending_commands++;
                        }
                        ri->flags |= SRI_SCRIPT_KILL_SENT;
                    }
                }
            }

            // 更新实例最后一次回复 PING 命令的时间
            link->last_pong_time = mstime();
        }

        /* This is called when we get the reply about the PUBLISH command we send
         * to the master to advertise this sentinel. */
        // 处理 PUBLISH 命令的回复
        void sentinelPublishReplyCallback(redisAsyncContext *c, void *reply, void *privdata) {
            sentinelRedisInstance *ri = privdata;
            instanceLink *link = c->data;
            redisReply *r;

            if (!reply || !link) return;
            link->pending_commands--;
            r = reply;

            /* Only update pub_time if we actually published our message. Otherwise
             * we'll retry again in 100 milliseconds. */
            // 如果命令发送成功，那么更新 last_pub_time
            if (r->type != REDIS_REPLY_ERROR)
                ri->last_pub_time = mstime();
        }

        /* Process a hello message received via Pub/Sub in master or slave instance,
         * or sent directly to this sentinel via the (fake) PUBLISH command of Sentinel.
         *
         * 处理从 Pub/Sub 连接得来的，来自主服务器或者从服务器的 hello 消息。
         * hello 消息也可能是另一个 Sentinel 通过 PUBLISH 命令发送过来的。
         *
         * If the master name specified in the message is not known, the message is
         * discareded.
         *
         * 如果消息里面指定的主服务器的名字是未知的，那么这条消息将被丢弃。
         */
        void sentinelProcessHelloMessage(char *hello, int hello_len) {
            /* Format is composed of 8 tokens:
             * 0=ip,1=port,2=runid,3=current_epoch,4=master_name,
             * 5=master_ip,6=master_port,7=master_config_epoch. */
            int numtokens, port, removed, master_port;
            uint64_t current_epoch, master_config_epoch;
            char **token = sdssplitlen(hello, hello_len, ",", 1, &numtokens);
            sentinelRedisInstance *si, *master;

            if (numtokens == 8) {
                /* Obtain a reference to the master this hello message is about */
                // 获取主服务器的名字，并丢弃和未知主服务器相关的消息。
                master = sentinelGetMasterByName(token[4]);
                if (!master) goto cleanup; /* Unknown master, skip the message. */

                /* First, try to see if we already have this sentinel. */
                // 看这个 Sentinel 是否已经认识发送消息的 Sentinel
                port = atoi(token[1]);
                master_port = atoi(token[6]);
                si = getSentinelRedisInstanceByAddrAndRunID(
                        master->sentinels,token[0],port,token[2]);
                current_epoch = strtoull(token[3],NULL,10);
                master_config_epoch = strtoull(token[7],NULL,10);

                if (!si) {

                    // 这个 Sentinel 不认识发送消息的 Sentinel
                    // 将对方加入到 Sentinel 列表中
                    /* If not, remove all the sentinels that have the same runid
                     * because there was an address change, and add the same Sentinel
                     * with the new address back. */
                    removed = removeMatchingSentinelFromMaster(master,token[2]);
                    if (removed) {
                        sentinelEvent(LL_NOTICE,"+sentinel-address-switch",master,
                                      "%@ ip %s port %d for %s", token[0],port,token[2]);
                    } else {
                        /* Check if there is another Sentinel with the same address this
                         * new one is reporting. What we do if this happens is to set its
                         * port to 0, to signal the address is invalid. We'll update it
                         * later if we get an HELLO message. */
                        sentinelRedisInstance *other =
                                getSentinelRedisInstanceByAddrAndRunID(
                                        master->sentinels, token[0],port,NULL);
                        if (other) {
                            sentinelEvent(LL_NOTICE,"+sentinel-invalid-addr",other,"%@");
                            other->addr->port = 0; /* It means: invalid address. */
                            sentinelUpdateSentinelAddressInAllMasters(other);
                        }
                    }

                    /* Add the new sentinel. */
                    si = createSentinelRedisInstance(token[2],SRI_SENTINEL,
                                                     token[0],port,master->quorum,master);

                    if (si) {
                        if (!removed) sentinelEvent(LL_NOTICE,"+sentinel",si,"%@");
                        /* The runid is NULL after a new instance creation and
                         * for Sentinels we don't have a later chance to fill it,
                         * so do it now. */
                        si->runid = sdsnew(token[2]);
                        sentinelTryConnectionSharing(si);
                        if (removed) sentinelUpdateSentinelAddressInAllMasters(si);
                        sentinelFlushConfig();
                    }
                }

                /* Update local current_epoch if received current_epoch is greater.*/
                // 如果消息中记录的纪元比 Sentinel 当前的纪元要高，那么更新纪元
                if (current_epoch > sentinel.current_epoch) {
                    sentinel.current_epoch = current_epoch;
                    sentinelFlushConfig();
                    sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
                                  (unsigned long long) sentinel.current_epoch);
                }

                /* Update master info if received configuration is newer. */
                // 如果消息中记录的配置信息更新，那么对主服务器的信息进行更新
                if (si && master->config_epoch < master_config_epoch) {
                    master->config_epoch = master_config_epoch;
                    if (master_port != master->addr->port ||
                    !sentinelAddrEqualsHostname(master->addr, token[5]))
                    {
                        sentinelAddr *old_addr;

                        sentinelEvent(LL_WARNING,"+config-update-from",si,"%@");
                        sentinelEvent(LL_WARNING,"+switch-master",
                                      master,"%s %s %d %s %d",
                                      master->name,
                                      announceSentinelAddr(master->addr), master->addr->port,
                                      token[5], master_port);

                        old_addr = dupSentinelAddr(master->addr);
                        sentinelResetMasterAndChangeAddress(master, token[5], master_port);
                        sentinelCallClientReconfScript(master,
                                                       SENTINEL_OBSERVER,"start",
                                                       old_addr,master->addr);
                        releaseSentinelAddr(old_addr);
                    }
                }

                /* Update the state of the Sentinel. */
                // 更新我方 Sentinel 记录的对方 Sentinel 的信息。
                if (si) si->last_hello_time = mstime();
            }

            cleanup:
            sdsfreesplitres(token,numtokens);
        }


        /* This is our Pub/Sub callback for the Hello channel. It's useful in order
         * to discover other sentinels attached at the same master. */
        // 此回调函数用于处理 Hello 频道的返回值，它可以发现其他正在订阅同一主服务器的 Sentinel
        void sentinelReceiveHelloMessages(redisAsyncContext *c, void *reply, void *privdata) {
            sentinelRedisInstance *ri = privdata;
            redisReply *r;
            UNUSED(c);

            if (!reply || !ri) return;
            r = reply;

            /* Update the last activity in the pubsub channel. Note that since we
             * receive our messages as well this timestamp can be used to detect
             * if the link is probably disconnected even if it seems otherwise. */
            // 更新最后一次接收频道命令的时间
            ri->link->pc_last_activity = mstime();

            /* Sanity check in the reply we expect, so that the code that follows
             * can avoid to check for details. */
            // 只处理频道发来的信息，不处理订阅时和退订时产生的信息
            if (r->type != REDIS_REPLY_ARRAY ||
            r->elements != 3 ||
            r->element[0]->type != REDIS_REPLY_STRING ||
            r->element[1]->type != REDIS_REPLY_STRING ||
            r->element[2]->type != REDIS_REPLY_STRING ||
            strcmp(r->element[0]->str,"message") != 0) return;

            /* We are not interested in meeting ourselves */
            // 只处理非自己发送的信息
            if (strstr(r->element[2]->str,sentinel.myid) != NULL) return;

            sentinelProcessHelloMessage(r->element[2]->str, r->element[2]->len);
        }

        /* Send a "Hello" message via Pub/Sub to the specified 'ri' Redis
         * instance in order to broadcast the current configuration for this
         * master, and to advertise the existence of this Sentinel at the same time.
         *
         * 向给定 ri 实例的频道发送信息，
         * 从而传播关于给定主服务器的配置，
         * 并向其他 Sentinel 宣告本 Sentinel 的存在。
         *
         * The message has the following format:
         *
         * 发送信息的格式如下：
         *
         * sentinel_ip,sentinel_port,sentinel_runid,current_epoch,
         * master_name,master_ip,master_port,master_config_epoch.
         *
         * Sentinel IP,Sentinel 端口号,Sentinel 的运行 ID,Sentinel 当前的纪元,
         * 主服务器的名称,主服务器的 IP,主服务器的端口号,主服务器的配置纪元.
         *
         * Returns REDIS_OK if the PUBLISH was queued correctly, otherwise
         * REDIS_ERR is returned.
         *
         * PUBLISH 命令成功入队时返回 REDIS_OK ，
         * 否则返回 REDIS_ERR 。
         */
        int sentinelSendHello(sentinelRedisInstance *ri) {
            char ip[NET_IP_STR_LEN];
            char payload[NET_IP_STR_LEN+1024];
            int retval;
            char *announce_ip;
            int announce_port;

            // 如果实例是主服务器，那么使用此实例的信息
            // 如果实例是从服务器，那么使用这个从服务器的主服务器的信息
            sentinelRedisInstance *master = (ri->flags & SRI_MASTER) ? ri : ri->master;

            // 获取地址信息

            sentinelAddr *master_addr = sentinelGetCurrentMasterAddress(master);

            if (ri->link->disconnected) return C_ERR;

            /* Use the specified announce address if specified, otherwise try to
             * obtain our own IP address. */
            // 获取实例自身的地址
            if (sentinel.announce_ip) {
                announce_ip = sentinel.announce_ip;
            } else {
                if (anetFdToString(ri->link->cc->c.fd,ip,sizeof(ip),NULL,FD_TO_SOCK_NAME) == -1)
                    return C_ERR;
                announce_ip = ip;
            }
            if (sentinel.announce_port) announce_port = sentinel.announce_port;
            else if (server.tls_replication && server.tls_port) announce_port = server.tls_port;
            else announce_port = server.port;

            /* Format and send the Hello message. */
            // 格式化信息
            snprintf(payload,sizeof(payload),
                     "%s,%d,%s,%llu," /* Info about this sentinel. */
                     "%s,%s,%d,%llu", /* Info about current master. */
                     announce_ip, announce_port, sentinel.myid,
                     (unsigned long long) sentinel.current_epoch,
                     /* --- */
                     master->name,announceSentinelAddr(master_addr),master_addr->port,
                     (unsigned long long) master->config_epoch);

            // 发送信息
            retval = redisAsyncCommand(ri->link->cc,
                                       sentinelPublishReplyCallback, ri, "%s %s %s",
                                       sentinelInstanceMapCommand(ri,"PUBLISH"),
                                       SENTINEL_HELLO_CHANNEL,payload);
            if (retval != C_OK) return C_ERR;
            ri->link->pending_commands++;
            return C_OK;
        }

        /* Reset last_pub_time in all the instances in the specified dictionary
         * in order to force the delivery of a Hello update ASAP. */
        void sentinelForceHelloUpdateDictOfRedisInstances(dict *instances) {
            dictIterator *di;
            dictEntry *de;

            di = dictGetSafeIterator(instances);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);
                if (ri->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
                    ri->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
            }
            dictReleaseIterator(di);
        }

        /* This function forces the delivery of a "Hello" message (see
         * sentinelSendHello() top comment for further information) to all the Redis
         * and Sentinel instances related to the specified 'master'.
         *
         * It is technically not needed since we send an update to every instance
         * with a period of SENTINEL_PUBLISH_PERIOD milliseconds, however when a
         * Sentinel upgrades a configuration it is a good idea to deliver an update
         * to the other Sentinels ASAP. */
        int sentinelForceHelloUpdateForMaster(sentinelRedisInstance *master) {
            if (!(master->flags & SRI_MASTER)) return C_ERR;
            if (master->last_pub_time >= (SENTINEL_PUBLISH_PERIOD+1))
                master->last_pub_time -= (SENTINEL_PUBLISH_PERIOD+1);
            sentinelForceHelloUpdateDictOfRedisInstances(master->sentinels);
            sentinelForceHelloUpdateDictOfRedisInstances(master->slaves);
            return C_OK;
        }

        /* Send a PING to the specified instance and refresh the act_ping_time
         * if it is zero (that is, if we received a pong for the previous ping).
         *
         * On error zero is returned, and we can't consider the PING command
         * queued in the connection. */
        // 向指定的 Sentinel 发送 PING 命令。
        int sentinelSendPing(sentinelRedisInstance *ri) {
            int retval = redisAsyncCommand(ri->link->cc,
                                           sentinelPingReplyCallback, ri, "%s",
                                           sentinelInstanceMapCommand(ri,"PING"));
            if (retval == C_OK) {
                ri->link->pending_commands++;
                ri->link->last_ping_time = mstime();
                /* We update the active ping time only if we received the pong for
                 * the previous ping, otherwise we are technically waiting since the
                 * first ping that did not receive a reply. */
                if (ri->link->act_ping_time == 0)
                    ri->link->act_ping_time = ri->link->last_ping_time;
                return 1;
            } else {
                return 0;
            }
        }

        // 根据时间和实例类型等情况，向实例发送命令，比如 INFO 、PING 和 PUBLISH
        // 虽然函数的名字包含 Ping ，但命令并不只发送 PING 命令
        /* Send periodic PING, INFO, and PUBLISH to the Hello channel to
         * the specified master or slave instance. */
        void sentinelSendPeriodicCommands(sentinelRedisInstance *ri) {
            mstime_t now = mstime();
            mstime_t info_period, ping_period;
            int retval;

            /* Return ASAP if we have already a PING or INFO already pending, or
             * in the case the instance is not properly connected. */

            // 函数不能在网络连接未创建时执行
            if (ri->link->disconnected) return;

            /* For INFO, PING, PUBLISH that are not critical commands to send we
             * also have a limit of SENTINEL_MAX_PENDING_COMMANDS. We don't
             * want to use a lot of memory just because a link is not working
             * properly (note that anyway there is a redundant protection about this,
             * that is, the link will be disconnected and reconnected if a long
             * timeout condition is detected. */

            // 为了避免 sentinel 在实例处于不正常状态时，发送过多命令
            // sentinel 只在待发送命令的数量未超过 SENTINEL_MAX_PENDING_COMMANDS 常量时
            // 才进行命令发送
            if (ri->link->pending_commands >=
            SENTINEL_MAX_PENDING_COMMANDS * ri->link->refcount) return;

            /* If this is a slave of a master in O_DOWN condition we start sending
             * it INFO every second, instead of the usual SENTINEL_INFO_PERIOD
             * period. In this state we want to closely monitor slaves in case they
             * are turned into masters by another Sentinel, or by the sysadmin.
             *
             * Similarly we monitor the INFO output more often if the slave reports
             * to be disconnected from the master, so that we can have a fresh
             * disconnection time figure. */


            // 对于从服务器来说， sentinel 默认每 SENTINEL_INFO_PERIOD 秒向它发送一次 INFO 命令
            // 但是，当从服务器的主服务器处于 SDOWN 状态，或者正在执行故障转移时
            // 为了更快速地捕捉从服务器的变动， sentinel 会将发送 INFO 命令的频率该为每秒一次
            if ((ri->flags & SRI_SLAVE) &&
            ((ri->master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS)) ||
            (ri->master_link_down_time != 0)))
            {
                info_period = 1000;
            } else {
                info_period = SENTINEL_INFO_PERIOD;
            }

            /* We ping instances every time the last received pong is older than
             * the configured 'down-after-milliseconds' time, but every second
             * anyway if 'down-after-milliseconds' is greater than 1 second. */
            ping_period = ri->down_after_period;
            if (ping_period > SENTINEL_PING_PERIOD) ping_period = SENTINEL_PING_PERIOD;
            // 实例不是 Sentinel （主服务器或者从服务器）
            // 并且以下条件的其中一个成立：
            // 1）SENTINEL 未收到过这个服务器的 INFO 命令回复
            // 2）距离上一次该实例回复 INFO 命令已经超过 info_period 间隔
            // 那么向实例发送 INFO 命令
            /* Send INFO to masters and slaves, not sentinels. */
            if ((ri->flags & SRI_SENTINEL) == 0 &&
            (ri->info_refresh == 0 ||
            (now - ri->info_refresh) > info_period))
            {
                retval = redisAsyncCommand(ri->link->cc,
                                           sentinelInfoReplyCallback, ri, "%s",
                                           sentinelInstanceMapCommand(ri,"INFO"));
                if (retval == C_OK) ri->link->pending_commands++;
            }

            /* Send PING to all the three kinds of instances. */
            if ((now - ri->link->last_pong_time) > ping_period &&
            (now - ri->link->last_ping_time) > ping_period/2) {
                sentinelSendPing(ri);
            }

            /* PUBLISH hello messages to all the three kinds of instances. */
            if ((now - ri->last_pub_time) > SENTINEL_PUBLISH_PERIOD) {
                sentinelSendHello(ri);
            }
        }

        /* =========================== SENTINEL command ============================= */

        /* SENTINEL CONFIG SET <option> */

        void sentinelConfigSetCommand(client *c) {
            robj *o = c->argv[3];
            robj *val = c->argv[4];
            long long numval;
            int drop_conns = 0;

            if (!strcasecmp(o->ptr, "resolve-hostnames")) {
                if ((numval = yesnotoi(val->ptr)) == -1) goto badfmt;
                sentinel.resolve_hostnames = numval;
            } else if (!strcasecmp(o->ptr, "announce-hostnames")) {
                if ((numval = yesnotoi(val->ptr)) == -1) goto badfmt;
                sentinel.announce_hostnames = numval;
            } else if (!strcasecmp(o->ptr, "announce-ip")) {
                if (sentinel.announce_ip) sdsfree(sentinel.announce_ip);
                sentinel.announce_ip = sdsnew(val->ptr);
            } else if (!strcasecmp(o->ptr, "announce-port")) {
                if (getLongLongFromObject(val, &numval) == C_ERR ||
                numval < 0 || numval > 65535)
                    goto badfmt;
                sentinel.announce_port = numval;
            } else if (!strcasecmp(o->ptr, "sentinel-user")) {
                sdsfree(sentinel.sentinel_auth_user);
                sentinel.sentinel_auth_user = sdslen(val->ptr) == 0 ?
                        NULL : sdsdup(val->ptr);
                drop_conns = 1;
            } else if (!strcasecmp(o->ptr, "sentinel-pass")) {
                sdsfree(sentinel.sentinel_auth_pass);
                sentinel.sentinel_auth_pass = sdslen(val->ptr) == 0 ?
                        NULL : sdsdup(val->ptr);
                drop_conns = 1;
            } else {
                addReplyErrorFormat(c, "Invalid argument '%s' to SENTINEL CONFIG SET",
                                    (char *) o->ptr);
                return;
            }

            sentinelFlushConfig();
            addReply(c, shared.ok);

            /* Drop Sentinel connections to initiate a reconnect if needed. */
            if (drop_conns)
                sentinelDropConnections();

            return;

            badfmt:
            addReplyErrorFormat(c, "Invalid value '%s' to SENTINEL CONFIG SET '%s'",
                                (char *) val->ptr, (char *) o->ptr);
        }

        /* SENTINEL CONFIG GET <option> */
        void sentinelConfigGetCommand(client *c) {
            robj *o = c->argv[3];
            const char *pattern = o->ptr;
            void *replylen = addReplyDeferredLen(c);
            int matches = 0;

            if (stringmatch(pattern,"resolve-hostnames",1)) {
                addReplyBulkCString(c,"resolve-hostnames");
                addReplyBulkCString(c,sentinel.resolve_hostnames ? "yes" : "no");
                matches++;
            }

            if (stringmatch(pattern, "announce-hostnames", 1)) {
                addReplyBulkCString(c,"announce-hostnames");
                addReplyBulkCString(c,sentinel.announce_hostnames ? "yes" : "no");
                matches++;
            }

            if (stringmatch(pattern, "announce-ip", 1)) {
                addReplyBulkCString(c,"announce-ip");
                addReplyBulkCString(c,sentinel.announce_ip ? sentinel.announce_ip : "");
                matches++;
            }

            if (stringmatch(pattern, "announce-port", 1)) {
                addReplyBulkCString(c, "announce-port");
                addReplyBulkLongLong(c, sentinel.announce_port);
                matches++;
            }

            if (stringmatch(pattern, "sentinel-user", 1)) {
                addReplyBulkCString(c, "sentinel-user");
                addReplyBulkCString(c, sentinel.sentinel_auth_user ? sentinel.sentinel_auth_user : "");
                matches++;
            }

            if (stringmatch(pattern, "sentinel-pass", 1)) {
                addReplyBulkCString(c, "sentinel-pass");
                addReplyBulkCString(c, sentinel.sentinel_auth_pass ? sentinel.sentinel_auth_pass : "");
                matches++;
            }

            setDeferredMapLen(c, replylen, matches);
        }


        // 返回字符串表示的故障转移状态
        const char *sentinelFailoverStateStr(int state) {
            switch(state) {
                case SENTINEL_FAILOVER_STATE_NONE: return "none";
                case SENTINEL_FAILOVER_STATE_WAIT_START: return "wait_start";
                case SENTINEL_FAILOVER_STATE_SELECT_SLAVE: return "select_slave";
                case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE: return "send_slaveof_noone";
                case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION: return "wait_promotion";
                case SENTINEL_FAILOVER_STATE_RECONF_SLAVES: return "reconf_slaves";
                case SENTINEL_FAILOVER_STATE_UPDATE_CONFIG: return "update_config";
                default: return "unknown";
            }
        }

        /* Redis instance to Redis protocol representation. */
        // 以 Redis 协议的形式返回 Redis 实例的情况
        void addReplySentinelRedisInstance(client *c, sentinelRedisInstance *ri) {
            char *flags = sdsempty();
            void *mbl;
            int fields = 0;

            mbl = addReplyDeferredLen(c);

            addReplyBulkCString(c,"name");
            addReplyBulkCString(c,ri->name);
            fields++;

            addReplyBulkCString(c,"ip");
            addReplyBulkCString(c,announceSentinelAddr(ri->addr));
            fields++;

            addReplyBulkCString(c,"port");
            addReplyBulkLongLong(c,ri->addr->port);
            fields++;

            addReplyBulkCString(c,"runid");
            addReplyBulkCString(c,ri->runid ? ri->runid : "");
            fields++;

            addReplyBulkCString(c,"flags");
            if (ri->flags & SRI_S_DOWN) flags = sdscat(flags,"s_down,");
            if (ri->flags & SRI_O_DOWN) flags = sdscat(flags,"o_down,");
            if (ri->flags & SRI_MASTER) flags = sdscat(flags,"master,");
            if (ri->flags & SRI_SLAVE) flags = sdscat(flags,"slave,");
            if (ri->flags & SRI_SENTINEL) flags = sdscat(flags,"sentinel,");
            if (ri->link->disconnected) flags = sdscat(flags,"disconnected,");
            if (ri->flags & SRI_MASTER_DOWN) flags = sdscat(flags,"master_down,");
            if (ri->flags & SRI_FAILOVER_IN_PROGRESS)
                flags = sdscat(flags,"failover_in_progress,");
            if (ri->flags & SRI_PROMOTED) flags = sdscat(flags,"promoted,");
            if (ri->flags & SRI_RECONF_SENT) flags = sdscat(flags,"reconf_sent,");
            if (ri->flags & SRI_RECONF_INPROG) flags = sdscat(flags,"reconf_inprog,");
            if (ri->flags & SRI_RECONF_DONE) flags = sdscat(flags,"reconf_done,");
            if (ri->flags & SRI_FORCE_FAILOVER) flags = sdscat(flags,"force_failover,");
            if (ri->flags & SRI_SCRIPT_KILL_SENT) flags = sdscat(flags,"script_kill_sent,");

            if (sdslen(flags) != 0) sdsrange(flags,0,-2); /* remove last "," */
            addReplyBulkCString(c,flags);
            sdsfree(flags);
            fields++;

            addReplyBulkCString(c,"link-pending-commands");
            addReplyBulkLongLong(c,ri->link->pending_commands);
            fields++;

            addReplyBulkCString(c,"link-refcount");
            addReplyBulkLongLong(c,ri->link->refcount);
            fields++;

            if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
                addReplyBulkCString(c,"failover-state");
                addReplyBulkCString(c,(char*)sentinelFailoverStateStr(ri->failover_state));
                fields++;
            }

            addReplyBulkCString(c,"last-ping-sent");
            addReplyBulkLongLong(c,
                                 ri->link->act_ping_time ? (mstime() - ri->link->act_ping_time) : 0);
            fields++;

            addReplyBulkCString(c,"last-ok-ping-reply");
            addReplyBulkLongLong(c,mstime() - ri->link->last_avail_time);
            fields++;

            addReplyBulkCString(c,"last-ping-reply");
            addReplyBulkLongLong(c,mstime() - ri->link->last_pong_time);
            fields++;

            if (ri->flags & SRI_S_DOWN) {
                addReplyBulkCString(c,"s-down-time");
                addReplyBulkLongLong(c,mstime()-ri->s_down_since_time);
                fields++;
            }

            if (ri->flags & SRI_O_DOWN) {
                addReplyBulkCString(c,"o-down-time");
                addReplyBulkLongLong(c,mstime()-ri->o_down_since_time);
                fields++;
            }

            addReplyBulkCString(c,"down-after-milliseconds");
            addReplyBulkLongLong(c,ri->down_after_period);
            fields++;

            /* Masters and Slaves */
            if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
                addReplyBulkCString(c,"info-refresh");
                addReplyBulkLongLong(c,
                                     ri->info_refresh ? (mstime() - ri->info_refresh) : 0);
                fields++;

                addReplyBulkCString(c,"role-reported");
                addReplyBulkCString(c, (ri->role_reported == SRI_MASTER) ? "master" :
                "slave");
                fields++;

                addReplyBulkCString(c,"role-reported-time");
                addReplyBulkLongLong(c,mstime() - ri->role_reported_time);
                fields++;
            }

            /* Only masters */
            if (ri->flags & SRI_MASTER) {
                addReplyBulkCString(c,"config-epoch");
                addReplyBulkLongLong(c,ri->config_epoch);
                fields++;

                addReplyBulkCString(c,"num-slaves");
                addReplyBulkLongLong(c,dictSize(ri->slaves));
                fields++;

                addReplyBulkCString(c,"num-other-sentinels");
                addReplyBulkLongLong(c,dictSize(ri->sentinels));
                fields++;

                addReplyBulkCString(c,"quorum");
                addReplyBulkLongLong(c,ri->quorum);
                fields++;

                addReplyBulkCString(c,"failover-timeout");
                addReplyBulkLongLong(c,ri->failover_timeout);
                fields++;

                addReplyBulkCString(c,"parallel-syncs");
                addReplyBulkLongLong(c,ri->parallel_syncs);
                fields++;

                if (ri->notification_script) {
                    addReplyBulkCString(c,"notification-script");
                    addReplyBulkCString(c,ri->notification_script);
                    fields++;
                }

                if (ri->client_reconfig_script) {
                    addReplyBulkCString(c,"client-reconfig-script");
                    addReplyBulkCString(c,ri->client_reconfig_script);
                    fields++;
                }
            }

            /* Only slaves */
            if (ri->flags & SRI_SLAVE) {
                addReplyBulkCString(c,"master-link-down-time");
                addReplyBulkLongLong(c,ri->master_link_down_time);
                fields++;

                addReplyBulkCString(c,"master-link-status");
                addReplyBulkCString(c,
                                    (ri->slave_master_link_status == SENTINEL_MASTER_LINK_STATUS_UP) ?
                                    "ok" : "err");
                fields++;

                addReplyBulkCString(c,"master-host");
                addReplyBulkCString(c,
                                    ri->slave_master_host ? ri->slave_master_host : "?");
                fields++;

                addReplyBulkCString(c,"master-port");
                addReplyBulkLongLong(c,ri->slave_master_port);
                fields++;

                addReplyBulkCString(c,"slave-priority");
                addReplyBulkLongLong(c,ri->slave_priority);
                fields++;

                addReplyBulkCString(c,"slave-repl-offset");
                addReplyBulkLongLong(c,ri->slave_repl_offset);
                fields++;

                addReplyBulkCString(c,"replica-announced");
                addReplyBulkLongLong(c,ri->replica_announced);
                fields++;
            }

            /* Only sentinels */
            if (ri->flags & SRI_SENTINEL) {
                addReplyBulkCString(c,"last-hello-message");
                addReplyBulkLongLong(c,mstime() - ri->last_hello_time);
                fields++;

                addReplyBulkCString(c,"voted-leader");
                addReplyBulkCString(c,ri->leader ? ri->leader : "?");
                fields++;

                addReplyBulkCString(c,"voted-leader-epoch");
                addReplyBulkLongLong(c,ri->leader_epoch);
                fields++;
            }

            setDeferredMapLen(c,mbl,fields);
        }

        /* Output a number of instances contained inside a dictionary as
         * Redis protocol. */
        // 打印各个实例的情况
        void addReplyDictOfRedisInstances(client *c, dict *instances) {
            dictIterator *di;
            dictEntry *de;
            long slaves = 0;
            void *replylen = addReplyDeferredLen(c);

            di = dictGetIterator(instances);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);

                /* don't announce unannounced replicas */
                if (ri->flags & SRI_SLAVE && !ri->replica_announced) continue;
                addReplySentinelRedisInstance(c,ri);
                slaves++;
            }
            dictReleaseIterator(di);
            setDeferredArrayLen(c, replylen, slaves);
        }

        /* Lookup the named master into sentinel.masters.
         * If the master is not found reply to the client with an error and returns
         * NULL. */
        // 在 sentinel.masters 字典中查找给定名字的 master
        // 没找到则返回 NULL
        sentinelRedisInstance *sentinelGetMasterByNameOrReplyError(client *c,
                                                                   robj *name)
                                                                   {
            sentinelRedisInstance *ri;

            ri = dictFetchValue(sentinel.masters,name->ptr);
            if (!ri) {
                addReplyError(c,"No such master with that name");
                return NULL;
            }
            return ri;
                                                                   }

#define SENTINEL_ISQR_OK 0
#define SENTINEL_ISQR_NOQUORUM (1<<0)
#define SENTINEL_ISQR_NOAUTH (1<<1)
int sentinelIsQuorumReachable(sentinelRedisInstance *master, int *usableptr) {
            dictIterator *di;
            dictEntry *de;
            int usable = 1; /* Number of usable Sentinels. Init to 1 to count myself. */
            int result = SENTINEL_ISQR_OK;
            int voters = dictSize(master->sentinels)+1; /* Known Sentinels + myself. */

            di = dictGetIterator(master->sentinels);
            while((de = dictNext(di)) != NULL) {
                sentinelRedisInstance *ri = dictGetVal(de);

                if (ri->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue;
                usable++;
            }
            dictReleaseIterator(di);

            if (usable < (int)master->quorum) result |= SENTINEL_ISQR_NOQUORUM;
            if (usable < voters/2+1) result |= SENTINEL_ISQR_NOAUTH;
            if (usableptr) *usableptr = usable;
            return result;
        }


        // SENTINEL 命令的实现
        void sentinelCommand(client *c) {
            if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
                const char *help[] = {
                        "CKQUORUM <master-name>",
                        "    Check if the current Sentinel configuration is able to reach the quorum",
                        "    needed to failover a master and the majority needed to authorize the",
                        "    failover.",
                        "CONFIG SET <param> <value>",
                        "    Set a global Sentinel configuration parameter.",
                        "CONFIG GET <param>",
                        "    Get global Sentinel configuration parameter.",
                        "GET-MASTER-ADDR-BY-NAME <master-name>",
                        "    Return the ip and port number of the master with that name.",
                        "FAILOVER <master-name>",
                        "    Manually failover a master node without asking for agreement from other",
                        "    Sentinels",
                        "FLUSHCONFIG",
                        "    Force Sentinel to rewrite its configuration on disk, including the current",
                        "    Sentinel state.",
                        "INFO-CACHE <master-name>",
                        "    Return last cached INFO output from masters and all its replicas.",
                        "IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>",
                        "    Check if the master specified by ip:port is down from current Sentinel's",
                        "    point of view.",
                        "MASTER <master-name>",
                        "    Show the state and info of the specified master.",
                        "MASTERS",
                        "    Show a list of monitored masters and their state.",
                        "MONITOR <name> <ip> <port> <quorum>",
                        "    Start monitoring a new master with the specified name, ip, port and quorum.",
                        "MYID",
                        "    Return the ID of the Sentinel instance.",
                        "PENDING-SCRIPTS",
                        "    Get pending scripts information.",
                        "REMOVE <master-name>",
                        "    Remove master from Sentinel's monitor list.",
                        "REPLICAS <master-name>",
                        "    Show a list of replicas for this master and their state.",
                        "RESET <pattern>",
                        "    Reset masters for specific master name matching this pattern.",
                        "SENTINELS <master-name>",
                        "    Show a list of Sentinel instances for this master and their state.",
                        "SET <master-name> <option> <value>",
                        "    Set configuration paramters for certain masters.",
                        "SIMULATE-FAILURE (CRASH-AFTER-ELECTION|CRASH-AFTER-PROMOTION|HELP)",
                        "    Simulate a Sentinel crash.",
                        NULL
                };
                addReplyHelp(c, help);
            } else if (!strcasecmp(c->argv[1]->ptr,"masters")) {
                /* SENTINEL MASTERS */
                if (c->argc != 2) goto numargserr;
                addReplyDictOfRedisInstances(c,sentinel.masters);
            } else if (!strcasecmp(c->argv[1]->ptr,"master")) {
                /* SENTINEL MASTER <name> */
                sentinelRedisInstance *ri;

                if (c->argc != 3) goto numargserr;
                if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
                == NULL) return;
                addReplySentinelRedisInstance(c,ri);
            } else if (!strcasecmp(c->argv[1]->ptr,"slaves") ||
            !strcasecmp(c->argv[1]->ptr,"replicas"))
            {
                /* SENTINEL REPLICAS <master-name> */
                sentinelRedisInstance *ri;

                if (c->argc != 3) goto numargserr;
                if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
                    return;
                addReplyDictOfRedisInstances(c,ri->slaves);
            } else if (!strcasecmp(c->argv[1]->ptr,"sentinels")) {
                /* SENTINEL SENTINELS <master-name> */
                sentinelRedisInstance *ri;

                if (c->argc != 3) goto numargserr;
                if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
                    return;
                addReplyDictOfRedisInstances(c,ri->sentinels);
            } else if (!strcasecmp(c->argv[1]->ptr,"myid") && c->argc == 2) {
                /* SENTINEL MYID */
                addReplyBulkCBuffer(c,sentinel.myid,CONFIG_RUN_ID_SIZE);
            } else if (!strcasecmp(c->argv[1]->ptr,"is-master-down-by-addr")) {
                /* SENTINEL IS-MASTER-DOWN-BY-ADDR <ip> <port> <current-epoch> <runid>
                 *
                 * Arguments:
                 *
                 * ip and port are the ip and port of the master we want to be
                 * checked by Sentinel. Note that the command will not check by
                 * name but just by master, in theory different Sentinels may monitor
                 * different masters with the same name.
                 *
                 * current-epoch is needed in order to understand if we are allowed
                 * to vote for a failover leader or not. Each Sentinel can vote just
                 * one time per epoch.
                 *
                 * runid is "*" if we are not seeking for a vote from the Sentinel
                 * in order to elect the failover leader. Otherwise it is set to the
                 * runid we want the Sentinel to vote if it did not already voted.
                 */
                sentinelRedisInstance *ri;
                long long req_epoch;
                uint64_t leader_epoch = 0;
                char *leader = NULL;
                long port;
                int isdown = 0;

                if (c->argc != 6) goto numargserr;
                if (getLongFromObjectOrReply(c,c->argv[3],&port,NULL) != C_OK ||
                getLongLongFromObjectOrReply(c,c->argv[4],&req_epoch,NULL)
                != C_OK)
                    return;
                ri = getSentinelRedisInstanceByAddrAndRunID(sentinel.masters,
                                                            c->argv[2]->ptr,port,NULL);

                /* It exists? Is actually a master? Is subjectively down? It's down.
                 * Note: if we are in tilt mode we always reply with "0". */
                if (!sentinel.tilt && ri && (ri->flags & SRI_S_DOWN) &&
                (ri->flags & SRI_MASTER))
                    isdown = 1;

                /* Vote for the master (or fetch the previous vote) if the request
                 * includes a runid, otherwise the sender is not seeking for a vote. */
                if (ri && ri->flags & SRI_MASTER && strcasecmp(c->argv[5]->ptr,"*")) {
                    leader = sentinelVoteLeader(ri,(uint64_t)req_epoch,
                                                c->argv[5]->ptr,
                                                &leader_epoch);
                }

                /* Reply with a three-elements multi-bulk reply:
                 * down state, leader, vote epoch. */

                // 多条回复
                // 1) <down_state>    1 代表下线， 0 代表未下线
                // 2) <leader_runid>  Sentinel 选举作为领头 Sentinel 的运行 ID
                // 3) <leader_epoch>  领头 Sentinel 目前的配置纪元
                addReplyArrayLen(c,3);
                addReply(c, isdown ? shared.cone : shared.czero);
                addReplyBulkCString(c, leader ? leader : "*");
                addReplyLongLong(c, (long long)leader_epoch);
                if (leader) sdsfree(leader);
            } else if (!strcasecmp(c->argv[1]->ptr,"reset")) {
                /* SENTINEL RESET <pattern> */
                if (c->argc != 3) goto numargserr;
                addReplyLongLong(c,sentinelResetMastersByPattern(c->argv[2]->ptr,SENTINEL_GENERATE_EVENT));
            } else if (!strcasecmp(c->argv[1]->ptr,"get-master-addr-by-name")) {
                /* SENTINEL GET-MASTER-ADDR-BY-NAME <master-name> */
                sentinelRedisInstance *ri;

                if (c->argc != 3) goto numargserr;
                ri = sentinelGetMasterByName(c->argv[2]->ptr);
                if (ri == NULL) {
                    addReplyNullArray(c);
                } else {
                    sentinelAddr *addr = sentinelGetCurrentMasterAddress(ri);

                    addReplyArrayLen(c,2);
                    addReplyBulkCString(c,announceSentinelAddr(addr));
                    addReplyBulkLongLong(c,addr->port);
                }
            } else if (!strcasecmp(c->argv[1]->ptr,"failover")) {
                /* SENTINEL FAILOVER <master-name> */
                sentinelRedisInstance *ri;

                if (c->argc != 3) goto numargserr;
                if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2])) == NULL)
                    return;
                if (ri->flags & SRI_FAILOVER_IN_PROGRESS) {
                    addReplySds(c,sdsnew("-INPROG Failover already in progress\r\n"));
                    return;
                }
                if (sentinelSelectSlave(ri) == NULL) {
                    addReplySds(c,sdsnew("-NOGOODSLAVE No suitable replica to promote\r\n"));
                    return;
                }
                serverLog(LL_WARNING,"Executing user requested FAILOVER of '%s'",
                          ri->name);
                sentinelStartFailover(ri);
                ri->flags |= SRI_FORCE_FAILOVER;
                addReply(c,shared.ok);
            } else if (!strcasecmp(c->argv[1]->ptr,"pending-scripts")) {
                /* SENTINEL PENDING-SCRIPTS */

                if (c->argc != 2) goto numargserr;
                sentinelPendingScriptsCommand(c);
            } else if (!strcasecmp(c->argv[1]->ptr,"monitor")) {
                /* SENTINEL MONITOR <name> <ip> <port> <quorum> */
                sentinelRedisInstance *ri;
                long quorum, port;
                char ip[NET_IP_STR_LEN];

                if (c->argc != 6) goto numargserr;
                if (getLongFromObjectOrReply(c,c->argv[5],&quorum,"Invalid quorum")
                != C_OK) return;
                if (getLongFromObjectOrReply(c,c->argv[4],&port,"Invalid port")
                != C_OK) return;

                if (quorum <= 0) {
                    addReplyError(c, "Quorum must be 1 or greater.");
                    return;
                }

                /* If resolve-hostnames is used, actual DNS resolution may take place.
                 * Otherwise just validate address.
                 */
                if (anetResolve(NULL,c->argv[3]->ptr,ip,sizeof(ip),
                                sentinel.resolve_hostnames ? ANET_NONE : ANET_IP_ONLY) == ANET_ERR) {
                    addReplyError(c, "Invalid IP address or hostname specified");
                    return;
                }

                /* Parameters are valid. Try to create the master instance. */
                ri = createSentinelRedisInstance(c->argv[2]->ptr,SRI_MASTER,
                                                 c->argv[3]->ptr,port,quorum,NULL);
                if (ri == NULL) {
                    addReplyError(c,sentinelCheckCreateInstanceErrors(SRI_MASTER));
                } else {
                    sentinelFlushConfig();
                    sentinelEvent(LL_WARNING,"+monitor",ri,"%@ quorum %d",ri->quorum);
                    addReply(c,shared.ok);
                }
            } else if (!strcasecmp(c->argv[1]->ptr,"flushconfig")) {
                if (c->argc != 2) goto numargserr;
                sentinelFlushConfig();
                addReply(c,shared.ok);
                return;
            } else if (!strcasecmp(c->argv[1]->ptr,"remove")) {
                /* SENTINEL REMOVE <name> */
                sentinelRedisInstance *ri;

                if (c->argc != 3) goto numargserr;
                if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
                == NULL) return;
                sentinelEvent(LL_WARNING,"-monitor",ri,"%@");
                dictDelete(sentinel.masters,c->argv[2]->ptr);
                sentinelFlushConfig();
                addReply(c,shared.ok);
            } else if (!strcasecmp(c->argv[1]->ptr,"ckquorum")) {
                /* SENTINEL CKQUORUM <name> */
                sentinelRedisInstance *ri;
                int usable;

                if (c->argc != 3) goto numargserr;
                if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
                == NULL) return;
                int result = sentinelIsQuorumReachable(ri,&usable);
                if (result == SENTINEL_ISQR_OK) {
                    addReplySds(c, sdscatfmt(sdsempty(),
                                             "+OK %i usable Sentinels. Quorum and failover authorization "
                                             "can be reached\r\n",usable));
                } else {
                    sds e = sdscatfmt(sdsempty(),
                                      "-NOQUORUM %i usable Sentinels. ",usable);
                    if (result & SENTINEL_ISQR_NOQUORUM)
                        e = sdscat(e,"Not enough available Sentinels to reach the"
                                     " specified quorum for this master");
                    if (result & SENTINEL_ISQR_NOAUTH) {
                        if (result & SENTINEL_ISQR_NOQUORUM) e = sdscat(e,". ");
                        e = sdscat(e, "Not enough available Sentinels to reach the"
                                      " majority and authorize a failover");
                    }
                    e = sdscat(e,"\r\n");
                    addReplySds(c,e);
                }
            } else if (!strcasecmp(c->argv[1]->ptr,"set")) {
                if (c->argc < 3) goto numargserr;
                sentinelSetCommand(c);
            } else if (!strcasecmp(c->argv[1]->ptr,"config")) {
                if (c->argc < 3) goto numargserr;
                if (!strcasecmp(c->argv[2]->ptr,"set") && c->argc == 5)
                    sentinelConfigSetCommand(c);
                else if (!strcasecmp(c->argv[2]->ptr,"get") && c->argc == 4)
                    sentinelConfigGetCommand(c);
                else
                    addReplyError(c, "Only SENTINEL CONFIG GET <option> / SET <option> <value> are supported.");
            } else if (!strcasecmp(c->argv[1]->ptr,"info-cache")) {
                /* SENTINEL INFO-CACHE <name> */
                if (c->argc < 2) goto numargserr;
                mstime_t now = mstime();

                /* Create an ad-hoc dictionary type so that we can iterate
                 * a dictionary composed of just the master groups the user
                 * requested. */
                dictType copy_keeper = instancesDictType;
                copy_keeper.valDestructor = NULL;
                dict *masters_local = sentinel.masters;
                if (c->argc > 2) {
                    masters_local = dictCreate(&copy_keeper, NULL);

                    for (int i = 2; i < c->argc; i++) {
                        sentinelRedisInstance *ri;
                        ri = sentinelGetMasterByName(c->argv[i]->ptr);
                        if (!ri) continue; /* ignore non-existing names */
                        dictAdd(masters_local, ri->name, ri);
                    }
                }

                /* Reply format:
                 *   1.) master name
                 *   2.) 1.) info from master
                 *       2.) info from replica
                 *       ...
                 *   3.) other master name
                 *   ...
                 */
                addReplyArrayLen(c,dictSize(masters_local) * 2);

                dictIterator  *di;
                dictEntry *de;
                di = dictGetIterator(masters_local);
                while ((de = dictNext(di)) != NULL) {
                    sentinelRedisInstance *ri = dictGetVal(de);
                    addReplyBulkCBuffer(c,ri->name,strlen(ri->name));
                    addReplyArrayLen(c,dictSize(ri->slaves) + 1); /* +1 for self */
                    addReplyArrayLen(c,2);
                    addReplyLongLong(c,
                                     ri->info_refresh ? (now - ri->info_refresh) : 0);
                    if (ri->info)
                        addReplyBulkCBuffer(c,ri->info,sdslen(ri->info));
                    else
                        addReplyNull(c);

                    dictIterator *sdi;
                    dictEntry *sde;
                    sdi = dictGetIterator(ri->slaves);
                    while ((sde = dictNext(sdi)) != NULL) {
                        sentinelRedisInstance *sri = dictGetVal(sde);
                        addReplyArrayLen(c,2);
                        addReplyLongLong(c,
                                         ri->info_refresh ? (now - sri->info_refresh) : 0);
                        if (sri->info)
                            addReplyBulkCBuffer(c,sri->info,sdslen(sri->info));
                        else
                            addReplyNull(c);
                    }
                    dictReleaseIterator(sdi);
                }
                dictReleaseIterator(di);
                if (masters_local != sentinel.masters) dictRelease(masters_local);
            } else if (!strcasecmp(c->argv[1]->ptr,"simulate-failure")) {
                /* SENTINEL SIMULATE-FAILURE <flag> <flag> ... <flag> */
                int j;

                sentinel.simfailure_flags = SENTINEL_SIMFAILURE_NONE;
                for (j = 2; j < c->argc; j++) {
                    if (!strcasecmp(c->argv[j]->ptr,"crash-after-election")) {
                        sentinel.simfailure_flags |=
                                SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION;
                        serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                                             "will crash after being successfully elected as failover "
                                             "leader");
                    } else if (!strcasecmp(c->argv[j]->ptr,"crash-after-promotion")) {
                        sentinel.simfailure_flags |=
                                SENTINEL_SIMFAILURE_CRASH_AFTER_PROMOTION;
                        serverLog(LL_WARNING,"Failure simulation: this Sentinel "
                                             "will crash after promoting the selected replica to master");
                    } else if (!strcasecmp(c->argv[j]->ptr,"help")) {
                        addReplyArrayLen(c,2);
                        addReplyBulkCString(c,"crash-after-election");
                        addReplyBulkCString(c,"crash-after-promotion");
                    } else {
                        addReplyError(c,"Unknown failure simulation specified");
                        return;
                    }
                }
                addReply(c,shared.ok);
            } else {
                addReplySubcommandSyntaxError(c);
            }
            return;

            numargserr:
            addReplyErrorFormat(c,"Wrong number of arguments for 'sentinel %s'",
                                (char*)c->argv[1]->ptr);
        }

#define info_section_from_redis(section_name) do { \
if (defsections || allsections || !strcasecmp(section,section_name)) { \
sds redissection; \
if (sections++) info = sdscat(info,"\r\n"); \
redissection = genRedisInfoString(section_name); \
info = sdscatlen(info,redissection,sdslen(redissection)); \
sdsfree(redissection); \
} \
} while(0)

/* SENTINEL INFO [section] */

// sentinel 模式下的 INFO 命令实现
void sentinelInfoCommand(client *c) {
    if (c->argc > 2) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    int defsections = 0, allsections = 0;
    char *section = c->argc == 2 ? c->argv[1]->ptr : NULL;
    if (section) {
        allsections = !strcasecmp(section,"all");
        defsections = !strcasecmp(section,"default");
    } else {
        defsections = 1;
    }

    int sections = 0;
    sds info = sdsempty();

    info_section_from_redis("server");
    info_section_from_redis("clients");
    info_section_from_redis("cpu");
    info_section_from_redis("stats");

    if (defsections || allsections || !strcasecmp(section,"sentinel")) {
        dictIterator *di;
        dictEntry *de;
        int master_id = 0;

        if (sections++) info = sdscat(info,"\r\n");
        info = sdscatprintf(info,
                            "# Sentinel\r\n"
                            "sentinel_masters:%lu\r\n"
                            "sentinel_tilt:%d\r\n"
                            "sentinel_running_scripts:%d\r\n"
                            "sentinel_scripts_queue_length:%ld\r\n"
                            "sentinel_simulate_failure_flags:%lu\r\n",
                            dictSize(sentinel.masters),
                            sentinel.tilt,
                            sentinel.running_scripts,
                            listLength(sentinel.scripts_queue),
                            sentinel.simfailure_flags);

        di = dictGetIterator(sentinel.masters);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);
            char *status = "ok";

            if (ri->flags & SRI_O_DOWN) status = "odown";
            else if (ri->flags & SRI_S_DOWN) status = "sdown";
            info = sdscatprintf(info,
                                "master%d:name=%s,status=%s,address=%s:%d,"
                                "slaves=%lu,sentinels=%lu\r\n",
                                master_id++, ri->name, status,
                                announceSentinelAddr(ri->addr), ri->addr->port,
                                dictSize(ri->slaves),
                                dictSize(ri->sentinels)+1);
        }
        dictReleaseIterator(di);
    }

    addReplyBulkSds(c, info);
}

/* Implements Sentinel version of the ROLE command. The output is
 * "sentinel" and the list of currently monitored master names. */
void sentinelRoleCommand(client *c) {
    dictIterator *di;
    dictEntry *de;

    addReplyArrayLen(c,2);
    addReplyBulkCBuffer(c,"sentinel",8);
    addReplyArrayLen(c,dictSize(sentinel.masters));

    di = dictGetIterator(sentinel.masters);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        addReplyBulkCString(c,ri->name);
    }
    dictReleaseIterator(di);
}

/* SENTINEL SET <mastername> [<option> <value> ...] */
void sentinelSetCommand(client *c) {
    sentinelRedisInstance *ri;
    int j, changes = 0;
    int badarg = 0; /* Bad argument position for error reporting. */
    char *option;

    if ((ri = sentinelGetMasterByNameOrReplyError(c,c->argv[2]))
    == NULL) return;

    /* Process option - value pairs. */
    for (j = 3; j < c->argc; j++) {
        int moreargs = (c->argc-1) - j;
        option = c->argv[j]->ptr;
        long long ll;
        int old_j = j; /* Used to know what to log as an event. */

        if (!strcasecmp(option,"down-after-milliseconds") && moreargs > 0) {
            /* down-after-millisecodns <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->down_after_period = ll;
            sentinelPropagateDownAfterPeriod(ri);
            changes++;
        } else if (!strcasecmp(option,"failover-timeout") && moreargs > 0) {
            /* failover-timeout <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->failover_timeout = ll;
            changes++;
        } else if (!strcasecmp(option,"parallel-syncs") && moreargs > 0) {
            /* parallel-syncs <milliseconds> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->parallel_syncs = ll;
            changes++;
        } else if (!strcasecmp(option,"notification-script") && moreargs > 0) {
            /* notification-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                              "Reconfiguration of scripts path is denied for "
                              "security reasons. Check the deny-scripts-reconfig "
                              "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                              "Notification script seems non existing or non executable");
                goto seterr;
            }
            sdsfree(ri->notification_script);
            ri->notification_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"client-reconfig-script") && moreargs > 0) {
            /* client-reconfig-script <path> */
            char *value = c->argv[++j]->ptr;
            if (sentinel.deny_scripts_reconfig) {
                addReplyError(c,
                              "Reconfiguration of scripts path is denied for "
                              "security reasons. Check the deny-scripts-reconfig "
                              "configuration directive in your Sentinel configuration");
                goto seterr;
            }

            if (strlen(value) && access(value,X_OK) == -1) {
                addReplyError(c,
                              "Client reconfiguration script seems non existing or "
                              "non executable");
                goto seterr;
            }
            sdsfree(ri->client_reconfig_script);
            ri->client_reconfig_script = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"auth-pass") && moreargs > 0) {
            /* auth-pass <password> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_pass);
            ri->auth_pass = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"auth-user") && moreargs > 0) {
            /* auth-user <username> */
            char *value = c->argv[++j]->ptr;
            sdsfree(ri->auth_user);
            ri->auth_user = strlen(value) ? sdsnew(value) : NULL;
            changes++;
        } else if (!strcasecmp(option,"quorum") && moreargs > 0) {
            /* quorum <count> */
            robj *o = c->argv[++j];
            if (getLongLongFromObject(o,&ll) == C_ERR || ll <= 0) {
                badarg = j;
                goto badfmt;
            }
            ri->quorum = ll;
            changes++;
        } else if (!strcasecmp(option,"rename-command") && moreargs > 1) {
            /* rename-command <oldname> <newname> */
            sds oldname = c->argv[++j]->ptr;
            sds newname = c->argv[++j]->ptr;

            if ((sdslen(oldname) == 0) || (sdslen(newname) == 0)) {
                badarg = sdslen(newname) ? j-1 : j;
                goto badfmt;
            }

            /* Remove any older renaming for this command. */
            dictDelete(ri->renamed_commands,oldname);

            /* If the target name is the same as the source name there
             * is no need to add an entry mapping to itself. */
            if (!dictSdsKeyCaseCompare(NULL,oldname,newname)) {
                oldname = sdsdup(oldname);
                newname = sdsdup(newname);
                dictAdd(ri->renamed_commands,oldname,newname);
            }
            changes++;
        } else {
            addReplyErrorFormat(c,"Unknown option or number of arguments for "
                                  "SENTINEL SET '%s'", option);
            goto seterr;
        }

        /* Log the event. */
        int numargs = j-old_j+1;
        switch(numargs) {
            case 2:
                sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s",(char*)c->argv[old_j]->ptr,
                              (char*)c->argv[old_j+1]->ptr);
                break;
                case 3:
                    sentinelEvent(LL_WARNING,"+set",ri,"%@ %s %s %s",(char*)c->argv[old_j]->ptr,
                                  (char*)c->argv[old_j+1]->ptr,
                                  (char*)c->argv[old_j+2]->ptr);
                    break;
                    default:
                        sentinelEvent(LL_WARNING,"+set",ri,"%@ %s",(char*)c->argv[old_j]->ptr);
                        break;
        }
    }

    if (changes) sentinelFlushConfig();
    addReply(c,shared.ok);
    return;

    badfmt: /* Bad format errors */
    addReplyErrorFormat(c,"Invalid argument '%s' for SENTINEL SET '%s'",
                        (char*)c->argv[badarg]->ptr,option);
    seterr:
    if (changes) sentinelFlushConfig();
    return;
}

/* Our fake PUBLISH command: it is actually useful only to receive hello messages
 * from the other sentinel instances, and publishing to a channel other than
 * SENTINEL_HELLO_CHANNEL is forbidden.
 *
 * Because we have a Sentinel PUBLISH, the code to send hello messages is the same
 * for all the three kind of instances: masters, slaves, sentinels. */
void sentinelPublishCommand(client *c) {
    if (strcmp(c->argv[1]->ptr,SENTINEL_HELLO_CHANNEL)) {
        addReplyError(c, "Only HELLO messages are accepted by Sentinel instances.");
        return;
    }
    sentinelProcessHelloMessage(c->argv[2]->ptr,sdslen(c->argv[2]->ptr));
    addReplyLongLong(c,1);
}

/* ===================== SENTINEL availability checks ======================= */

/* Is this instance down from our point of view? */
// 检查实例是否以下线（从本 Sentinel 的角度来看）
void sentinelCheckSubjectivelyDown(sentinelRedisInstance *ri) {
    mstime_t elapsed = 0;

    if (ri->link->act_ping_time)
        elapsed = mstime() - ri->link->act_ping_time;
    else if (ri->link->disconnected)
        elapsed = mstime() - ri->link->last_avail_time;

    /* Check if we are in need for a reconnection of one of the
     * links, because we are detecting low activity.
     *
     * 如果检测到连接的活跃度（activity）很低，那么考虑重断开连接，并进行重连
     *
     * 1) Check if the command link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have a
     *    pending ping for more than half the timeout. */
    // 考虑断开实例的 cc 连接
    if (ri->link->cc &&
    (mstime() - ri->link->cc_conn_time) >
    SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
    ri->link->act_ping_time != 0 && /* There is a pending ping... */
    /* The pending ping is delayed, and we did not receive
     * error replies as well. */
    (mstime() - ri->link->act_ping_time) > (ri->down_after_period/2) &&
    (mstime() - ri->link->last_pong_time) > (ri->down_after_period/2))
    {
        instanceLinkCloseConnection(ri->link,ri->link->cc);
    }

    /* 2) Check if the pubsub link seems connected, was connected not less
     *    than SENTINEL_MIN_LINK_RECONNECT_PERIOD, but still we have no
     *    activity in the Pub/Sub channel for more than
     *    SENTINEL_PUBLISH_PERIOD * 3.
     */
    // 考虑断开实例的 pc 连接
    if (ri->link->pc &&
    (mstime() - ri->link->pc_conn_time) >
    SENTINEL_MIN_LINK_RECONNECT_PERIOD &&
    (mstime() - ri->link->pc_last_activity) > (SENTINEL_PUBLISH_PERIOD*3))
    {
        instanceLinkCloseConnection(ri->link,ri->link->pc);
    }

    /* Update the SDOWN flag. We believe the instance is SDOWN if:
     *
     * 更新 SDOWN 标识。如果以下条件被满足，那么 Sentinel 认为实例已下线：
     * 1) It is not replying.
     *    它没有回应命令
     * 2) We believe it is a master, it reports to be a slave for enough time
     *    to meet the down_after_period, plus enough time to get two times
     *    INFO report from the instance.
     *    Sentinel 认为实例是主服务器，这个服务器向 Sentinel 报告它将成为从服务器，
     *    但在超过给定时限之后，服务器仍然没有完成这一角色转换。
     */
    if (elapsed > ri->down_after_period ||
    (ri->flags & SRI_MASTER &&
    ri->role_reported == SRI_SLAVE &&
    mstime() - ri->role_reported_time >
    (ri->down_after_period+SENTINEL_INFO_PERIOD*2)))
    {
        /* Is subjectively down */
        if ((ri->flags & SRI_S_DOWN) == 0) {
            // 发送事件
            sentinelEvent(LL_WARNING,"+sdown",ri,"%@");
            // 记录进入 SDOWN 状态的时间
            ri->s_down_since_time = mstime();
            // 打开 SDOWN 标志
            ri->flags |= SRI_S_DOWN;
        }
    } else {
        // 移除（可能有的） SDOWN 状态
        /* Is subjectively up */
        if (ri->flags & SRI_S_DOWN) {
            // 发送事件
            sentinelEvent(LL_WARNING,"-sdown",ri,"%@");
            // 移除相关标志
            ri->flags &= ~(SRI_S_DOWN|SRI_SCRIPT_KILL_SENT);
        }
    }
}

/* Is this instance down according to the configured quorum?
 *
 * 根据给定数量的 Sentinel 投票，判断实例是否已下线。
 *
 * Note that ODOWN is a weak quorum, it only means that enough Sentinels
 * reported in a given time range that the instance was not reachable.
 *
 * 注意 ODOWN 是一个 weak quorum ，它只意味着有足够多的 Sentinel
 * 在**给定的时间范围内**报告实例不可达。
 *
 * However messages can be delayed so there are no strong guarantees about
 * N instances agreeing at the same time about the down state.
 *
 * 因为 Sentinel 对实例的检测信息可能带有延迟，
 * 所以实际上 N 个 Sentinel **不可能在同一时间内**判断主服务器进入了下线状态。
 */
void sentinelCheckObjectivelyDown(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    unsigned int quorum = 0, odown = 0;

    // 如果当前 Sentinel 将主服务器判断为主观下线
    // 那么检查是否有其他 Sentinel 同意这一判断
    // 当同意的数量足够时，将主服务器判断为客观下线
    if (master->flags & SRI_S_DOWN) {
        /* Is down for enough sentinels? */

        // 统计同意的 Sentinel 数量（起始的 1 代表本 Sentinel）
        quorum = 1; /* the current sentinel. */

        /* Count all the other sentinels. */
        // 统计其他认为 master 进入下线状态的 Sentinel 的数量
        di = dictGetIterator(master->sentinels);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *ri = dictGetVal(de);

            // 该 SENTINEL 也认为 master 已下线
            if (ri->flags & SRI_MASTER_DOWN) quorum++;
        }
        dictReleaseIterator(di);

        // 如果投票得出的支持数目大于等于判断 ODOWN 所需的票数
        // 那么进入 ODOWN 状态
        if (quorum >= master->quorum) odown = 1;
    }

    /* Set the flag accordingly to the outcome. */
    if (odown) {

        // master 已 ODOWN
        if ((master->flags & SRI_O_DOWN) == 0) {
            // 发送事件
            sentinelEvent(LL_WARNING,"+odown",master,"%@ #quorum %d/%d",
                          quorum, master->quorum);
            // 打开 ODOWN 标志
            master->flags |= SRI_O_DOWN;
            // 记录进入 ODOWN 的时间
            master->o_down_since_time = mstime();
        }
    } else {
        // 未进入 ODOWN
        if (master->flags & SRI_O_DOWN) {

            // 如果 master 曾经进入过 ODOWN 状态，那么移除该状态

            // 发送事件
            sentinelEvent(LL_WARNING,"-odown",master,"%@");
            // 移除 ODOWN 标志
            master->flags &= ~SRI_O_DOWN;
        }
    }
}

/* Receive the SENTINEL is-master-down-by-addr reply, see the
 * sentinelAskMasterStateToOtherSentinels() function for more information. */
// 本回调函数用于处理SENTINEL 接收到其他 SENTINEL
// 发回的 SENTINEL is-master-down-by-addr 命令的回复
void sentinelReceiveIsMasterDownReply(redisAsyncContext *c, void *reply, void *privdata) {
    sentinelRedisInstance *ri = privdata;
    instanceLink *link = c->data;
    redisReply *r;

    if (!reply || !link) return;
    link->pending_commands--;
    r = reply;

    /* Ignore every error or unexpected reply.
     * 忽略错误回复
     * Note that if the command returns an error for any reason we'll
     * end clearing the SRI_MASTER_DOWN flag for timeout anyway. */
    if (r->type == REDIS_REPLY_ARRAY && r->elements == 3 &&
    r->element[0]->type == REDIS_REPLY_INTEGER &&
    r->element[1]->type == REDIS_REPLY_STRING &&
    r->element[2]->type == REDIS_REPLY_INTEGER)
    {
        // 更新最后一次回复询问的时间
        ri->last_master_down_reply_time = mstime();

        // 设置 SENTINEL 认为主服务器的状态
        if (r->element[0]->integer == 1) {
            // 已下线
            ri->flags |= SRI_MASTER_DOWN;
        } else {
            // 未下线
            ri->flags &= ~SRI_MASTER_DOWN;
        }

        // 如果运行 ID 不是 "*" 的话，那么这是一个带投票的回复
        if (strcmp(r->element[1]->str,"*")) {
            /* If the runid in the reply is not "*" the Sentinel actually
             * replied with a vote. */
            sdsfree(ri->leader);
            // 打印日志
            if ((long long)ri->leader_epoch != r->element[2]->integer)
                serverLog(LL_WARNING,
                          "%s voted for %s %llu", ri->name,
                          r->element[1]->str,
                          (unsigned long long) r->element[2]->integer);
            // 设置实例的领头
            ri->leader = sdsnew(r->element[1]->str);
            ri->leader_epoch = r->element[2]->integer;
        }
    }
}

/* If we think the master is down, we start sending
 * SENTINEL IS-MASTER-DOWN-BY-ADDR requests to other sentinels
 * in order to get the replies that allow to reach the quorum
 * needed to mark the master in ODOWN state and trigger a failover. */
// 如果 Sentinel 认为主服务器已下线，
// 那么它会通过向其他 Sentinel 发送 SENTINEL is-master-down-by-addr 命令，
// 尝试获得足够的票数，将主服务器标记为 ODOWN 状态，并开始一次故障转移操作
#define SENTINEL_ASK_FORCED (1<<0)
void sentinelAskMasterStateToOtherSentinels(sentinelRedisInstance *master, int flags) {
    dictIterator *di;
    dictEntry *de;

    // 遍历正在监视相同 master 的所有 sentinel
    // 向它们发送 SENTINEL is-master-down-by-addr 命令
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 距离该 sentinel 最后一次回复 SENTINEL master-down-by-addr 命令已经过了多久
        mstime_t elapsed = mstime() - ri->last_master_down_reply_time;

        char port[32];
        int retval;

        /* If the master state from other sentinel is too old, we clear it. */
        // 如果目标 Sentinel 关于主服务器的信息已经太久没更新，那么我们清除它
        if (elapsed > SENTINEL_ASK_PERIOD*5) {
            ri->flags &= ~SRI_MASTER_DOWN;
            sdsfree(ri->leader);
            ri->leader = NULL;
        }

        /* Only ask if master is down to other sentinels if:
         *
         * 只在以下情况满足时，才向其他 sentinel 询问主服务器是否已下线
         *
         * 1) We believe it is down, or there is a failover in progress.
         *    本 sentinel 相信服务器已经下线，或者针对该主服务器的故障转移操作正在执行
         * 2) Sentinel is connected.
         *    目标 Sentinel 与本 Sentinel 已连接
         * 3) We did not received the info within SENTINEL_ASK_PERIOD ms.
         *    当前 Sentinel 在 SENTINEL_ASK_PERIOD 毫秒内没有获得过目标 Sentinel 发来的信息
         * 4) 条件 1 和条件 2 满足而条件 3 不满足，但是 flags 参数给定了 SENTINEL_ASK_FORCED 标识
         */
        if ((master->flags & SRI_S_DOWN) == 0) continue;
        if (ri->link->disconnected) continue;
        if (!(flags & SENTINEL_ASK_FORCED) &&
        mstime() - ri->last_master_down_reply_time < SENTINEL_ASK_PERIOD)
            continue;

        /* Ask */
        // 发送 SENTINEL is-master-down-by-addr 命令
        ll2string(port,sizeof(port),master->addr->port);
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelReceiveIsMasterDownReply, ri,
                                   "%s is-master-down-by-addr %s %s %llu %s",
                                   sentinelInstanceMapCommand(ri,"SENTINEL"),
                                   announceSentinelAddr(master->addr), port,
                                   sentinel.current_epoch,
                                   // 如果本 Sentinel 已经检测到 master 进入 ODOWN
                                   // 并且要开始一次故障转移，那么向其他 Sentinel 发送自己的运行 ID
                                   // 让对方将给自己投一票（如果对方在这个纪元内还没有投票的话）
                                   (master->failover_state > SENTINEL_FAILOVER_STATE_NONE) ?
                                   sentinel.myid : "*");
        if (retval == C_OK) ri->link->pending_commands++;
    }
    dictReleaseIterator(di);
}

/* =============================== FAILOVER ================================= */

/* Crash because of user request via SENTINEL simulate-failure command. */
void sentinelSimFailureCrash(void) {
    serverLog(LL_WARNING,
              "Sentinel CRASH because of SENTINEL simulate-failure");
    exit(99);
}

/* Vote for the sentinel with 'req_runid' or return the old vote if already
 * voted for the specified 'req_epoch' or one greater.
 *
 * 为运行 ID 为 req_runid 的 Sentinel 投上一票，有两种额外情况可能出现：
 * 1) 如果 Sentinel 在 req_epoch 纪元已经投过票了，那么返回之前投的票。
 * 2) 如果 Sentinel 已经为大于 req_epoch 的纪元投过票了，那么返回更大纪元的投票。
 *
 * If a vote is not available returns NULL, otherwise return the Sentinel
 * runid and populate the leader_epoch with the epoch of the vote.
 *
 * 如果投票暂时不可用，那么返回 NULL 。
 * 否则返回 Sentinel 的运行 ID ，并将被投票的纪元保存到 leader_epoch 指针的值里面。
 */
char *sentinelVoteLeader(sentinelRedisInstance *master, uint64_t req_epoch, char *req_runid, uint64_t *leader_epoch) {
    if (req_epoch > sentinel.current_epoch) {
        sentinel.current_epoch = req_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
                      (unsigned long long) sentinel.current_epoch);
    }

    if (master->leader_epoch < req_epoch && sentinel.current_epoch <= req_epoch)
    {
        sdsfree(master->leader);
        master->leader = sdsnew(req_runid);
        master->leader_epoch = sentinel.current_epoch;
        sentinelFlushConfig();
        sentinelEvent(LL_WARNING,"+vote-for-leader",master,"%s %llu",
                      master->leader, (unsigned long long) master->leader_epoch);
        /* If we did not voted for ourselves, set the master failover start
         * time to now, in order to force a delay before we can start a
         * failover for the same master. */
        if (strcasecmp(master->leader,sentinel.myid))
            master->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    }

    *leader_epoch = master->leader_epoch;
    return master->leader ? sdsnew(master->leader) : NULL;
}

// 记录客观 leader 投票的结构
struct sentinelLeader {

    // sentinel 的运行 id
    char *runid;

    // 该 sentinel 获得的票数
    unsigned long votes;
};

/* Helper function for sentinelGetLeader, increment the counter
 * relative to the specified runid. */
// 为给定 ID 的 Sentinel 实例增加一票
int sentinelLeaderIncr(dict *counters, char *runid) {
    dictEntry *existing, *de;
    uint64_t oldval;

    de = dictAddRaw(counters,runid,&existing);
    if (existing) {
        oldval = dictGetUnsignedIntegerVal(existing);
        dictSetUnsignedIntegerVal(existing,oldval+1);
        return oldval+1;
    } else {
        serverAssert(de != NULL);
        dictSetUnsignedIntegerVal(de,1);
        return 1;
    }
}

/* Scan all the Sentinels attached to this master to check if there
 * is a leader for the specified epoch.
 *
 * 扫描所有监视 master 的 Sentinels ，查看是否有 Sentinels 是这个纪元的领头。
 *
 * To be a leader for a given epoch, we should have the majority of
 * the Sentinels we know (ever seen since the last SENTINEL RESET) that
 * reported the same instance as leader for the same epoch.
 *
 * 要让一个 Sentinel 成为本纪元的领头，
 * 这个 Sentinel 必须让大多数其他 Sentinel 承认它是该纪元的领头才行。
 */
// 选举出 master 在指定 epoch 上的领头
char *sentinelGetLeader(sentinelRedisInstance *master, uint64_t epoch) {
    dict *counters;
    dictIterator *di;
    dictEntry *de;
    unsigned int voters = 0, voters_quorum;
    char *myvote;
    char *winner = NULL;
    uint64_t leader_epoch;
    uint64_t max_votes = 0;

    serverAssert(master->flags & (SRI_O_DOWN|SRI_FAILOVER_IN_PROGRESS));
    // 统计器
    counters = dictCreate(&leaderVotesDictType,NULL);

    voters = dictSize(master->sentinels)+1; /* All the other sentinels and me.*/

    /* Count other sentinels votes */
    // 统计其他 sentinel 的主观 leader 投票
    di = dictGetIterator(master->sentinels);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *ri = dictGetVal(de);

        // 为目标 Sentinel 选出的领头 Sentinel 增加一票
        if (ri->leader != NULL && ri->leader_epoch == sentinel.current_epoch)
            sentinelLeaderIncr(counters,ri->leader);

        // 统计投票数量
        voters++;
    }
    dictReleaseIterator(di);

    /* Check what's the winner. For the winner to win, it needs two conditions:
     *
     * 选出领头 leader ，它必须满足以下两个条件：
     *
     * 1) Absolute majority between voters (50% + 1).
     *    有多于一般的 Sentinel 支持
     * 2) And anyway at least master->quorum votes.
     *    投票数至少要有 master->quorum 那么多
     */
    di = dictGetIterator(counters);
    while((de = dictNext(di)) != NULL) {

        // 取出票数
        uint64_t votes = dictGetUnsignedIntegerVal(de);

        // 选出票数最大的人
        if (votes > max_votes) {
            max_votes = votes;
            winner = dictGetKey(de);
        }
    }
    dictReleaseIterator(di);

    /* Count this Sentinel vote:
     * if this Sentinel did not voted yet, either vote for the most
     * common voted sentinel, or for itself if no vote exists at all. */
    // 本 Sentinel 进行投票
    // 如果 Sentinel 之前还没有进行投票，那么有两种选择：
    // 1）如果选出了 winner （最多票数支持的 Sentinel ），那么这个 Sentinel 也投 winner 一票
    // 2）如果没有选出 winner ，那么 Sentinel 投自己一票
    if (winner)
        myvote = sentinelVoteLeader(master,epoch,winner,&leader_epoch);
    else
        myvote = sentinelVoteLeader(master,epoch,sentinel.myid,&leader_epoch);

    // 领头 Sentinel 已选出，并且领头的纪元和给定的纪元一样
    if (myvote && leader_epoch == epoch) {

        // 为领头 Sentinel 增加一票（这一票来自本 Sentinel ）
        uint64_t votes = sentinelLeaderIncr(counters,myvote);

        // 如果投票之后的票数比最大票数要大，那么更换领头 Sentinel
        if (votes > max_votes) {
            max_votes = votes;
            winner = myvote;
        }
    }

    // 如果支持领头的投票数量不超过半数
    // 并且支持票数不超过 master 配置指定的投票数量
    // 那么这次领头选举无效
    voters_quorum = voters/2+1;
    if (winner && (max_votes < voters_quorum || max_votes < master->quorum))
        winner = NULL;

    // 返回领头 Sentinel ，或者 NULL
    winner = winner ? sdsnew(winner) : NULL;
    sdsfree(myvote);
    dictRelease(counters);
    return winner;
}

/* Send SLAVEOF to the specified instance, always followed by a
 * CONFIG REWRITE command in order to store the new configuration on disk
 * when possible (that is, if the Redis instance is recent enough to support
 * config rewriting, and if the server was started with a configuration file).
 *
 * 向指定实例发送 SLAVEOF 命令，并在可能时，执行 CONFIG REWRITE 命令，
 * 将当前配置保存到磁盘中。
 *
 * If Host is NULL the function sends "SLAVEOF NO ONE".
 *
 * 如果 host 参数为 NULL ，那么向实例发送 SLAVEOF NO ONE 命令
 *
 * The command returns C_OK if the SLAVEOF command was accepted for
 * (later) delivery otherwise C_ERR. The command replies are just
 * discarded.
 * 命令入队成功（异步发送）时，函数返回 REDIS_OK ，
 * 入队失败时返回 REDIS_ERR ，
 * 命令回复会被丢弃。
 */
int sentinelSendSlaveOf(sentinelRedisInstance *ri, const sentinelAddr *addr) {
    char portstr[32];
    const char *host;
    int retval;

    /* If host is NULL we send SLAVEOF NO ONE that will turn the instance
    * into a master. */
    if (!addr) {
        host = "NO";
        memcpy(portstr,"ONE",4);
    } else {
        host = announceSentinelAddr(addr);
        ll2string(portstr,sizeof(portstr),addr->port);
    }

    /* In order to send SLAVEOF in a safe way, we send a transaction performing
     * the following tasks:
     * 1) Reconfigure the instance according to the specified host/port params.
     * 2) Rewrite the configuration.
     * 3) Disconnect all clients (but this one sending the command) in order
     *    to trigger the ask-master-on-reconnection protocol for connected
     *    clients.
     *
     * Note that we don't check the replies returned by commands, since we
     * will observe instead the effects in the next INFO output. */
    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s",
                               sentinelInstanceMapCommand(ri,"MULTI"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s %s %s",
                               sentinelInstanceMapCommand(ri,"SLAVEOF"),
                               host, portstr);
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s REWRITE",
                               sentinelInstanceMapCommand(ri,"CONFIG"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    /* CLIENT KILL TYPE <type> is only supported starting from Redis 2.8.12,
     * however sending it to an instance not understanding this command is not
     * an issue because CLIENT is variadic command, so Redis will not
     * recognized as a syntax error, and the transaction will not fail (but
     * only the unsupported command will fail). */
    for (int type = 0; type < 2; type++) {
        retval = redisAsyncCommand(ri->link->cc,
                                   sentinelDiscardReplyCallback, ri, "%s KILL TYPE %s",
                                   sentinelInstanceMapCommand(ri,"CLIENT"),
                                   type == 0 ? "normal" : "pubsub");
        if (retval == C_ERR) return retval;
        ri->link->pending_commands++;
    }

    retval = redisAsyncCommand(ri->link->cc,
                               sentinelDiscardReplyCallback, ri, "%s",
                               sentinelInstanceMapCommand(ri,"EXEC"));
    if (retval == C_ERR) return retval;
    ri->link->pending_commands++;

    return C_OK;
}

/* Setup the master state to start a failover. */
// 设置主服务器的状态，开始一次故障转移
void sentinelStartFailover(sentinelRedisInstance *master) {
    serverAssert(master->flags & SRI_MASTER);

    // 更新故障转移状态
    master->failover_state = SENTINEL_FAILOVER_STATE_WAIT_START;

    // 更新主服务器状态
    master->flags |= SRI_FAILOVER_IN_PROGRESS;

    // 更新纪元
    master->failover_epoch = ++sentinel.current_epoch;
    sentinelEvent(LL_WARNING,"+new-epoch",master,"%llu",
                  (unsigned long long) sentinel.current_epoch);
    sentinelEvent(LL_WARNING,"+try-failover",master,"%@");
    // 记录故障转移状态的变更时间
    master->failover_start_time = mstime()+rand()%SENTINEL_MAX_DESYNC;
    master->failover_state_change_time = mstime();
}

/* This function checks if there are the conditions to start the failover,
 * that is:
 *
 * 这个函数检查是否需要开始一次故障转移操作：
 *
 * 1) Master must be in ODOWN condition.
 *    主服务器已经计入 ODOWN 状态。
 * 2) No failover already in progress.
 *    当前没有针对同一主服务器的故障转移操作在执行。
 * 3) No failover already attempted recently.
 *    最近时间内，这个主服务器没有尝试过执行故障转移
 *    （应该是为了防止频繁执行）。
 *
 * We still don't know if we'll win the election so it is possible that we
 * start the failover but that we'll not be able to act.
 *
 * 虽然 Sentinel 可以发起一次故障转移，但因为故障转移操作是由领头 Sentinel 执行的，
 * 所以发起故障转移的 Sentinel 不一定就是执行故障转移的 Sentinel 。
 *
 * Return non-zero if a failover was started.
 *
 * 如果故障转移操作成功开始，那么函数返回非 0 值。
 */
int sentinelStartFailoverIfNeeded(sentinelRedisInstance *master) {
    /* We can't failover if the master is not in O_DOWN state. */
    if (!(master->flags & SRI_O_DOWN)) return 0;

    /* Failover already in progress? */
    if (master->flags & SRI_FAILOVER_IN_PROGRESS) return 0;

    /* Last failover attempt started too little time ago? */
    if (mstime() - master->failover_start_time <
    master->failover_timeout*2)
    {
        if (master->failover_delay_logged != master->failover_start_time) {
            time_t clock = (master->failover_start_time +
                    master->failover_timeout*2) / 1000;
            char ctimebuf[26];

            ctime_r(&clock,ctimebuf);
            ctimebuf[24] = '\0'; /* Remove newline. */
            master->failover_delay_logged = master->failover_start_time;
            serverLog(LL_WARNING,
                      "Next failover delay: I will not start a failover before %s",
                      ctimebuf);
        }
        return 0;
    }

    // 开始一次故障转移
    sentinelStartFailover(master);
    return 1;
}

/* Select a suitable slave to promote. The current algorithm only uses
 * the following parameters:
 *
 * 在多个从服务器中选出一个作为新的主服务器。
 * 算法使用以下参数：
 *
 * 1) None of the following conditions: S_DOWN, O_DOWN, DISCONNECTED.
 *    带有 S_DOWN 、 O_DOWN 和 DISCONNECTED 状态的从服务器不会被选中。
 * 2) Last time the slave replied to ping no more than 5 times the PING period.
 *    距离最近一次回复 PING 命令超过 5 秒以上的从服务区不会被选中。
 * 3) info_refresh not older than 3 times the INFO refresh period.
 *    距离最后一次回复 INFO 命令的时间超过 info_refresh 时长三倍的从服务器不会被考虑。
 * 4) master_link_down_time no more than:
 *    主从服务器之间的连接断开时间不能超过：
 *     (now - master->s_down_since_time) + (master->down_after_period * 10).
 *    Basically since the master is down from our POV, the slave reports
 *    to be disconnected no more than 10 times the configured down-after-period.
 *    因为从当前 Sentinel 来看，主服务器已经处于下线状态，
 *    所以正常来说，
 *    从服务器与主服务器之间的连接断开时间不应该超过 down-after-period 的十倍。
 *    This is pretty much black magic but the idea is, the master was not
 *    available so the slave may be lagging, but not over a certain time.
 *    这听上去有点像黑魔法，不过这个判断的主意是这样的：
 *    当主服务器下线之后，主从服务器的连接就会断开，但只要先下线的是主服务器而不是从服务器
 *    （换句话说，连接断开是由主服务器而不是从服务器造成的）
 *    那么主从服务器之间的连接断开时间就不会太长。
 *    Anyway we'll select the best slave according to replication offset.
 *    不过这只是一个辅助手段，因为最终我们都会使用复制偏移量来挑选从服务器。
 * 5) Slave priority can't be zero, otherwise the slave is discarded.
 *    从服务器的优先级不能为 0 ，优先级为 0 的从服务器表示被禁用。
 *
 * Among all the slaves matching the above conditions we select the slave
 * with, in order of sorting key:
 *
 * 符合以上条件的从服务器，我们会按以下条件对从服务器进行排序：
 *
 * - lower slave_priority.
 *   优先级更小的从服务器优先。
 * - bigger processed replication offset.
 *   复制偏移量较大的从服务器优先。
 * - lexicographically smaller runid.
 *   运行 ID 较小的从服务器优先。
 *
 * Basically if runid is the same, the slave that processed more commands
 * from the master is selected.
 *
 * 如果运行 ID 相同，那么执行命令数量较多的那个从服务器会被选中。
 *
 * The function returns the pointer to the selected slave, otherwise
 * NULL if no suitable slave was found.
 *
 * sentinelSelectSlave 函数返回被选中从服务器的实例指针，
 * 如果没有何时的从服务器，那么返回 NULL 。
 */

/* Helper for sentinelSelectSlave(). This is used by qsort() in order to
 * sort suitable slaves in a "better first" order, to take the first of
 * the list. */
// 排序函数，用于选出更好的从服务器
int compareSlavesForPromotion(const void *a, const void *b) {
    sentinelRedisInstance **sa = (sentinelRedisInstance **)a,
    **sb = (sentinelRedisInstance **)b;
    char *sa_runid, *sb_runid;

    // 优先级较小的从服务器胜出
    if ((*sa)->slave_priority != (*sb)->slave_priority)
        return (*sa)->slave_priority - (*sb)->slave_priority;

    /* If priority is the same, select the slave with greater replication
     * offset (processed more data from the master). */
    // 如果优先级相同，那么复制偏移量较大的那个从服务器胜出
    // （偏移量较大表示从主服务器获取的数据更多，更完整）
    if ((*sa)->slave_repl_offset > (*sb)->slave_repl_offset) {
        return -1; /* a < b */
    } else if ((*sa)->slave_repl_offset < (*sb)->slave_repl_offset) {
        return 1; /* a > b */
    }

    /* If the replication offset is the same select the slave with that has
     * the lexicographically smaller runid. Note that we try to handle runid
     * == NULL as there are old Redis versions that don't publish runid in
     * INFO. A NULL runid is considered bigger than any other runid. */
    // 如果复制偏移量也相同，那么选出运行 ID 较低的那个从服务器
    // 注意，对于没有运行 ID 的旧版 Redis 来说，默认的运行 ID 为 NULL
    sa_runid = (*sa)->runid;
    sb_runid = (*sb)->runid;
    if (sa_runid == NULL && sb_runid == NULL) return 0;
    else if (sa_runid == NULL) return 1;  /* a > b */
    else if (sb_runid == NULL) return -1; /* a < b */
    return strcasecmp(sa_runid, sb_runid);
}

// 从主服务器的所有从服务器中，挑选一个作为新的主服务器
// 如果没有合格的新主服务器，那么返回 NULL
sentinelRedisInstance *sentinelSelectSlave(sentinelRedisInstance *master) {

    sentinelRedisInstance **instance =
            zmalloc(sizeof(instance[0])*dictSize(master->slaves));
    sentinelRedisInstance *selected = NULL;
    int instances = 0;
    dictIterator *di;
    dictEntry *de;
    mstime_t max_master_down_time = 0;

    // 计算可以接收的，从服务器与主服务器之间的最大下线时间
    // 这个值可以保证被选中的从服务器的数据库不会太旧
    if (master->flags & SRI_S_DOWN)
        max_master_down_time += mstime() - master->s_down_since_time;
    max_master_down_time += master->down_after_period * 10;

    // 遍历所有从服务器
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {

        // 从服务器实例
        sentinelRedisInstance *slave = dictGetVal(de);
        mstime_t info_validity_time;

        // 忽略所有 SDOWN 、ODOWN 或者已断线的从服务器
        if (slave->flags & (SRI_S_DOWN|SRI_O_DOWN)) continue;
        if (slave->link->disconnected) continue;
        if (mstime() - slave->link->last_avail_time > SENTINEL_PING_PERIOD*5) continue;
        if (slave->slave_priority == 0) continue;

        /* If the master is in SDOWN state we get INFO for slaves every second.
         * Otherwise we get it with the usual period so we need to account for
         * a larger delay. */
        // 如果主服务器处于 SDOWN 状态，那么 Sentinel 以每秒一次的频率向从服务器发送 INFO 命令
        // 否则以平常频率向从服务器发送 INFO 命令
        // 这里要检查 INFO 命令的返回值是否合法，检查的时间会乘以一个倍数，以计算延迟
        if (master->flags & SRI_S_DOWN)
            info_validity_time = SENTINEL_PING_PERIOD*5;
        else
            info_validity_time = SENTINEL_INFO_PERIOD*3;
        // INFO 回复已过期，不考虑
        if (mstime() - slave->info_refresh > info_validity_time) continue;

        // 从服务器下线的时间过长，不考虑
        if (slave->master_link_down_time > max_master_down_time) continue;

        // 将被选中的 slave 保存到数组中
        instance[instances++] = slave;
    }
    dictReleaseIterator(di);

    if (instances) {

        // 对被选中的从服务器进行排序
        qsort(instance,instances,sizeof(sentinelRedisInstance*),
              compareSlavesForPromotion);

        // 分值最低的从服务器为被选中服务器
        selected = instance[0];
    }
    zfree(instance);

    // 返回被选中的从服务区
    return selected;
}

/* ---------------- Failover state machine implementation ------------------- */

// 准备执行故障转移
void sentinelFailoverWaitStart(sentinelRedisInstance *ri) {
    char *leader;
    int isleader;

    /* Check if we are the leader for the failover epoch. */
    // 获取给定纪元的领头 Sentinel
    leader = sentinelGetLeader(ri, ri->failover_epoch);

    // 本 Sentinel 是否为领头 Sentinel ？
    isleader = leader && strcasecmp(leader,sentinel.myid) == 0;
    sdsfree(leader);

    /* If I'm not the leader, and it is not a forced failover via
     * SENTINEL FAILOVER, then I can't continue with the failover. */
    // 如果本 Sentinel 不是领头，并且这次故障迁移不是一次强制故障迁移操作
    // 那么本 Sentinel 不做动作
    if (!isleader && !(ri->flags & SRI_FORCE_FAILOVER)) {
        int election_timeout = SENTINEL_ELECTION_TIMEOUT;

        /* The election timeout is the MIN between SENTINEL_ELECTION_TIMEOUT
         * and the configured failover timeout. */
        // 当选的时长（类似于任期）是 SENTINEL_ELECTION_TIMEOUT
        // 和 Sentinel 设置的故障迁移时长之间的较小那个值
        if (election_timeout > ri->failover_timeout)
            election_timeout = ri->failover_timeout;

        /* Abort the failover if I'm not the leader after some time. */
        // Sentinel 的当选时间已过，取消故障转移计划
        if (mstime() - ri->failover_start_time > election_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-not-elected",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    // 本 Sentinel 作为领头，开始执行故障迁移操作...

    sentinelEvent(LL_WARNING,"+elected-leader",ri,"%@");
    if (sentinel.simfailure_flags & SENTINEL_SIMFAILURE_CRASH_AFTER_ELECTION)
        sentinelSimFailureCrash();
    // 进入选择从服务器状态
    ri->failover_state = SENTINEL_FAILOVER_STATE_SELECT_SLAVE;
    ri->failover_state_change_time = mstime();
    sentinelEvent(LL_WARNING,"+failover-state-select-slave",ri,"%@");
}

// 选择合适的从服务器作为新的主服务器
void sentinelFailoverSelectSlave(sentinelRedisInstance *ri) {

    // 在旧主服务器所属的从服务器中，选择新服务器
    sentinelRedisInstance *slave = sentinelSelectSlave(ri);

    /* We don't handle the timeout in this state as the function aborts
     * the failover or go forward in the next state. */
    // 没有合适的从服务器，直接终止故障转移操作
    if (slave == NULL) {
        // 没有可用的从服务器可以提升为新主服务器，故障转移操作无法执行
        sentinelEvent(LL_WARNING,"-failover-abort-no-good-slave",ri,"%@");
        // 中止故障转移
        sentinelAbortFailover(ri);
    } else {

        // 成功选定新主服务器
        // 发送事件
        sentinelEvent(LL_WARNING,"+selected-slave",slave,"%@");
        // 打开实例的升级标记
        slave->flags |= SRI_PROMOTED;

        // 记录被选中的从服务器
        ri->promoted_slave = slave;

        // 更新故障转移状态
        ri->failover_state = SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE;

        // 更新状态改变时间
        ri->failover_state_change_time = mstime();

        // 发送事件
        sentinelEvent(LL_NOTICE,"+failover-state-send-slaveof-noone",
                      slave, "%@");
    }
}

// 向被选中的从服务器发送 SLAVEOF no one 命令
// 将它升级为新的主服务器
void sentinelFailoverSendSlaveOfNoOne(sentinelRedisInstance *ri) {
    int retval;

    /* We can't send the command to the promoted slave if it is now
     * disconnected. Retry again and again with this state until the timeout
     * is reached, then abort the failover. */

    // 如果选中的从服务器断线了，那么在给定的时间内重试
    // 如果给定时间内选中的从服务器也没有上线，那么终止故障迁移操作
    // （一般来说出现这种情况的机会很小，因为在选择新的主服务器时，
    // 已经断线的从服务器是不会被选中的，所以这种情况只会出现在
    // 从服务器被选中，并且发送 SLAVEOF NO ONE 命令之前的这段时间内）
    if (ri->promoted_slave->link->disconnected) {
        // 如果超过时限，就不再重试
        if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
            sentinelEvent(LL_WARNING,"-failover-abort-slave-timeout",ri,"%@");
            sentinelAbortFailover(ri);
        }
        return;
    }

    /* Send SLAVEOF NO ONE command to turn the slave into a master.
     *
     * 向被升级的从服务器发送 SLAVEOF NO ONE 命令，将它变为一个主服务器。
     *
     * We actually register a generic callback for this command as we don't
     * really care about the reply. We check if it worked indirectly observing
     * if INFO returns a different role (master instead of slave).
     *
     * 这里没有为命令回复关联一个回调函数，因为从服务器是否已经转变为主服务器可以
     * 通过向从服务器发送 INFO 命令来确认
     */
    retval = sentinelSendSlaveOf(ri->promoted_slave,NULL);
    if (retval != C_OK) return;
    sentinelEvent(LL_NOTICE, "+failover-state-wait-promotion",
                  ri->promoted_slave,"%@");

    // 更新状态
    // 这个状态会让 Sentinel 等待被选中的从服务器升级为主服务器
    ri->failover_state = SENTINEL_FAILOVER_STATE_WAIT_PROMOTION;
    // 更新状态改变的时间
    ri->failover_state_change_time = mstime();
}

/* We actually wait for promotion indirectly checking with INFO when the
 * slave turns into a master. */
// Sentinel 会通过 INFO 命令的回复检查从服务器是否已经转变为主服务器
// 这里只负责检查时限
void sentinelFailoverWaitPromotion(sentinelRedisInstance *ri) {
    /* Just handle the timeout. Switching to the next state is handled
     * by the function parsing the INFO command of the promoted slave. */
    if (mstime() - ri->failover_state_change_time > ri->failover_timeout) {
        sentinelEvent(LL_WARNING,"-failover-abort-slave-timeout",ri,"%@");
        sentinelAbortFailover(ri);
    }
}

// 判断故障转移操作是否结束
// 结束可以因为超时，也可以因为所有从服务器已经同步到新主服务器
void sentinelFailoverDetectEnd(sentinelRedisInstance *master) {
    int not_reconfigured = 0, timeout = 0;
    dictIterator *di;
    dictEntry *de;

    // 上次 failover 状态更新以来，经过的时间
    mstime_t elapsed = mstime() - master->failover_state_change_time;

    /* We can't consider failover finished if the promoted slave is
     * not reachable. */
    // 如果新主服务器已经下线，那么故障转移操作不成功
    if (master->promoted_slave == NULL ||
    master->promoted_slave->flags & SRI_S_DOWN) return;

    /* The failover terminates once all the reachable slaves are properly
     * configured. */
    // 计算未完成同步的从服务器的数量
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // 新主服务器和已完成同步的从服务器不计算在内
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        // 已下线的从服务器不计算在内
        if (slave->flags & SRI_S_DOWN) continue;

        // 增一
        not_reconfigured++;
    }
    dictReleaseIterator(di);

    /* Force end of failover on timeout. */
    // 故障操作因为超时而结束
    if (elapsed > master->failover_timeout) {
        // 忽略未完成的从服务器
        not_reconfigured = 0;
        // 打开超时标志
        timeout = 1;
        // 发送超时事件
        sentinelEvent(LL_WARNING,"+failover-end-for-timeout",master,"%@");
    }

    // 所有从服务器都已完成同步，故障转移结束
    if (not_reconfigured == 0) {
        sentinelEvent(LL_WARNING,"+failover-end",master,"%@");
        // 更新故障转移状态
        // 这一状态将告知 Sentinel ，所有从服务器都已经同步到新主服务器
        master->failover_state = SENTINEL_FAILOVER_STATE_UPDATE_CONFIG;
        // 更新状态改变的时间
        master->failover_state_change_time = mstime();
    }

    /* If I'm the leader it is a good idea to send a best effort SLAVEOF
     * command to all the slaves still not reconfigured to replicate with
     * the new master. */
    if (timeout) {
        dictIterator *di;
        dictEntry *de;

        // 遍历所有从服务器
        di = dictGetIterator(master->slaves);
        while((de = dictNext(di)) != NULL) {
            sentinelRedisInstance *slave = dictGetVal(de);
            int retval;

            // 跳过已发送 SLAVEOF 命令，以及已经完成同步的所有从服务器
            if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE|SRI_RECONF_SENT)) continue;
            if (slave->link->disconnected) continue;
            // 发送命令
            retval = sentinelSendSlaveOf(slave,master->promoted_slave->addr);
            if (retval == C_OK) {
                sentinelEvent(LL_NOTICE,"+slave-reconf-sent-be",slave,"%@");
                // 打开从服务器的 SLAVEOF 命令已发送标记
                slave->flags |= SRI_RECONF_SENT;
            }
        }
        dictReleaseIterator(di);
    }
}

/* Send SLAVE OF <new master address> to all the remaining slaves that
 * still don't appear to have the configuration updated. */
// 向所有尚未同步新主服务器的从服务器发送 SLAVEOF <new-master-address> 命令
void sentinelFailoverReconfNextSlave(sentinelRedisInstance *master) {
    dictIterator *di;
    dictEntry *de;
    int in_progress = 0;

    // 计算正在同步新主服务器的从服务器数量
    di = dictGetIterator(master->slaves);
    while((de = dictNext(di)) != NULL) {
        sentinelRedisInstance *slave = dictGetVal(de);

        // SLAVEOF 命令已发送，或者同步正在进行
        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG))
            in_progress++;
    }
    dictReleaseIterator(di);

    // 如果正在同步的从服务器的数量少于 parallel-syncs 选项的值
    // 那么继续遍历从服务器，并让从服务器对新主服务器进行同步
    di = dictGetIterator(master->slaves);
    while(in_progress < master->parallel_syncs &&
    (de = dictNext(di)) != NULL)
    {
        sentinelRedisInstance *slave = dictGetVal(de);
        int retval;

        /* Skip the promoted slave, and already configured slaves. */
        // 跳过新主服务器，以及已经完成了同步的从服务器
        if (slave->flags & (SRI_PROMOTED|SRI_RECONF_DONE)) continue;

        /* If too much time elapsed without the slave moving forward to
         * the next state, consider it reconfigured even if it is not.
         * Sentinels will detect the slave as misconfigured and fix its
         * configuration later. */
        if ((slave->flags & SRI_RECONF_SENT) &&
        (mstime() - slave->slave_reconf_sent_time) >
        SENTINEL_SLAVE_RECONF_TIMEOUT)
        {
            // 发送重拾同步事件
            sentinelEvent(LL_NOTICE,"-slave-reconf-sent-timeout",slave,"%@");
            // 清除已发送 SLAVEOF 命令的标记
            slave->flags &= ~SRI_RECONF_SENT;
            slave->flags |= SRI_RECONF_DONE;
        }

        /* Nothing to do for instances that are disconnected or already
         * in RECONF_SENT state. */
        // 如果已向从服务器发送 SLAVEOF 命令，或者同步正在进行
        // 又或者从服务器已断线，那么略过该服务器
        if (slave->flags & (SRI_RECONF_SENT|SRI_RECONF_INPROG)) continue;
        if (slave->link->disconnected) continue;

        /* Send SLAVEOF <new master>. */
        // 向从服务器发送 SLAVEOF 命令，让它同步新主服务器
        retval = sentinelSendSlaveOf(slave,master->promoted_slave->addr);
        if (retval == C_OK) {
            // 将状态改为 SLAVEOF 命令已发送
            slave->flags |= SRI_RECONF_SENT;
            // 更新发送 SLAVEOF 命令的时间
            slave->slave_reconf_sent_time = mstime();
            sentinelEvent(LL_NOTICE,"+slave-reconf-sent",slave,"%@");
            // 增加当前正在同步的从服务器的数量
            in_progress++;
        }
    }
    dictReleaseIterator(di);

    /* Check if all the slaves are reconfigured and handle timeout. */
    // 判断是否所有从服务器的同步都已经完成
    sentinelFailoverDetectEnd(master);
}

/* This function is called when the slave is in
 * SENTINEL_FAILOVER_STATE_UPDATE_CONFIG state. In this state we need
 * to remove it from the master table and add the promoted slave instead. */
// 这个函数在 master 已下线，并且对这个 master 的故障迁移操作已经完成时调用
// 这个 master 会被移除出 master 表格，并由新的主服务器代替
void sentinelFailoverSwitchToPromotedSlave(sentinelRedisInstance *master) {
    /// 选出要添加的 master
    sentinelRedisInstance *ref = master->promoted_slave ?
            master->promoted_slave : master;

    // 发送更新 master 事件
    sentinelEvent(LL_WARNING,"+switch-master",master,"%s %s %d %s %d",
                  // 原 master 信息
                  master->name, announceSentinelAddr(master->addr), master->addr->port,
                  // 新 master 信息
                  announceSentinelAddr(ref->addr), ref->addr->port);
    // 用新主服务器的信息代替原 master 的信息
    sentinelResetMasterAndChangeAddress(master,ref->addr->hostname,ref->addr->port);
}

// 执行故障转移
void sentinelFailoverStateMachine(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_MASTER);

    // master 未进入故障转移状态，直接返回
    if (!(ri->flags & SRI_FAILOVER_IN_PROGRESS)) return;

    switch(ri->failover_state) {

        // 等待故障转移开始
        case SENTINEL_FAILOVER_STATE_WAIT_START:
            sentinelFailoverWaitStart(ri);
            break;

            // 选择新主服务器
            case SENTINEL_FAILOVER_STATE_SELECT_SLAVE:
                sentinelFailoverSelectSlave(ri);
                break;

                // 升级被选中的从服务器为新主服务器
                case SENTINEL_FAILOVER_STATE_SEND_SLAVEOF_NOONE:
                    sentinelFailoverSendSlaveOfNoOne(ri);
                    break;

                    // 等待升级生效，如果升级超时，那么重新选择新主服务器
                    // 具体情况请看 sentinelRefreshInstanceInfo 函数
                    case SENTINEL_FAILOVER_STATE_WAIT_PROMOTION:
                        sentinelFailoverWaitPromotion(ri);
                        break;

                        // 向从服务器发送 SLAVEOF 命令，让它们同步新主服务器
                        case SENTINEL_FAILOVER_STATE_RECONF_SLAVES:
                            sentinelFailoverReconfNextSlave(ri);
                            break;
    }
}

/* Abort a failover in progress:
 *
 * 关于中途停止执行故障转移：
 *
 * This function can only be called before the promoted slave acknowledged
 * the slave -> master switch. Otherwise the failover can't be aborted and
 * will reach its end (possibly by timeout).
 *
 * 这个函数只能在被选中的从服务器升级为新的主服务器之前调用，
 * 否则故障转移就不能中途停止，
 * 并且会一直执行到结束。
 */
void sentinelAbortFailover(sentinelRedisInstance *ri) {
    serverAssert(ri->flags & SRI_FAILOVER_IN_PROGRESS);
    serverAssert(ri->failover_state <= SENTINEL_FAILOVER_STATE_WAIT_PROMOTION);

    // 移除相关标识
    ri->flags &= ~(SRI_FAILOVER_IN_PROGRESS|SRI_FORCE_FAILOVER);

    // 清除状态
    ri->failover_state = SENTINEL_FAILOVER_STATE_NONE;
    ri->failover_state_change_time = mstime();

    // 清除新主服务器的升级标识
    if (ri->promoted_slave) {
        ri->promoted_slave->flags &= ~SRI_PROMOTED;
        // 清空新服务器
        ri->promoted_slave = NULL;
    }
}

/* ======================== SENTINEL timer handler ==========================
 * This is the "main" our Sentinel, being sentinel completely non blocking
 * in design. The function is called every second.
 * -------------------------------------------------------------------------- */

/* Perform scheduled operations for the specified Redis instance. */
// 对给定的实例执行定期操作
void sentinelHandleRedisInstance(sentinelRedisInstance *ri) {

    /* ========== MONITORING HALF ============ */
    /* ==========     监控操作    =========*/

    /* Every kind of instance */
    /* 对所有类型实例进行处理 */

    // 如果有需要的话，创建连向实例的网络连接
    sentinelReconnectInstance(ri);

    // 根据情况，向实例发送 PING、 INFO 或者 PUBLISH 命令
    sentinelSendPeriodicCommands(ri);

    /* ============== ACTING HALF ============= */
    /* ==============  故障检测   ============= */

    /* We don't proceed with the acting half if we are in TILT mode.
     * TILT happens when we find something odd with the time, like a
     * sudden change in the clock. */
    // 如果 Sentinel 处于 TILT 模式，那么不执行故障检测。
    if (sentinel.tilt) {

        // 如果 TILI 模式未解除，那么不执行动作
        if (mstime()-sentinel.tilt_start_time < SENTINEL_TILT_PERIOD) return;

        // 时间已过，退出 TILT 模式
        sentinel.tilt = 0;
        sentinelEvent(LL_WARNING,"-tilt",NULL,"#tilt mode exited");
    }

    /* Every kind of instance */
    // 检查给定实例是否进入 SDOWN 状态
    sentinelCheckSubjectivelyDown(ri);

    /* Masters and slaves */
    if (ri->flags & (SRI_MASTER|SRI_SLAVE)) {
        /* Nothing so far. */
    }

    /* Only masters */
    /* 对主服务器进行处理 */
    if (ri->flags & SRI_MASTER) {

        // 判断 master 是否进入 ODOWN 状态
        sentinelCheckObjectivelyDown(ri);

        // 如果主服务器进入了 ODOWN 状态，那么开始一次故障转移操作
        if (sentinelStartFailoverIfNeeded(ri))
            // 强制向其他 Sentinel 发送 SENTINEL is-master-down-by-addr 命令
            // 刷新其他 Sentinel 关于主服务器的状态
            sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_ASK_FORCED);

        // 执行故障转移
        sentinelFailoverStateMachine(ri);

        // 如果有需要的话，向其他 Sentinel 发送 SENTINEL is-master-down-by-addr 命令
        // 刷新其他 Sentinel 关于主服务器的状态
        // 这一句是对那些没有进入 if(sentinelStartFailoverIfNeeded(ri)) { /* ... */ }
        // 语句的主服务器使用的
        sentinelAskMasterStateToOtherSentinels(ri,SENTINEL_NO_FLAGS);
    }
}

/* Perform scheduled operations for all the instances in the dictionary.
 * Recursively call the function against dictionaries of slaves. */
// 对被 Sentinel 监视的所有实例（包括主服务器、从服务器和其他 Sentinel ）
// 进行定期操作
//
//  sentinelHandleRedisInstance
//              |
//              |
//              v
//            master
//             /  \
//            /    \
//           v      v
//       slaves    sentinels
void sentinelHandleDictOfRedisInstances(dict *instances) {
    dictIterator *di;
    dictEntry *de;
    sentinelRedisInstance *switch_to_promoted = NULL;

    /* There are a number of things we need to perform against every master. */
    // 遍历多个实例，这些实例可以是多个主服务器、多个从服务器或者多个 sentinel
    di = dictGetIterator(instances);
    while((de = dictNext(di)) != NULL) {

        // 取出实例对应的实例结构
        sentinelRedisInstance *ri = dictGetVal(de);

        // 执行调度操作
        sentinelHandleRedisInstance(ri);

        // 如果被遍历的是主服务器，那么递归地遍历该主服务器的所有从服务器
        // 以及所有 sentinel
        if (ri->flags & SRI_MASTER) {

            // 所有从服务器
            sentinelHandleDictOfRedisInstances(ri->slaves);

            // 所有 sentinel
            sentinelHandleDictOfRedisInstances(ri->sentinels);

            // 对已下线主服务器（ri）的故障迁移已经完成
            // ri 的所有从服务器都已经同步到新主服务器
            if (ri->failover_state == SENTINEL_FAILOVER_STATE_UPDATE_CONFIG) {
                // 已选出新的主服务器
                switch_to_promoted = ri;
            }
        }
    }
    // 将原主服务器（已下线）从主服务器表格中移除，并使用新主服务器代替它
    if (switch_to_promoted)
        sentinelFailoverSwitchToPromotedSlave(switch_to_promoted);
    dictReleaseIterator(di);
}

/* This function checks if we need to enter the TITL mode.
 *
 * 这个函数检查 sentinel 是否需要进入 TITL 模式。
 *
 * The TILT mode is entered if we detect that between two invocations of the
 * timer interrupt, a negative amount of time, or too much time has passed.
 *
 * 当程序发现两次执行 sentinel 之间的时间差为负值，或者过大时，
 * 就会进入 TILT 模式。
 *
 * Note that we expect that more or less just 100 milliseconds will pass
 * if everything is fine. However we'll see a negative number or a
 * difference bigger than SENTINEL_TILT_TRIGGER milliseconds if one of the
 * following conditions happen:
 *
 * 通常来说，两次执行 sentinel 之间的差会在 100 毫秒左右，
 * 但当出现以下内容时，这个差就可能会出现异常：
 *
 * 1) The Sentinel process for some time is blocked, for every kind of
 * random reason: the load is huge, the computer was frozen for some time
 * in I/O or alike, the process was stopped by a signal. Everything.
 *    sentinel 进程因为某些原因而被阻塞，比如载入的数据太大，计算机 I/O 任务繁重，
 *    进程被信号停止，诸如此类。
 *
 * 2) The system clock was altered significantly.
 *    系统的时钟产生了非常明显的变化。
 *
 * Under both this conditions we'll see everything as timed out and failing
 * without good reasons. Instead we enter the TILT mode and wait
 * for SENTINEL_TILT_PERIOD to elapse before starting to act again.
 *
 * 当出现以上两种情况时， sentinel 可能会将任何实例都视为掉线，并无原因地判断实例为失效。
 * 为了避免这种情况，我们让 sentinel 进入 TILT 模式，
 * 停止进行任何动作，并等待 SENTINEL_TILT_PERIOD 秒钟。
 *
 * During TILT time we still collect information, we just do not act.
 *
 * TILT 模式下的 sentinel 仍然会进行监控并收集信息，
 * 它只是不执行诸如故障转移、下线判断之类的操作而已。
 */
void sentinelCheckTiltCondition(void) {

    // 计算当前时间
    mstime_t now = mstime();

    // 计算上次运行 sentinel 和当前时间的差
    mstime_t delta = now - sentinel.previous_time;

    // 如果差为负数，或者大于 2 秒钟，那么进入 TILT 模式
    if (delta < 0 || delta > SENTINEL_TILT_TRIGGER) {
        // 打开标记
        sentinel.tilt = 1;
        // 记录进入 TILT 模式的开始时间
        sentinel.tilt_start_time = mstime();
        // 打印事件
        sentinelEvent(LL_WARNING,"+tilt",NULL,"#tilt mode entered");
    }
    // 更新最后一次 sentinel 运行时间
    sentinel.previous_time = mstime();
}

// sentinel 模式的主函数，由 redis.c/serverCron 函数调用
void sentinelTimer(void) {

    // 记录本次 sentinel 调用的事件，
    // 并判断是否需要进入 TITL 模式
    sentinelCheckTiltCondition();

    // 执行定期操作
    // 比如 PING 实例、分析主服务器和从服务器的 INFO 命令
    // 向其他监视相同主服务器的 sentinel 发送问候信息
    // 并接收其他 sentinel 发来的问候信息
    // 执行故障转移操作，等等
    sentinelHandleDictOfRedisInstances(sentinel.masters);

    // 运行等待执行的脚本
    sentinelRunPendingScripts();

    // 清理已执行完毕的脚本，并重试出错的脚本
    sentinelCollectTerminatedScripts();

    // 杀死运行超时的脚本
    sentinelKillTimedoutScripts();

    /* We continuously change the frequency of the Redis "timer interrupt"
     * in order to desynchronize every Sentinel from every other.
     * This non-determinism avoids that Sentinels started at the same time
     * exactly continue to stay synchronized asking to be voted at the
     * same time again and again (resulting in nobody likely winning the
     * election because of split brain voting). */
    server.hz = CONFIG_DEFAULT_HZ + rand() % CONFIG_DEFAULT_HZ;
}
