/* Extracted from anet.c to work properly with Hiredis error reporting.
 *
 * Copyright (c) 2009-2011, Salvatore Sanfilippo <antirez at gmail dot com>
 * Copyright (c) 2010-2014, Pieter Noordhuis <pcnoordhuis at gmail dot com>
 * Copyright (c) 2015, Matt Stancliff <matt at genges dot com>,
 *                     Jan-Erik Rediger <janerik at fnordig dot com>
 *
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

#include "fmacros.h"
#include <sys/types.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <limits.h>
#include <stdlib.h>

#include "net.h"
#include "sds.h"
#include "sockcompat.h"
#include "win32.h"

/* Defined in hiredis.c */
void __redisSetError(redisContext *c, int type, const char *str);

void redisNetClose(redisContext *c) {
    if (c && c->fd != REDIS_INVALID_FD) {
        close(c->fd);
        c->fd = REDIS_INVALID_FD;
    }
}

ssize_t redisNetRead(redisContext *c, char *buf, size_t bufcap) {
    ssize_t nread = recv(c->fd, buf, bufcap, 0);
    if (nread == -1) {
        if ((errno == EWOULDBLOCK && !(c->flags & REDIS_BLOCK)) || (errno == EINTR)) {
            /* Try again later */
            return 0;
        } else if(errno == ETIMEDOUT && (c->flags & REDIS_BLOCK)) {
            /* especially in windows */
            __redisSetError(c, REDIS_ERR_TIMEOUT, "recv timeout");
            return -1;
        } else {
            __redisSetError(c, REDIS_ERR_IO, NULL);
            return -1;
        }
    } else if (nread == 0) {
        __redisSetError(c, REDIS_ERR_EOF, "Server closed the connection");
        return -1;
    } else {
        return nread;
    }
}

ssize_t redisNetWrite(redisContext *c) {
    ssize_t nwritten = send(c->fd, c->obuf, hi_sdslen(c->obuf), 0);
    if (nwritten < 0) {
        if ((errno == EWOULDBLOCK && !(c->flags & REDIS_BLOCK)) || (errno == EINTR)) {
            /* Try again later */
        } else {
            __redisSetError(c, REDIS_ERR_IO, NULL);
            return -1;
        }
    }
    return nwritten;
}

static void __redisSetErrorFromErrno(redisContext *c, int type, const char *prefix) {
    int errorno = errno;  /* snprintf() may change errno */
    char buf[128] = { 0 };
    size_t len = 0;

    if (prefix != NULL)
        len = snprintf(buf,sizeof(buf),"%s: ",prefix);
    strerror_r(errorno, (char *)(buf + len), sizeof(buf) - len);
    __redisSetError(c,type,buf);
}

static int redisSetReuseAddr(redisContext *c) {
    int on = 1;
    if (setsockopt(c->fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on)) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,NULL);
        redisNetClose(c);
        return REDIS_ERR;
    }
    return REDIS_OK;
}

static int redisCreateSocket(redisContext *c, int type) {
    redisFD s;
    if ((s = socket(type, SOCK_STREAM, 0)) == REDIS_INVALID_FD) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,NULL);
        return REDIS_ERR;
    }
    c->fd = s;
    if (type == AF_INET) {
        if (redisSetReuseAddr(c) == REDIS_ERR) {
            return REDIS_ERR;
        }
    }
    return REDIS_OK;
}

static int redisSetBlocking(redisContext *c, int blocking) {
#ifndef _WIN32
    // 非 win32
    int flags;

    /* Set the socket nonblocking.
     * Note that fcntl(2) for F_GETFL and F_SETFL can't be
     * interrupted by a signal. */
//    https://blog.csdn.net/fengxinlinux/article/details/51980837
//https://blog.csdn.net/linking530/article/details/7205305?
//获取文件打开方式的标志，标志值含义与open调用一致
    if ((flags = fcntl(c->fd, F_GETFL)) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,"fcntl(F_GETFL)");
        redisNetClose(c);
        return REDIS_ERR;
    }

    if (blocking) /* 取消文件的某个flags，比如文件是非阻塞的，想设置成为阻塞: */
        flags &= ~O_NONBLOCK;
    else    /* 增加文件的某个flags，比如文件是阻塞的，想设置成非阻塞: */
        flags |= O_NONBLOCK;

    if (fcntl(c->fd, F_SETFL, flags) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,"fcntl(F_SETFL)");
        redisNetClose(c);
        return REDIS_ERR;
    }
#else
    u_long mode = blocking ? 0 : 1;
    if (ioctl(c->fd, FIONBIO, &mode) == -1) {
        __redisSetErrorFromErrno(c, REDIS_ERR_IO, "ioctl(FIONBIO)");
        redisNetClose(c);
        return REDIS_ERR;
    }
#endif /* _WIN32 */
    return REDIS_OK;
}

int redisKeepAlive(redisContext *c, int interval) {
    int val = 1;
    redisFD fd = c->fd;

    if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) == -1){
        __redisSetError(c,REDIS_ERR_OTHER,strerror(errno));
        return REDIS_ERR;
    }

    val = interval;

#if defined(__APPLE__) && defined(__MACH__)
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &val, sizeof(val)) < 0) {
        __redisSetError(c,REDIS_ERR_OTHER,strerror(errno));
        return REDIS_ERR;
    }
#else
#if defined(__GLIBC__) && !defined(__FreeBSD_kernel__)
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
        __redisSetError(c,REDIS_ERR_OTHER,strerror(errno));
        return REDIS_ERR;
    }

    val = interval/3;
    if (val == 0) val = 1;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
        __redisSetError(c,REDIS_ERR_OTHER,strerror(errno));
        return REDIS_ERR;
    }

    val = 3;
    if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
        __redisSetError(c,REDIS_ERR_OTHER,strerror(errno));
        return REDIS_ERR;
    }
#endif
#endif

    return REDIS_OK;
}

// 开启tcp小包，就意味着禁用了Nagle算法，
int redisSetTcpNoDelay(redisContext *c) {
    int yes = 1;
    if (setsockopt(c->fd, IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,"setsockopt(TCP_NODELAY)");
        redisNetClose(c);
        return REDIS_ERR;
    }
    return REDIS_OK;
}

#define __MAX_MSEC (((LONG_MAX) - 999) / 1000)

static int redisContextTimeoutMsec(redisContext *c, long *result)
{
    const struct timeval *timeout = c->connect_timeout;
    long msec = -1;

    /* Only use timeout when not NULL. */
    if (timeout != NULL) {
        if (timeout->tv_usec > 1000000 || timeout->tv_sec > __MAX_MSEC) {
            *result = msec;
            return REDIS_ERR;
        }

        msec = (timeout->tv_sec * 1000) + ((timeout->tv_usec + 999) / 1000);

        if (msec < 0 || msec > INT_MAX) {
            msec = INT_MAX;
        }
    }

    *result = msec;
    return REDIS_OK;
}

// 检测链接是否成功
static int redisContextWaitReady(redisContext *c, long msec) {
    struct pollfd   wfd[1];

    wfd[0].fd     = c->fd;
    wfd[0].events = POLLOUT;

    if (errno == EINPROGRESS) {
        int res;

        // 获取 wfd 中可读的 链接
        if ((res = poll(wfd, 1, msec)) == -1) {
            __redisSetErrorFromErrno(c, REDIS_ERR_IO, "poll(2)");
            redisNetClose(c);
            return REDIS_ERR;
        } else if (res == 0) {
            // 获取超时
            /* Operation timed out */
            errno = ETIMEDOUT;
            __redisSetErrorFromErrno(c,REDIS_ERR_IO,NULL);
            redisNetClose(c);
            return REDIS_ERR;
        }

        // 检测链接是否成功
        if (redisCheckConnectDone(c, &res) != REDIS_OK || res == 0) {
            redisCheckSocketError(c);
            return REDIS_ERR;
        }

        return REDIS_OK;
    }

    __redisSetErrorFromErrno(c,REDIS_ERR_IO,NULL);
    redisNetClose(c);
    return REDIS_ERR;
}

/**
 *
 * @param c
 * @param completed 0 表示链接中， 1表示链接完成
 * @return REDIS_OK 表示链接成功
 */
int redisCheckConnectDone(redisContext *c, int *completed) {
    int rc = connect(c->fd, (const struct sockaddr *)c->saddr, c->addrlen);
    if (rc == 0) {
        // 链接成功
        *completed = 1;
        return REDIS_OK;
    }
    switch (errno) {
    case EISCONN:
        // 已经链接完成
        *completed = 1;
        return REDIS_OK;
    case EALREADY:
//        套接字为非阻塞套接字，并且原来的连接请求还未完成。
    case EINPROGRESS:
        //        套接字为非阻塞套接字，且连接请求没有立即完成。
    case EWOULDBLOCK:
        //        没有足够空闲的本地端口。
        *completed = 0;
        return REDIS_OK;
    default:
        return REDIS_ERR;
    }
}

int redisCheckSocketError(redisContext *c) {
    int err = 0, errno_saved = errno;
    socklen_t errlen = sizeof(err);

    if (getsockopt(c->fd, SOL_SOCKET, SO_ERROR, &err, &errlen) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,"getsockopt(SO_ERROR)");
        return REDIS_ERR;
    }

    if (err == 0) {
        err = errno_saved;
    }

    if (err) {
        errno = err;
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,NULL);
        return REDIS_ERR;
    }

    return REDIS_OK;
}

// 设置套接字 读写时间 超时时间
int redisContextSetTimeout(redisContext *c, const struct timeval tv) {
    const void *to_ptr = &tv;
    size_t to_sz = sizeof(tv);

//    SO_RCVTIMEO和SO_SNDTIMEO ，它们分别用来设置socket接收数据超时时间和发送数据超时时间。
//    因此，这两个选项仅对与数据收发相关的系统调用有效，这些系统调用包括：send, sendmsg, recv, recvmsg, accept, connect 。
//    这两个选项设置后，若超时， 返回-1，并设置errno为EAGAIN或EWOULDBLOCK.
//    其中connect超时的话，也是返回-1, 但errno设置为EINPROGRESS

    if (setsockopt(c->fd,SOL_SOCKET,SO_RCVTIMEO,to_ptr,to_sz) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,"setsockopt(SO_RCVTIMEO)");
        return REDIS_ERR;
    }
    if (setsockopt(c->fd,SOL_SOCKET,SO_SNDTIMEO,to_ptr,to_sz) == -1) {
        __redisSetErrorFromErrno(c,REDIS_ERR_IO,"setsockopt(SO_SNDTIMEO)");
        return REDIS_ERR;
    }
    return REDIS_OK;
}

// 设置链接超时间时间
int redisContextUpdateConnectTimeout(redisContext *c, const struct timeval *timeout) {
    /* Same timeval struct, short circuit */
    if (c->connect_timeout == timeout)
        return REDIS_OK;

    /* Allocate context timeval if we need to */
    if (c->connect_timeout == NULL) {
        c->connect_timeout = hi_malloc(sizeof(*c->connect_timeout));
        if (c->connect_timeout == NULL)
            return REDIS_ERR;
    }

    memcpy(c->connect_timeout, timeout, sizeof(*c->connect_timeout));
    return REDIS_OK;
}

// 设置命令超时时间
int redisContextUpdateCommandTimeout(redisContext *c, const struct timeval *timeout) {
    /* Same timeval struct, short circuit */
    if (c->command_timeout == timeout)
        return REDIS_OK;

    /* Allocate context timeval if we need to */
    if (c->command_timeout == NULL) {
        c->command_timeout = hi_malloc(sizeof(*c->command_timeout));
        if (c->command_timeout == NULL)
            return REDIS_ERR;
    }

    memcpy(c->command_timeout, timeout, sizeof(*c->command_timeout));
    return REDIS_OK;
}

static int _redisContextConnectTcp(redisContext *c, const char *addr, int port,
                                   const struct timeval *timeout,
                                   const char *source_addr) {
    redisFD s;
    int rv, n;
    char _port[6];  /* strlen("65535"); */
    struct addrinfo hints, *servinfo, *bservinfo, *p, *b;
    // 获取当前标志位 是否阻塞
    int blocking = (c->flags & REDIS_BLOCK);
    //  获取当前标志位 是否复用地址
    int reuseaddr = (c->flags & REDIS_REUSEADDR);
    int reuses = 0;
    long timeout_msec = -1;

    servinfo = NULL;
    c->connection_type = REDIS_CONN_TCP;
    c->tcp.port = port;

    /* We need to take possession of the passed parameters
     * to make them reusable for a reconnect.
     * We also carefully check we don't free data we already own,
     * as in the case of the reconnect method.
     *
     * This is a bit ugly, but atleast it works and doesn't leak memory.
     **/
    // 初始化host
    if (c->tcp.host != addr) {
        // 链接host 未指定或者不相同
        hi_free(c->tcp.host);

        c->tcp.host = hi_strdup(addr);
        if (c->tcp.host == NULL)
            goto oom;
    }

    // 初始化超时时间
    if (timeout) {
        if (redisContextUpdateConnectTimeout(c, timeout) == REDIS_ERR)
            goto oom;
    } else {
        hi_free(c->connect_timeout);
        c->connect_timeout = NULL;
    }

    if (redisContextTimeoutMsec(c, &timeout_msec) != REDIS_OK) {
        // 超时时间不正确
        __redisSetError(c, REDIS_ERR_IO, "Invalid timeout specified");
        goto error;
    }

    // 初始化链接原地址
    if (source_addr == NULL) {
        hi_free(c->tcp.source_addr);
        c->tcp.source_addr = NULL;
    } else if (c->tcp.source_addr != source_addr) {
        hi_free(c->tcp.source_addr);
        c->tcp.source_addr = hi_strdup(source_addr);
    }

    // 初始化目标端口
    snprintf(_port, 6, "%d", port);

    // 设置目标 地址解析方式
    memset(&hints,0,sizeof(hints));
    hints.ai_family = AF_INET;
    hints.ai_socktype = SOCK_STREAM;

    /* Try with IPv6 if no IPv4 address was found. We do it in this order since
     * in a Redis client you can't afford to test if you have IPv6 connectivity
     * as this would add latency to every connect. Otherwise a more sensible
     * route could be: Use IPv6 if both addresses are available and there is IPv6
     * connectivity. */
    // 解析目标地址， 获取地址信息
    if ((rv = getaddrinfo(c->tcp.host,_port,&hints,&servinfo)) != 0) {
         hints.ai_family = AF_INET6;
         if ((rv = getaddrinfo(addr,_port,&hints,&servinfo)) != 0) {
            __redisSetError(c,REDIS_ERR_OTHER,gai_strerror(rv));
            return REDIS_ERR;
        }
    }
    for (p = servinfo; p != NULL; p = p->ai_next) {
addrretry:
        // 套接字创建失败
        if ((s = socket(p->ai_family,p->ai_socktype,p->ai_protocol)) == REDIS_INVALID_FD)
            continue;

        c->fd = s;
        // 设置套接字 非阻塞
        if (redisSetBlocking(c,0) != REDIS_OK)
            goto error;
        if (c->tcp.source_addr) {
            int bound = 0;
            /* Using getaddrinfo saves us from self-determining IPv4 vs IPv6 */
            // 解析本机地址信息
            if ((rv = getaddrinfo(c->tcp.source_addr, NULL, &hints, &bservinfo)) != 0) {
                char buf[128];
                snprintf(buf,sizeof(buf),"Can't get addr: %s",gai_strerror(rv));
                __redisSetError(c,REDIS_ERR_OTHER,buf);
                goto error;
            }

            if (reuseaddr) {
                n = 1;
                // 一个端口释放后会等待两分钟之后才能再被使用，
                // SO_REUSEADDR是让端口释放后立即就可以被再次使用。
                if (setsockopt(s, SOL_SOCKET, SO_REUSEADDR, (char*) &n,
                               sizeof(n)) < 0) {
                    freeaddrinfo(bservinfo);
                    goto error;
                }
            }

            // 尝试依次对本地地址进行绑定，成功跳出
            for (b = bservinfo; b != NULL; b = b->ai_next) {
                if (bind(s,b->ai_addr,b->ai_addrlen) != -1) {
                    bound = 1;
                    break;
                }
            }
            // 释放
            freeaddrinfo(bservinfo);
            if (!bound) {
                // 绑定失败
                char buf[128];
                snprintf(buf,sizeof(buf),"Can't bind socket: %s",strerror(errno));
                __redisSetError(c,REDIS_ERR_OTHER,buf);
                goto error;
            }
        }

        /* For repeat connection */
        hi_free(c->saddr);
        c->saddr = hi_malloc(p->ai_addrlen);
        if (c->saddr == NULL)
            goto oom;

        memcpy(c->saddr, p->ai_addr, p->ai_addrlen);
        c->addrlen = p->ai_addrlen;

        // 链接目标地址
        if (connect(s,p->ai_addr,p->ai_addrlen) == -1) {
            if (errno == EHOSTUNREACH) {
                // 无法匹配到 路由表，或者无法找到目标地址
                redisNetClose(c);
                continue;
            } else if (errno == EINPROGRESS) {
//                连接还在进行中。
                if (blocking) {
                    // 阻塞模式下会等待 直到链接成功
                    // 非阻塞模式下就直接返回了
                    goto wait_for_ready;
                }
                // 那么就代表连接还在进行中。
                // 后面可以通过poll或者select来判断socket是否可写，
                // 如果可以写，说明连接完成了。
                /* This is ok.
                 * Note that even when it's in blocking mode, we unset blocking
                 * for `connect()`
                 */
            } else if (errno == EADDRNOTAVAIL && reuseaddr) {
                //   /* Can't assign requested address */ 无法分配地址
                // 端口被占用，内存不足，等情况
                if (++reuses >= REDIS_CONNECT_RETRIES) {
                    // 重试10 报错
                    goto error;
                } else {
                    redisNetClose(c);
                    goto addrretry;
                }
            } else {
                wait_for_ready:
                // 链接失败， error
                if (redisContextWaitReady(c,timeout_msec) != REDIS_OK)
                    goto error;
                // 禁用了Nagle算法，允许小包传输
                if (redisSetTcpNoDelay(c) != REDIS_OK)
                    goto error;
            }
        }

        // 原链接标志 阻塞，恢复套接字阻塞状态
        if (blocking && redisSetBlocking(c,1) != REDIS_OK)
            goto error;

        // 设置标志位，表示链接成功
        c->flags |= REDIS_CONNECTED;
        rv = REDIS_OK;
        goto end;
    }
    if (p == NULL) {
        char buf[128];
        snprintf(buf,sizeof(buf),"Can't create socket: %s",strerror(errno));
        __redisSetError(c,REDIS_ERR_OTHER,buf);
        goto error;
    }

oom:
    __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
error:
    rv = REDIS_ERR;
end:
    if(servinfo) {
        freeaddrinfo(servinfo);
    }

    return rv;  // Need to return REDIS_OK if alright
}

int redisContextConnectTcp(redisContext *c, const char *addr, int port,
                           const struct timeval *timeout) {
    return _redisContextConnectTcp(c, addr, port, timeout, NULL);
}

int redisContextConnectBindTcp(redisContext *c, const char *addr, int port,
                               const struct timeval *timeout,
                               const char *source_addr) {
    return _redisContextConnectTcp(c, addr, port, timeout, source_addr);
}

int redisContextConnectUnix(redisContext *c, const char *path, const struct timeval *timeout) {
#ifndef _WIN32
// 非 win32

    // 是否是阻塞模式
    int blocking = (c->flags & REDIS_BLOCK);
    struct sockaddr_un *sa;
    long timeout_msec = -1;

    // 创建要接字
    if (redisCreateSocket(c,AF_UNIX) < 0)
        return REDIS_ERR;

    // 设置成非阻塞模式
    if (redisSetBlocking(c,0) != REDIS_OK)
        return REDIS_ERR;

    c->connection_type = REDIS_CONN_UNIX;
    if (c->unix_sock.path != path) {
        hi_free(c->unix_sock.path);

        c->unix_sock.path = hi_strdup(path);
        if (c->unix_sock.path == NULL)
            goto oom;
    }

    if (timeout) {
        // 设置联检超时
        if (redisContextUpdateConnectTimeout(c, timeout) == REDIS_ERR)
            goto oom;
    } else {
        hi_free(c->connect_timeout);
        c->connect_timeout = NULL;
    }

    // 检查是否超过最大时间
    if (redisContextTimeoutMsec(c,&timeout_msec) != REDIS_OK)
        return REDIS_ERR;

    /* Don't leak sockaddr if we're reconnecting */
    if (c->saddr) hi_free(c->saddr);

    sa = (struct sockaddr_un*)(c->saddr = hi_malloc(sizeof(struct sockaddr_un)));
    if (sa == NULL)
        goto oom;

    c->addrlen = sizeof(struct sockaddr_un);
    sa->sun_family = AF_UNIX;
    strncpy(sa->sun_path, path, sizeof(sa->sun_path) - 1);
    if (connect(c->fd, (struct sockaddr*)sa, sizeof(*sa)) == -1) {
        if (errno == EINPROGRESS && !blocking) {
            // 非阻塞直接返回
            /* This is ok. */
        } else {
            // 阻塞模式
            if (redisContextWaitReady(c,timeout_msec) != REDIS_OK)
                return REDIS_ERR;
        }
    }

    /* Reset socket to be blocking after connect(2). */
    // 恢复成组赛模式
    if (blocking && redisSetBlocking(c,1) != REDIS_OK)
        return REDIS_ERR;

    c->flags |= REDIS_CONNECTED;
    return REDIS_OK;
#else
    /* We currently do not support Unix sockets for Windows. */
    /* TODO(m): https://devblogs.microsoft.com/commandline/af_unix-comes-to-windows/ */
    errno = EPROTONOSUPPORT;
    return REDIS_ERR;
#endif /* _WIN32 */
oom:
    __redisSetError(c, REDIS_ERR_OOM, "Out of memory");
    return REDIS_ERR;
}
