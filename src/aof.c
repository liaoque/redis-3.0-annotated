/*
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
#include "bio.h"
#include "rio.h"

#include <signal.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <sys/time.h>
#include <sys/resource.h>
#include <sys/wait.h>
#include <sys/param.h>

void aofUpdateCurrentSize(void);

void aofClosePipes(void);

/* ----------------------------------------------------------------------------
 * AOF rewrite buffer implementation.
 *
 * AOF 重写缓存的实现。
 *
 * The following code implement a simple buffer used in order to accumulate
 * changes while the background process is rewriting the AOF file.
 *
 * 以下代码实现了一个简单的缓存，
 * 它可以在 BGREWRITEAOF 执行的过程中，累积所有修改数据集的命令。
 *
 * We only need to append, but can't just use realloc with a large block
 * because 'huge' reallocs are not always handled as one could expect
 * (via remapping of pages at OS level) but may involve copying data.
 *
 * For this reason we use a list of blocks, every block is
 * AOF_RW_BUF_BLOCK_SIZE bytes.
 *
 * 程序需要不断对这个缓存执行 append 操作，
 * 因为分配一个非常大的空间并不总是可能的，
 * 也可能产生大量的复制工作，
 * 所以这里使用多个大小为 AOF_RW_BUF_BLOCK_SIZE 的空间来保存命令。
 *
 * ------------------------------------------------------------------------- */

// 每个缓存块的大小
#define AOF_RW_BUF_BLOCK_SIZE (1024*1024*10)    /* 10 MB per block */

typedef struct aofrwblock {
    // 缓存块已使用字节数和可用字节数
    unsigned long used, free;

    // 缓存块
    char buf[AOF_RW_BUF_BLOCK_SIZE];
} aofrwblock;

/* This function free the old AOF rewrite buffer if needed, and initialize
 * a fresh new one. It tests for server.aof_rewrite_buf_blocks equal to NULL
 * so can be used for the first initialization as well. 
 *
 * 释放旧的 AOF 重写缓存，并初始化一个新的 AOF 缓存。
 *
 * 这个函数也可以单纯地用于 AOF 重写缓存的初始化。
 */
void aofRewriteBufferReset(void) {

    // 释放旧有的缓存（链表）
    if (server.aof_rewrite_buf_blocks)
        listRelease(server.aof_rewrite_buf_blocks);

    // 初始化新的缓存（链表）
    server.aof_rewrite_buf_blocks = listCreate();
    listSetFreeMethod(server.aof_rewrite_buf_blocks, zfree);
}

/* Return the current size of the AOF rewrite buffer. 
 *
 * 返回 AOF 重写缓存当前的大小
 */
unsigned long aofRewriteBufferSize(void) {
    listNode *ln;
    listIter li;
    unsigned long size = 0;

    // 取出链表中的缓存块
    listRewind(server.aof_rewrite_buf_blocks, &li);
    while ((ln = listNext(&li))) {
        // 总缓存大小 += block->used
        aofrwblock *block = listNodeValue(ln);
        size += block->used;
    }
    return size;
}

/* Event handler used to send data to the child process doing the AOF
 * rewrite. We send pieces of our AOF differences buffer so that the final
 * write when the child finishes the rewrite will be small. */
void aofChildWriteDiffData(aeEventLoop *el, int fd, void *privdata, int mask) {
    listNode *ln;
    aofrwblock *block;
    ssize_t nwritten;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(privdata);
    UNUSED(mask);

    while (1) {
        ln = listFirst(server.aof_rewrite_buf_blocks);
        block = ln ? ln->value : NULL;
        if (server.aof_stop_sending_diff || !block) {
            aeDeleteFileEvent(server.el, server.aof_pipe_write_data_to_child,
                              AE_WRITABLE);
            return;
        }
        if (block->used > 0) {
            nwritten = write(server.aof_pipe_write_data_to_child,
                             block->buf, block->used);
            if (nwritten <= 0) return;
            memmove(block->buf, block->buf + nwritten, block->used - nwritten);
            block->used -= nwritten;
            block->free += nwritten;
        }
        if (block->used == 0) listDelNode(server.aof_rewrite_buf_blocks, ln);
    }
}

/* Append data to the AOF rewrite buffer, allocating new blocks if needed. 
 *
 * 将字符数组 s 追加到 AOF 缓存的末尾，
 * 如果有需要的话，分配一个新的缓存块。
 */
void aofRewriteBufferAppend(unsigned char *s, unsigned long len) {
    // 指向最后一个缓存块
    listNode *ln = listLast(server.aof_rewrite_buf_blocks);
    aofrwblock *block = ln ? ln->value : NULL;

    while (len) {
        /* If we already got at least an allocated block, try appending
         * at least some piece into it. 
         *
         * 如果已经有至少一个缓存块，那么尝试将内容追加到这个缓存块里面
         */
        if (block) {
            unsigned long thislen = (block->free < len) ? block->free : len;
            if (thislen) {  /* The current block is not already full. */
                memcpy(block->buf + block->used, s, thislen);
                block->used += thislen;
                block->free -= thislen;
                s += thislen;
                len -= thislen;
            }
        }

        // 如果 block != NULL ，那么这里是创建另一个缓存块买容纳 block 装不下的内容
        // 如果 block == NULL ，那么这里是创建缓存链表的第一个缓存块
        if (len) { /* First block to allocate, or need another block. */
            int numblocks;
            // 10M缓冲区
            block = zmalloc(sizeof(*block));
            // 写死10M空余空间
            block->free = AOF_RW_BUF_BLOCK_SIZE;
            block->used = 0;
            // 链接到链表末尾
            listAddNodeTail(server.aof_rewrite_buf_blocks, block);

            /* Log every time we cross more 10 or 100 blocks, respectively
             * as a notice or warning. */
            // 每次创建 10 个缓存块就打印一个日志，用作标记或者提醒
            numblocks = listLength(server.aof_rewrite_buf_blocks);
            if (((numblocks + 1) % 10) == 0) {
                // 990M的时候 打印 #  “# == LL_WARNING”
                int level = ((numblocks + 1) % 100) == 0 ? LL_WARNING :
                            LL_NOTICE;
                serverLog(level, "Background AOF buffer size: %lu MB",
                          aofRewriteBufferSize() / (1024 * 1024));
            }
        }
    }

    /* Install a file event to send data to the rewrite child if there is
     * not one already. */
    if (aeGetFileEvents(server.el, server.aof_pipe_write_data_to_child) == 0) {
        aeCreateFileEvent(server.el, server.aof_pipe_write_data_to_child,
                          AE_WRITABLE, aofChildWriteDiffData, NULL);
    }
}

/* Write the buffer (possibly composed of multiple blocks) into the specified
 * fd. If a short write or any other error happens -1 is returned,
 * otherwise the number of bytes written is returned. 
 *
 * 将重写缓存中的所有内容（可能由多个块组成）写入到给定 fd 中。
 *
 * 如果没有 short write 或者其他错误发生，那么返回写入的字节数量，
 * 否则，返回 -1 。
 */
ssize_t aofRewriteBufferWrite(int fd) {
    listNode *ln;
    listIter li;
    ssize_t count = 0;

    // 遍历所有缓存块
    listRewind(server.aof_rewrite_buf_blocks, &li);
    while ((ln = listNext(&li))) {
        aofrwblock *block = listNodeValue(ln);
        ssize_t nwritten;

        if (block->used) {
            // 写入缓存块内容到 fd
            nwritten = write(fd, block->buf, block->used);
            if (nwritten != (ssize_t) block->used) {
                if (nwritten == 0) errno = EIO;
                return -1;
            }
            // 积累写入字节
            count += nwritten;
        }
    }
    return count;
}

/* ----------------------------------------------------------------------------
 * AOF file implementation
 * ------------------------------------------------------------------------- */

/* Return true if an AOf fsync is currently already in progress in a
 * BIO thread. */
int aofFsyncInProgress(void) {
    return bioPendingJobsOfType(BIO_AOF_FSYNC) != 0;
}

/* Starts a background task that performs fsync() against the specified
 * file descriptor (the one of the AOF file) in another thread. 
 *
 * 在另一个线程中，对给定的描述符 fd （指向 AOF 文件）执行一个后台 fsync() 操作。
 */
void aof_background_fsync(int fd) {
    bioCreateFsyncJob(fd);
}

/* Kills an AOFRW child process if exists */
void killAppendOnlyChild(void) {
    int statloc;
    /* No AOFRW child? return. */
    if (server.child_type != CHILD_TYPE_AOF) return;
    /* Kill AOFRW child, wait for child exit. */
    serverLog(LL_NOTICE, "Killing running AOF rewrite child: %ld",
              (long) server.child_pid);
    if (kill(server.child_pid, SIGUSR1) != -1) {
        while (waitpid(-1, &statloc, 0) != server.child_pid);
    }
    /* Reset the buffer accumulating changes while the child saves. */
    aofRewriteBufferReset();
    aofRemoveTempFile(server.child_pid);
    resetChildState();
    server.aof_rewrite_time_start = -1;
    /* Close pipes used for IPC between the two processes. */
    aofClosePipes();
}

/* Called when the user switches from "appendonly yes" to "appendonly no"
 * at runtime using the CONFIG command. 
 *
 * 在用户通过 CONFIG 命令在运行时关闭 AOF 持久化时调用
 */
void stopAppendOnly(void) {
    serverAssert(server.aof_state != AOF_OFF);
    // 将 AOF 缓存的内容写入并冲洗到 AOF 文件中
    // 参数 1 表示强制模式
    flushAppendOnlyFile(1);


    // 冲洗 AOF 文件
    if (redis_fsync(server.aof_fd) == -1) {
        serverLog(LL_WARNING, "Fail to fsync the AOF file: %s", strerror(errno));
    } else {
        server.aof_fsync_offset = server.aof_current_size;
        server.aof_last_fsync = server.unixtime;
    }
    // 关闭 AOF 文件
    close(server.aof_fd);

    // 清空 AOF 状态
    server.aof_fd = -1;
    server.aof_selected_db = -1;
    server.aof_state = AOF_OFF;
    server.aof_rewrite_scheduled = 0;

    // 杀死子进程
    killAppendOnlyChild();
    // 清理未完成的 AOF 重写留下来的缓存和临时文件
    sdsfree(server.aof_buf);
    server.aof_buf = sdsempty();
}

/* Called when the user switches from "appendonly no" to "appendonly yes"
 * at runtime using the CONFIG command. 
 *
 * 当用户在运行时使用 CONFIG 命令，
 * 从 appendonly no 切换到 appendonly yes 时执行
 */
int startAppendOnly(void) {
    char cwd[MAXPATHLEN]; /* Current working dir path for error messages. */
    int newfd;

    // 打开 AOF 文件
    newfd = open(server.aof_filename, O_WRONLY | O_APPEND | O_CREAT, 0644);
    serverAssert(server.aof_state == AOF_OFF);
    if (newfd == -1) {
        char *cwdp = getcwd(cwd, MAXPATHLEN);

        serverLog(LL_WARNING,
                  "Redis needs to enable the AOF but can't open the "
                  "append only file %s (in server root dir %s): %s",
                  server.aof_filename,
                  cwdp ? cwdp : "unknown",
                  strerror(errno));
        return C_ERR;
    }
    if (hasActiveChildProcess() && server.child_type != CHILD_TYPE_AOF) {
        server.aof_rewrite_scheduled = 1;
        serverLog(LL_WARNING,
                  "AOF was enabled but there is already another background operation. An AOF background was scheduled to start when possible.");
    } else {
        /* If there is a pending AOF rewrite, we need to switch it off and
         * start a new one: the old one cannot be reused because it is not
         * accumulating the AOF buffer. */
        if (server.child_type == CHILD_TYPE_AOF) {
            serverLog(LL_WARNING,
                      "AOF was enabled but there is already an AOF rewriting in background. Stopping background AOF and starting a rewrite now.");
            killAppendOnlyChild();
        }
        if (rewriteAppendOnlyFileBackground() == C_ERR) {
            // AOF 后台重写失败，关闭 AOF 文件
            close(newfd);
            serverLog(LL_WARNING,
                      "Redis needs to enable the AOF but can't trigger a background AOF rewrite operation. Check the above logs for more info about the error.");
            return C_ERR;
        }
    }
    /* We correctly switched on AOF, now wait for the rewrite to be complete
     * in order to append data on disk.
     * 等待重写执行完毕
 	 */
    server.aof_state = AOF_WAIT_REWRITE;
    server.aof_last_fsync = server.unixtime;
    server.aof_fd = newfd;

    /* If AOF fsync error in bio job, we just ignore it and log the event. */
    int aof_bio_fsync_status;
    atomicGet(server.aof_bio_fsync_status, aof_bio_fsync_status);
    if (aof_bio_fsync_status == C_ERR) {
        serverLog(LL_WARNING,
                  "AOF reopen, just ignore the AOF fsync error in bio job");
        atomicSet(server.aof_bio_fsync_status, C_OK);
    }

    /* If AOF was in error state, we just ignore it and log the event. */
    if (server.aof_last_write_status == C_ERR) {
        serverLog(LL_WARNING, "AOF reopen, just ignore the last error.");
        server.aof_last_write_status = C_OK;
    }
    return C_OK;
}

/* This is a wrapper to the write syscall in order to retry on short writes
 * or if the syscall gets interrupted. It could look strange that we retry
 * on short writes given that we are writing to a block device: normally if
 * the first call is short, there is a end-of-space condition, so the next
 * is likely to fail. However apparently in modern systems this is no longer
 * true, and in general it looks just more resilient to retry the write. If
 * there is an actual error condition we'll get it at the next try. */
ssize_t aofWrite(int fd, const char *buf, size_t len) {
    ssize_t nwritten = 0, totwritten = 0;

    while (len) {
        nwritten = write(fd, buf, len);

        if (nwritten < 0) {
            if (errno == EINTR) continue;
            return totwritten ? totwritten : -1;
        }

        len -= nwritten;
        buf += nwritten;
        totwritten += nwritten;
    }

    return totwritten;
}

/* Write the append only file buffer on disk.
 *
 * 将 AOF 缓存写入到文件中。
 *
 * Since we are required to write the AOF before replying to the client,
 * and the only way the client socket can get a write is entering when the
 * the event loop, we accumulate all the AOF writes in a memory
 * buffer and write it on disk using this function just before entering
 * the event loop again.
 *
 * 因为程序需要在回复客户端之前对 AOF 执行写操作。
 * 而客户端能执行写操作的唯一机会就是在事件 loop 中，
 * 因此，程序将所有 AOF 写累积到缓存中，
 * 并在重新进入事件 loop 之前，将缓存写入到文件中。
 *
 * About the 'force' argument:
 *
 * 关于 force 参数：
 *
 * When the fsync policy is set to 'everysec' we may delay the flush if there
 * is still an fsync() going on in the background thread, since for instance
 * on Linux write(2) will be blocked by the background fsync anyway.
 *
 * 当 fsync 策略为每秒钟保存一次时，如果后台线程仍然有 fsync 在执行，
 * 那么我们可能会延迟执行冲洗（flush）操作，
 * 因为 Linux 上的 write(2) 会被后台的 fsync 阻塞。
 *
 * When this happens we remember that there is some aof buffer to be
 * flushed ASAP, and will try to do that in the serverCron() function.
 *
 * 当这种情况发生时，说明需要尽快冲洗 aof 缓存，
 * 程序会尝试在 serverCron() 函数中对缓存进行冲洗。
 *
 * However if force is set to 1 we'll write regardless of the background
 * fsync. 
 *
 * 不过，如果 force 为 1 的话，那么不管后台是否正在 fsync ，
 * 程序都直接进行写入。
 */
#define AOF_WRITE_LOG_ERROR_RATE 30 /* Seconds between errors logging. */

void flushAppendOnlyFile(int force) {
    ssize_t nwritten;
    int sync_in_progress = 0;
    mstime_t latency;


    if (sdslen(server.aof_buf) == 0) {
        /* Check if we need to do fsync even the aof buffer is empty,
         * because previously in AOF_FSYNC_EVERYSEC mode, fsync is
         * called only when aof buffer is not empty, so if users
         * stop write commands before fsync called in one second,
         * the data in page cache cannot be flushed in time. */
        if (server.aof_fsync == AOF_FSYNC_EVERYSEC &&
            server.aof_fsync_offset != server.aof_current_size &&
            server.unixtime > server.aof_last_fsync &&
            !(sync_in_progress = aofFsyncInProgress())) {
            goto try_fsync;
        } else {
            // 缓冲区中没有任何内容，直接返回
            return;
        }
    }

    // 策略为每秒 FSYNC 
    // 是否有 SYNC 正在后台进行？
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC)
        sync_in_progress = aofFsyncInProgress();

    // 每秒 fsync ，并且强制写入为假
    if (server.aof_fsync == AOF_FSYNC_EVERYSEC && !force) {
        /* With this append fsync policy we do background fsyncing.
         *
         * 当 fsync 策略为每秒钟一次时， fsync 在后台执行。
         *
         * If the fsync is still in progress we can try to delay
         * the write for a couple of seconds. 
         *
         * 如果后台仍在执行 FSYNC ，那么我们可以延迟写操作一两秒
         * （如果强制执行 write 的话，服务器主线程将阻塞在 write 上面）
         */
        if (sync_in_progress) {

            // 有 fsync 正在后台进行 。。。
            if (server.aof_flush_postponed_start == 0) {
                /* No previous write postponing, remember that we are
                 * postponing the flush and return.
                 *
                 * 前面没有推迟过 write 操作，这里将推迟写操作的时间记录下来
                 * 然后就返回，不执行 write 或者 fsync
                 */
                server.aof_flush_postponed_start = server.unixtime;
                return;
            } else if (server.unixtime - server.aof_flush_postponed_start < 2) {
                /* We were already waiting for fsync to finish, but for less
                 * than two seconds this is still ok. Postpone again. 
                 *
                 * 如果之前已经因为 fsync 而推迟了 write 操作
                 * 但是推迟的时间不超过 2 秒，那么直接返回
                 * 不执行 write 或者 fsync
                 */
                return;
            }
            /* Otherwise fall trough, and go write since we can't wait
             * over two seconds. 
             *
             * 如果后台还有 fsync 在执行，并且 write 已经推迟 >= 2 秒
             * 那么执行写操作（write 将被阻塞）
             */
            server.aof_delayed_fsync++;
            serverLog(LL_NOTICE,
                      "Asynchronous AOF fsync is taking too long (disk is busy?). Writing the AOF buffer without waiting for fsync to complete, this may slow down Redis.");
        }
    }
    /* We want to perform a single write. This should be guaranteed atomic
     * at least if the filesystem we are writing is a real physical one.
     *
     * 执行单个 write 操作，如果写入设备是物理的话，那么这个操作应该是原子的
     *
     * While this will save us against the server being killed I don't think
     * there is much to do about the whole server stopping for power problems
     * or alike 
     *
     * 当然，如果出现像电源中断这样的不可抗现象，那么 AOF 文件也是可能会出现问题的
     * 这时就要用 redis-check-aof 程序来进行修复。
     */

    if (server.aof_flush_sleep && sdslen(server.aof_buf)) {
        usleep(server.aof_flush_sleep);
    }

    latencyStartMonitor(latency);
    nwritten = aofWrite(server.aof_fd, server.aof_buf, sdslen(server.aof_buf));
    latencyEndMonitor(latency);
    /* We want to capture different events for delayed writes:
     * when the delay happens with a pending fsync, or with a saving child
     * active, and when the above two conditions are missing.
     * We also use an additional event name to save all samples which is
     * useful for graphing / monitoring purposes. */
    if (sync_in_progress) {
        latencyAddSampleIfNeeded("aof-write-pending-fsync", latency);
    } else if (hasActiveChildProcess()) {
        latencyAddSampleIfNeeded("aof-write-active-child", latency);
    } else {
        latencyAddSampleIfNeeded("aof-write-alone", latency);
    }
    latencyAddSampleIfNeeded("aof-write", latency);

    /* We performed the write so reset the postponed flush sentinel to zero. */
    server.aof_flush_postponed_start = 0;

    if (nwritten != (ssize_t) sdslen(server.aof_buf)) {
        static time_t last_write_error_log = 0;
        int can_log = 0;

        /* Limit logging rate to 1 line per AOF_WRITE_LOG_ERROR_RATE seconds. */
        // 将日志的记录频率限制在每行 AOF_WRITE_LOG_ERROR_RATE 秒
        if ((server.unixtime - last_write_error_log) > AOF_WRITE_LOG_ERROR_RATE) {
            can_log = 1;
            last_write_error_log = server.unixtime;
        }

        /* Log the AOF write error and record the error code. */
        // 如果写入出错，那么尝试将该情况写入到日志里面
        if (nwritten == -1) {
            if (can_log) {
                serverLog(LL_WARNING, "Error writing to the AOF file: %s",
                          strerror(errno));
                server.aof_last_write_errno = errno;
            }
        } else {
            if (can_log) {
                serverLog(LL_WARNING, "Short write while writing to "
                                      "the AOF file: (nwritten=%lld, "
                                      "expected=%lld)",
                          (long long) nwritten,
                          (long long) sdslen(server.aof_buf));
            }

            // 尝试移除新追加的不完整内容
            if (ftruncate(server.aof_fd, server.aof_current_size) == -1) {
                if (can_log) {
                    serverLog(LL_WARNING, "Could not remove short write "
                                          "from the append-only file.  Redis may refuse "
                                          "to load the AOF the next time it starts.  "
                                          "ftruncate: %s", strerror(errno));
                }
            } else {
                /* If the ftruncate() succeeded we can set nwritten to
                 * -1 since there is no longer partial data into the AOF. */
                nwritten = -1;
            }
            server.aof_last_write_errno = ENOSPC;
        }

        /* Handle the AOF write error. */
        // 处理写入 AOF 文件时出现的错误
        if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            /* We can't recover when the fsync policy is ALWAYS since the reply
             * for the client is already in the output buffers (both writes and
             * reads), and the changes to the db can't be rolled back. Since we
             * have a contract with the user that on acknowledged or observed
             * writes are is synced on disk, we must exit. */
            serverLog(LL_WARNING,
                      "Can't recover from AOF write error when the AOF fsync policy is 'always'. Exiting...");
            exit(1);
        } else {
            /* Recover from failed write leaving data into the buffer. However
             * set an error to stop accepting writes as long as the error
             * condition is not cleared. */
            server.aof_last_write_status = C_ERR;

            /* Trim the sds buffer if there was a partial write, and there
             * was no way to undo it with ftruncate(2). */
            if (nwritten > 0) {
                server.aof_current_size += nwritten;
                sdsrange(server.aof_buf, nwritten, -1);
            }
            return; /* We'll try again on the next call... */
        }
    } else {
        /* Successful write(2). If AOF was in error state, restore the
         * OK state and log the event. */
        // 写入成功，更新最后写入状态
        if (server.aof_last_write_status == C_ERR) {
            serverLog(LL_WARNING,
                      "AOF write error looks solved, Redis can write again.");
            server.aof_last_write_status = C_OK;
        }
    }
    // 更新写入后的 AOF 文件大小
    server.aof_current_size += nwritten;

    /* Re-use AOF buffer when it is small enough. The maximum comes from the
     * arena size of 4k minus some overhead (but is otherwise arbitrary). 
     *
     * 如果 AOF 缓存的大小足够小的话，那么重用这个缓存，
     * 否则的话，释放 AOF 缓存。
     */
    if ((sdslen(server.aof_buf) + sdsavail(server.aof_buf)) < 4000) {
        // 清空缓存中的内容，等待重用
        sdsclear(server.aof_buf);
    } else {
        // 释放缓存
        sdsfree(server.aof_buf);
        server.aof_buf = sdsempty();
    }

    try_fsync:
    /* Don't fsync if no-appendfsync-on-rewrite is set to yes and there are
     * children doing I/O in the background. 
     *
     * 如果 no-appendfsync-on-rewrite 选项为开启状态，
     * 并且有 BGSAVE 或者 BGREWRITEAOF 正在进行的话，
     * 那么不执行 fsync 
     */
    if (server.aof_no_fsync_on_rewrite && hasActiveChildProcess())
        return;

    /* Perform the fsync if needed. */
    // 总是执行 fsnyc
    if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
        /* redis_fsync is defined as fdatasync() for Linux in order to avoid
         * flushing metadata. */
        latencyStartMonitor(latency);
        /* Let's try to get this data on the disk. To guarantee data safe when
         * the AOF fsync policy is 'always', we should exit if failed to fsync
         * AOF (see comment next to the exit(1) after write error above). */
        // 更新最后一次执行 fsnyc 的时间
        if (redis_fsync(server.aof_fd) == -1) {
            serverLog(LL_WARNING, "Can't persist AOF for fsync error when the "
                                  "AOF fsync policy is 'always': %s. Exiting...", strerror(errno));
            exit(1);
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-fsync-always", latency);
        server.aof_fsync_offset = server.aof_current_size;
        server.aof_last_fsync = server.unixtime;
        // 策略为每秒 fsnyc ，并且距离上次 fsync 已经超过 1 秒
    } else if ((server.aof_fsync == AOF_FSYNC_EVERYSEC &&
                server.unixtime > server.aof_last_fsync)) {
        // 放到后台执行
        if (!sync_in_progress) {
            aof_background_fsync(server.aof_fd);
            server.aof_fsync_offset = server.aof_current_size;
        }
        // 更新最后一次执行 fsync 的时间
        server.aof_last_fsync = server.unixtime;
    }
}

/*
 * 根据传入的命令和命令参数，将它们还原成协议格式。
 */
sds catAppendOnlyGenericCommand(sds dst, int argc, robj **argv) {
    char buf[32];
    int len, j;
    robj *o;

    // 重建命令的个数，格式为 *<count>\r\n
    // 例如 *3\r\n
    buf[0] = '*';
    len = 1 + ll2string(buf + 1, sizeof(buf) - 1, argc);
    buf[len++] = '\r';
    buf[len++] = '\n';
    dst = sdscatlen(dst, buf, len);

    // 重建命令和命令参数，格式为 $<length>\r\n<content>\r\n
    // 例如 $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
    for (j = 0; j < argc; j++) {
        o = getDecodedObject(argv[j]);

        // 组合 $<length>\r\n
        buf[0] = '$';
        len = 1 + ll2string(buf + 1, sizeof(buf) - 1, sdslen(o->ptr));
        buf[len++] = '\r';
        buf[len++] = '\n';
        dst = sdscatlen(dst, buf, len);

        // 组合 <content>\r\n
        dst = sdscatlen(dst, o->ptr, sdslen(o->ptr));
        dst = sdscatlen(dst, "\r\n", 2);

        decrRefCount(o);
    }

    // 返回重建后的协议内容
    return dst;
}

/* Create the sds representation of a PEXPIREAT command, using
 * 'seconds' as time to live and 'cmd' to understand what command
 * we are translating into a PEXPIREAT.
 *
 * 创建 PEXPIREAT 命令的 sds 表示，
 * cmd 参数用于指定转换的源指令， seconds 为 TTL （剩余生存时间）。
 *
 * This command is used in order to translate EXPIRE and PEXPIRE commands
 * into PEXPIREAT command so that we retain precision in the append only
 * file, and the time is always absolute and not relative.
 *
 * 这个函数用于将 EXPIRE 、 PEXPIRE 和 EXPIREAT 转换为 PEXPIREAT 
 * 从而在保证精确度不变的情况下，将过期时间从相对值转换为绝对值（一个 UNIX 时间戳）。
 *
 * （过期时间必须是绝对值，这样不管 AOF 文件何时被载入，该过期的 key 都会正确地过期。）
 */
sds catAppendOnlyExpireAtCommand(sds buf, struct redisCommand *cmd, robj *key, robj *seconds) {
    long long when;
    robj *argv[3];

    /* Make sure we can use strtoll 
	 *
     * 取出过期值
     */
    seconds = getDecodedObject(seconds);
    when = strtoll(seconds->ptr, NULL, 10);
    /* Convert argument into milliseconds for EXPIRE, SETEX, EXPIREAT
    *
    * 如果过期值的格式为秒，那么将它转换为毫秒
    */
    if (cmd->proc == expireCommand || cmd->proc == setexCommand ||
        cmd->proc == expireatCommand) {
        when *= 1000;
    }
    /* Convert into absolute time for EXPIRE, PEXPIRE, SETEX, PSETEX 
     *
     * 如果过期值的格式为相对值，那么将它转换为绝对值
     */
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == setexCommand || cmd->proc == psetexCommand) {
        when += mstime();
    }
    decrRefCount(seconds);
    // 构建 PEXPIREAT 命令
    argv[0] = shared.pexpireat;
    argv[1] = key;
    argv[2] = createStringObjectFromLongLong(when);
    // 追加到 AOF 缓存中
    buf = catAppendOnlyGenericCommand(buf, 3, argv);
    decrRefCount(argv[2]);
    return buf;
}

/*
 * 将命令追加到 AOF 文件中，
 * 如果 AOF 重写正在进行，那么也将命令追加到 AOF 重写缓存中。
 */
void feedAppendOnlyFile(struct redisCommand *cmd, int dictid, robj **argv, int argc) {
    sds buf = sdsempty();
    /* The DB this command was targeting is not the same as the last command
     * we appended. To issue a SELECT command is needed. 
     *
     * 使用 SELECT 命令，显式设置数据库，确保之后的命令被设置到正确的数据库
     */
    if (dictid != server.aof_selected_db) {
        char seldb[64];

        snprintf(seldb, sizeof(seldb), "%d", dictid);
        buf = sdscatprintf(buf, "*2\r\n$6\r\nSELECT\r\n$%lu\r\n%s\r\n",
                           (unsigned long) strlen(seldb), seldb);
        server.aof_selected_db = dictid;
    }

    // EXPIRE 、 PEXPIRE 和 EXPIREAT 命令
    if (cmd->proc == expireCommand || cmd->proc == pexpireCommand ||
        cmd->proc == expireatCommand) {
        /* Translate EXPIRE/PEXPIRE/EXPIREAT into PEXPIREAT 
         *
         * 将 EXPIRE 、 PEXPIRE 和 EXPIREAT 都翻译成 PEXPIREAT
         */
        buf = catAppendOnlyExpireAtCommand(buf, cmd, argv[1], argv[2]);


        // SETEX 和 PSETEX 命令
    } else if (cmd->proc == setCommand && argc > 3) {
        robj *pxarg = NULL;
        /* When SET is used with EX/PX argument setGenericCommand propagates them with PX millisecond argument.
         * So since the command arguments are re-written there, we can rely here on the index of PX being 3. */
        if (!strcasecmp(argv[3]->ptr, "px")) {
            pxarg = argv[4];
        }
        /* For AOF we convert SET key value relative time in milliseconds to SET key value absolute time in
         * millisecond. Whenever the condition is true it implies that original SET has been transformed
         * to SET PX with millisecond time argument so we do not need to worry about unit here.*/
        if (pxarg) {
            robj *millisecond = getDecodedObject(pxarg);
            long long when = strtoll(millisecond->ptr, NULL, 10);
            when += mstime();

            decrRefCount(millisecond);

            robj *newargs[5];
            newargs[0] = argv[0];
            newargs[1] = argv[1];
            newargs[2] = argv[2];
            newargs[3] = shared.pxat;
            newargs[4] = createStringObjectFromLongLong(when);
            buf = catAppendOnlyGenericCommand(buf, 5, newargs);
            decrRefCount(newargs[4]);
        } else {
            buf = catAppendOnlyGenericCommand(buf, argc, argv);
        }

        // 其他命令
    } else {
        /* All the other commands don't need translation or need the
         * same translation already operated in the command vector
         * for the replication itself. */
        buf = catAppendOnlyGenericCommand(buf, argc, argv);
    }

    /* Append to the AOF buffer. This will be flushed on disk just before
     * of re-entering the event loop, so before the client will get a
     * positive reply about the operation performed. 
     *
     * 将命令追加到 AOF 缓存中，
     * 在重新进入事件循环之前，这些命令会被冲洗到磁盘上，
     * 并向客户端返回一个回复。
     */
    if (server.aof_state == AOF_ON)
        server.aof_buf = sdscatlen(server.aof_buf, buf, sdslen(buf));

    /* If a background append only file rewriting is in progress we want to
     * accumulate the differences between the child DB and the current one
     * in a buffer, so that when the child process will do its work we
     * can append the differences to the new append only file. 
     *
     * 如果 BGREWRITEAOF 正在进行，
     * 那么我们还需要将命令追加到重写缓存中，
     * 从而记录当前正在重写的 AOF 文件和数据库当前状态的差异。
     */
    if (server.child_type == CHILD_TYPE_AOF)
        aofRewriteBufferAppend((unsigned char *) buf, sdslen(buf));

    // 释放
    sdsfree(buf);
}

/* ----------------------------------------------------------------------------
 * AOF loading
 * ------------------------------------------------------------------------- */

/* In Redis commands are always executed in the context of a client, so in
 * order to load the append only file we need to create a fake client. 
 *
 * Redis 命令必须由客户端执行，
 * 所以 AOF 装载程序需要创建一个无网络连接的客户端来执行 AOF 文件中的命令。
 */
struct client *createAOFClient(void) {
    struct client *c = zmalloc(sizeof(*c));

    selectDb(c, 0);
    c->id = CLIENT_ID_AOF; /* So modules can identify it's the AOF client. */
    c->conn = NULL;
    c->name = NULL;
    c->querybuf = sdsempty();
    c->querybuf_peak = 0;
    c->argc = 0;
    c->argv = NULL;
    c->original_argc = 0;
    c->original_argv = NULL;
    c->argv_len_sum = 0;
    c->bufpos = 0;

    /*
     * The AOF client should never be blocked (unlike master
     * replication connection).
     * This is because blocking the AOF client might cause
     * deadlock (because potentially no one will unblock it).
     * Also, if the AOF client will be blocked just for
     * background processing there is a chance that the
     * command execution order will be violated.
     */
    c->flags = CLIENT_DENY_BLOCKING;

    c->btype = BLOCKED_NONE;
    /* We set the fake client as a slave waiting for the synchronization
     * so that Redis will not try to send replies to this client. 
     *
     * 将客户端设置为正在等待同步的附属节点，这样客户端就不会发送回复了。
     */
    c->replstate = SLAVE_STATE_WAIT_BGSAVE_START;
    c->reply = listCreate();
    c->reply_bytes = 0;
    c->obuf_soft_limit_reached_time = 0;
    c->watched_keys = listCreate();
    c->peerid = NULL;
    c->sockname = NULL;
    c->resp = 2;
    c->user = NULL;
    listSetFreeMethod(c->reply, freeClientReplyValue);
    listSetDupMethod(c->reply, dupClientReplyValue);
    initClientMultiState(c);
    return c;
}


/*
 * 释放伪客户端
 */
void freeFakeClientArgv(struct client *c) {
    int j;
    // 释放查询缓存

    for (j = 0; j < c->argc; j++)
        decrRefCount(c->argv[j]);
    zfree(c->argv);
    c->argv_len_sum = 0;
}

void freeFakeClient(struct client *c) {
    sdsfree(c->querybuf);
    // 释放回复缓存
    listRelease(c->reply);
    // 释放监视的键
    listRelease(c->watched_keys);
    // 释放事务状态
    freeClientMultiState(c);
    freeClientOriginalArgv(c);
    zfree(c);
}

/* Replay the append log file. On success C_OK is returned. On non fatal
 * error (the append only file is zero-length) C_ERR is returned. On
 * fatal error an error message is logged and the program exists.
 *
 * 执行 AOF 文件中的命令。
 *
 * 出错时返回 REDIS_OK 。
 *
 * 出现非执行错误（比如文件长度为 0 ）时返回 REDIS_ERR 。
 *
 * 出现致命错误时打印信息到日志，并且程序退出。
 */
int loadAppendOnlyFile(char *filename) {

    // 为客户端
    struct client *fakeClient;
    // 打开 AOF 文件
    FILE *fp = fopen(filename, "r");
    struct redis_stat sb;
    int old_aof_state = server.aof_state;
    long loops = 0;
    off_t valid_up_to = 0; /* Offset of latest well-formed command loaded. */
    off_t valid_before_multi = 0; /* Offset before MULTI command loaded. */

    if (fp == NULL) {
        serverLog(LL_WARNING, "Fatal error: can't open the append log file for reading: %s", strerror(errno));
        exit(1);
    }

    /* Handle a zero-length AOF file as a special case. An empty AOF file
     * is a valid AOF because an empty server with AOF enabled will create
     * a zero length file at startup, that will remain like that if no write
     * operation is received. */
    // 检查文件的正确性
    if (fp && redis_fstat(fileno(fp), &sb) != -1 && sb.st_size == 0) {
        server.aof_current_size = 0;
        server.aof_fsync_offset = server.aof_current_size;
        fclose(fp);
        return C_ERR;
    }

    /* Temporarily disable AOF, to prevent EXEC from feeding a MULTI
     * to the same file we're about to read. 
     *
     * 暂时性地关闭 AOF ，防止在执行 MULTI 时，
     * EXEC 命令被传播到正在打开的 AOF 文件中。
     */
    server.aof_state = AOF_OFF;

    fakeClient = createAOFClient();
    // 设置服务器的状态为：正在载入
    // startLoading 定义于 rdb.c
    startLoadingFile(fp, filename, RDBFLAGS_AOF_PREAMBLE);

    /* Check if this AOF file has an RDB preamble. In that case we need to
     * load the RDB file and later continue loading the AOF tail. */
    char sig[5]; /* "REDIS" */
    if (fread(sig, 1, 5, fp) != 5 || memcmp(sig, "REDIS", 5) != 0) {
        /* No RDB preamble, seek back at 0 offset. */
        if (fseek(fp, 0, SEEK_SET) == -1) goto readerr;
    } else {
        /* RDB preamble. Pass loading the RDB functions. */
        rio rdb;

        serverLog(LL_NOTICE, "Reading RDB preamble from AOF file...");
        if (fseek(fp, 0, SEEK_SET) == -1) goto readerr;
        rioInitWithFile(&rdb, fp);
        if (rdbLoadRio(&rdb, RDBFLAGS_AOF_PREAMBLE, NULL) != C_OK) {
            serverLog(LL_WARNING, "Error reading the RDB preamble of the AOF file, AOF loading aborted");
            goto readerr;
        } else {
            serverLog(LL_NOTICE, "Reading the remaining AOF tail...");
        }
    }

    /* Read the actual AOF file, in REPL format, command by command. */
    while (1) {
        int argc, j;
        unsigned long len;
        robj **argv;
        char buf[128];
        sds argsds;
        struct redisCommand *cmd;

        /* Serve the clients from time to time 
         *
         * 间隔性地处理客户端发送来的请求
         * 因为服务器正处于载入状态，所以能正常执行的只有 PUBSUB 等模块
         */
        if (!(loops++ % 1000)) {
            loadingProgress(ftello(fp));
            processEventsWhileBlocked();
            processModuleLoadingProgressEvent(1);
        }

        // 读入文件内容到缓存
        if (fgets(buf, sizeof(buf), fp) == NULL) {
            if (feof(fp))
                // 文件已经读完，跳出
                break;
            else
                goto readerr;
        }
        // 确认协议格式，比如 *3\r\n
        if (buf[0] != '*') goto fmterr;
        if (buf[1] == '\0') goto readerr;
        // 取出命令参数，比如 *3\r\n 中的 3
        argc = atoi(buf + 1);
        // 至少要有一个参数（被调用的命令）
        if (argc < 1) goto fmterr;

        /* Load the next command in the AOF as our fake client
         * argv. */

        // 从文本中创建字符串对象：包括命令，以及命令参数
        // 例如 $3\r\nSET\r\n$3\r\nKEY\r\n$5\r\nVALUE\r\n
        // 将创建三个包含以下内容的字符串对象：
        // SET 、 KEY 、 VALUE
        argv = zmalloc(sizeof(robj *) * argc);
        fakeClient->argc = argc;
        fakeClient->argv = argv;

        for (j = 0; j < argc; j++) {
            /* Parse the argument len. */
            char *readres = fgets(buf, sizeof(buf), fp);
            if (readres == NULL || buf[0] != '$') {
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                if (readres == NULL)
                    goto readerr;
                else
                    goto fmterr;
            }

            // 读取参数值的长度
            len = strtol(buf + 1, NULL, 10);

            /* Read it into a string object. */
            // 读取参数值
            argsds = sdsnewlen(SDS_NOINIT, len);
            if (len && fread(argsds, len, 1, fp) == 0) {
                sdsfree(argsds);
                fakeClient->argc = j; /* Free up to j-1. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
            // 为参数创建对象
            argv[j] = createObject(OBJ_STRING, argsds);


            /* Discard CRLF. */
            if (fread(buf, 2, 1, fp) == 0) {
                fakeClient->argc = j + 1; /* Free up to j. */
                freeFakeClientArgv(fakeClient);
                goto readerr;
            }
        }

        /* Command lookup 
         *
         * 查找命令
         */
        cmd = lookupCommand(argv[0]->ptr);
        if (!cmd) {
            serverLog(LL_WARNING,
                      "Unknown command '%s' reading the append only file",
                      (char *) argv[0]->ptr);
            exit(1);
        }

        if (cmd == server.multiCommand) valid_before_multi = valid_up_to;

        /* Run the command in the context of a fake client 
         *
         * 调用伪客户端，执行命令
         */
        fakeClient->cmd = fakeClient->lastcmd = cmd;
        if (fakeClient->flags & CLIENT_MULTI &&
            fakeClient->cmd->proc != execCommand) {
            queueMultiCommand(fakeClient);
        } else {
            cmd->proc(fakeClient);
        }

        /* The fake client should not have a reply */
        serverAssert(fakeClient->bufpos == 0 &&
                     listLength(fakeClient->reply) == 0);

        /* The fake client should never get blocked */
        serverAssert((fakeClient->flags & CLIENT_BLOCKED) == 0);

        /* Clean up. Command code may have changed argv/argc so we use the
         * argv/argc of the client instead of the local variables. 
         *
         * 清理命令和命令参数对象
         */
        freeFakeClientArgv(fakeClient);
        fakeClient->cmd = NULL;
        if (server.aof_load_truncated) valid_up_to = ftello(fp);
        if (server.key_load_delay)
            debugDelay(server.key_load_delay);
    }

    /* This point can only be reached when EOF is reached without errors.
     * If the client is in the middle of a MULTI/EXEC, handle it as it was
     * a short read, even if technically the protocol is correct: we want
     * to remove the unprocessed tail and continue. 
     * 如果能执行到这里，说明 AOF 文件的全部内容都可以正确地读取，
     * 但是，还要检查 AOF 是否包含未正确结束的事务
     */
    if (fakeClient->flags & CLIENT_MULTI) {
        serverLog(LL_WARNING,
                  "Revert incomplete MULTI/EXEC transaction in AOF file");
        valid_up_to = valid_before_multi;
        goto uxeof;
    }

    loaded_ok: /* DB loaded, cleanup and return C_OK to the caller. */

    // 关闭 AOF 文件
    fclose(fp);
    // 释放伪客户端
    freeFakeClient(fakeClient);
    // 复原 AOF 状态
    server.aof_state = old_aof_state;
    // 停止载入
    stopLoading(1);
    // 更新服务器状态中， AOF 文件的当前大小
    aofUpdateCurrentSize();
    server.aof_rewrite_base_size = server.aof_current_size;
    // 记录前一次重写时的大小
    server.aof_fsync_offset = server.aof_current_size;
    return C_OK;

// 读入错误
    readerr: /* Read error. If feof(fp) is true, fall through to unexpected EOF. */

    // 非预期的末尾，可能是 AOF 文件在写入的中途遭遇了停机
    if (!feof(fp)) {
        if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
        fclose(fp);
        serverLog(LL_WARNING, "Unrecoverable error reading the append only file: %s", strerror(errno));
        exit(1);
    }

    uxeof: /* Unexpected AOF end of file. */
    if (server.aof_load_truncated) {
        serverLog(LL_WARNING, "!!! Warning: short read while loading the AOF file !!!");
        serverLog(LL_WARNING, "!!! Truncating the AOF at offset %llu !!!",
                  (unsigned long long) valid_up_to);
        if (valid_up_to == -1 || truncate(filename, valid_up_to) == -1) {
            if (valid_up_to == -1) {
                serverLog(LL_WARNING, "Last valid command offset is invalid");
            } else {
                serverLog(LL_WARNING, "Error truncating the AOF file: %s",
                          strerror(errno));
            }
        } else {
            /* Make sure the AOF file descriptor points to the end of the
             * file after the truncate call. */
            if (server.aof_fd != -1 && lseek(server.aof_fd, 0, SEEK_END) == -1) {
                serverLog(LL_WARNING, "Can't seek the end of the AOF file: %s",
                          strerror(errno));
            } else {
                serverLog(LL_WARNING,
                          "AOF loaded anyway because aof-load-truncated is enabled");
                goto loaded_ok;
            }
        }
    }
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    fclose(fp);
    serverLog(LL_WARNING,
              "Unexpected end of file reading the append only file. You can: 1) Make a backup of your AOF file, then use ./redis-check-aof --fix <filename>. 2) Alternatively you can set the 'aof-load-truncated' configuration option to yes and restart the server.");
    exit(1);

// 内容格式错误
    fmterr: /* Format error. */
    if (fakeClient) freeFakeClient(fakeClient); /* avoid valgrind warning */
    fclose(fp);
    serverLog(LL_WARNING,
              "Bad file format reading the append only file: make a backup of your AOF file, then use ./redis-check-aof --fix <filename>");
    exit(1);
}

/* ----------------------------------------------------------------------------
 * AOF rewrite
 * ------------------------------------------------------------------------- */

/* Delegate writing an object to writing a bulk string or bulk long long.
 * This is not placed in rio.c since that adds the server.h dependency. 
 *
 * 将 obj 所指向的整数对象或字符串对象的值写入到 r 当中。
 */
int rioWriteBulkObject(rio *r, robj *obj) {
    /* Avoid using getDecodedObject to help copy-on-write (we are often
     * in a child process when this function is called). */
    if (obj->encoding == OBJ_ENCODING_INT) {
        return rioWriteBulkLongLong(r, (long) obj->ptr);
    } else if (sdsEncodedObject(obj)) {
        return rioWriteBulkString(r, obj->ptr, sdslen(obj->ptr));
    } else {
        serverPanic("Unknown string encoding");
    }
}

/* Emit the commands needed to rebuild a list object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建列表对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：  RPUSH item1 item2 ... itemN
 */
int rewriteListObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = listTypeLength(o);

    if (o->encoding == OBJ_ENCODING_QUICKLIST) {
        quicklist *list = o->ptr;
        quicklistIter *li = quicklistGetIterator(list, AL_START_HEAD);
        quicklistEntry entry;
        // 先构建一个 RPUSH key 
        // 然后从 ZIPLIST 中取出最多 REDIS_AOF_REWRITE_ITEMS_PER_CMD 个元素
        // 之后重复第一步，直到 ZIPLIST 为空
        while (quicklistNext(li, &entry)) {
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;
                if (!rioWriteBulkCount(r, '*', 2 + cmd_items) ||
                    !rioWriteBulkString(r, "RPUSH", 5) ||
                    !rioWriteBulkObject(r, key)) {
                    quicklistReleaseIterator(li);
                    return 0;
                }
            }
            // 取出值
            if (entry.value) {
                if (!rioWriteBulkString(r, (char *) entry.value, entry.sz)) {
                    quicklistReleaseIterator(li);
                    return 0;
                }
            } else {
                if (!rioWriteBulkLongLong(r, entry.longval)) {
                    quicklistReleaseIterator(li);
                    return 0;
                }
            }
            // 移动指针，并计算被取出元素的数量
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        quicklistReleaseIterator(li);
    } else {
        serverPanic("Unknown list encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a set object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建集合对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：  SADD item1 item2 ... itemN
 */
int rewriteSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = setTypeSize(o);

    if (o->encoding == OBJ_ENCODING_INTSET) {
        int ii = 0;
        int64_t llval;

        while (intsetGet(o->ptr, ii++, &llval)) {
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r, '*', 2 + cmd_items) ||
                    !rioWriteBulkString(r, "SADD", 4) ||
                    !rioWriteBulkObject(r, key)) {
                    return 0;
                }
            }
            if (!rioWriteBulkLongLong(r, llval)) return 0;
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == OBJ_ENCODING_HT) {
        dictIterator *di = dictGetIterator(o->ptr);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r, '*', 2 + cmd_items) ||
                    !rioWriteBulkString(r, "SADD", 4) ||
                    !rioWriteBulkObject(r, key)) {
                    dictReleaseIterator(di);
                    return 0;
                }
            }
            if (!rioWriteBulkString(r, ele, sdslen(ele))) {
                dictReleaseIterator(di);
                return 0;
            }
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown set encoding");
    }
    return 1;
}

/* Emit the commands needed to rebuild a sorted set object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建有序集合对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：  ZADD score1 member1 score2 member2 ... scoreN memberN
 */
int rewriteSortedSetObject(rio *r, robj *key, robj *o) {
    long long count = 0, items = zsetLength(o);

    if (o->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *zl = o->ptr;
        unsigned char *eptr, *sptr;
        unsigned char *vstr;
        unsigned int vlen;
        long long vll;
        double score;

        eptr = ziplistIndex(zl, 0);
        serverAssert(eptr != NULL);
        sptr = ziplistNext(zl, eptr);
        serverAssert(sptr != NULL);

        while (eptr != NULL) {
            serverAssert(ziplistGet(eptr, &vstr, &vlen, &vll));
            score = zzlGetScore(sptr);

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r, '*', 2 + cmd_items * 2) ||
                    !rioWriteBulkString(r, "ZADD", 4) ||
                    !rioWriteBulkObject(r, key)) {
                    return 0;
                }
            }
            if (!rioWriteBulkDouble(r, score)) return 0;
            if (vstr != NULL) {
                if (!rioWriteBulkString(r, (char *) vstr, vlen)) return 0;
            } else {
                if (!rioWriteBulkLongLong(r, vll)) return 0;
            }
            zzlNext(zl, &eptr, &sptr);
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
    } else if (o->encoding == OBJ_ENCODING_SKIPLIST) {
        zset *zs = o->ptr;
        dictIterator *di = dictGetIterator(zs->dict);
        dictEntry *de;

        while ((de = dictNext(di)) != NULL) {
            sds ele = dictGetKey(de);
            double *score = dictGetVal(de);

            if (count == 0) {
                int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                                AOF_REWRITE_ITEMS_PER_CMD : items;

                if (!rioWriteBulkCount(r, '*', 2 + cmd_items * 2) ||
                    !rioWriteBulkString(r, "ZADD", 4) ||
                    !rioWriteBulkObject(r, key)) {
                    dictReleaseIterator(di);
                    return 0;
                }
            }
            if (!rioWriteBulkDouble(r, *score) ||
                !rioWriteBulkString(r, ele, sdslen(ele))) {
                dictReleaseIterator(di);
                return 0;
            }
            if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
            items--;
        }
        dictReleaseIterator(di);
    } else {
        serverPanic("Unknown sorted zset encoding");
    }
    return 1;
}

/* Write either the key or the value of the currently selected item of a hash.
 *
 * 选择写入哈希的 key 或者 value 到 r 中。
 *
 * The 'hi' argument passes a valid Redis hash iterator.
 *
 * hi 为 Redis 哈希迭代器
 *
 * The 'what' filed specifies if to write a key or a value and can be
 * either OBJ_HASH_KEY or OBJ_HASH_VALUE.
 *
 * what 决定了要写入的部分，可以是 REDIS_HASH_KEY 或 REDIS_HASH_VALUE
 *
 * The function returns 0 on error, non-zero on success. 
 *
 * 出错返回 0 ，成功返回非 0 。
 */
static int rioWriteHashIteratorCursor(rio *r, hashTypeIterator *hi, int what) {
    if (hi->encoding == OBJ_ENCODING_ZIPLIST) {
        unsigned char *vstr = NULL;
        unsigned int vlen = UINT_MAX;
        long long vll = LLONG_MAX;

        hashTypeCurrentFromZiplist(hi, what, &vstr, &vlen, &vll);
        if (vstr)
            return rioWriteBulkString(r, (char *) vstr, vlen);
        else
            return rioWriteBulkLongLong(r, vll);
    } else if (hi->encoding == OBJ_ENCODING_HT) {
        sds value = hashTypeCurrentFromHashTable(hi, what);
        return rioWriteBulkString(r, value, sdslen(value));
    }

    serverPanic("Unknown hash encoding");
    return 0;
}

/* Emit the commands needed to rebuild a hash object.
 * The function returns 0 on error, 1 on success. 
 *
 * 将重建哈希对象所需的命令写入到 r 。
 *
 * 出错返回 0 ，成功返回 1 。
 *
 * 命令的形式如下：HMSET field1 value1 field2 value2 ... fieldN valueN
 */
int rewriteHashObject(rio *r, robj *key, robj *o) {
    hashTypeIterator *hi;
    long long count = 0, items = hashTypeLength(o);

    hi = hashTypeInitIterator(o);
    while (hashTypeNext(hi) != C_ERR) {
        if (count == 0) {
            int cmd_items = (items > AOF_REWRITE_ITEMS_PER_CMD) ?
                            AOF_REWRITE_ITEMS_PER_CMD : items;

            if (!rioWriteBulkCount(r, '*', 2 + cmd_items * 2) ||
                !rioWriteBulkString(r, "HMSET", 5) ||
                !rioWriteBulkObject(r, key)) {
                hashTypeReleaseIterator(hi);
                return 0;
            }
        }

        if (!rioWriteHashIteratorCursor(r, hi, OBJ_HASH_KEY) ||
            !rioWriteHashIteratorCursor(r, hi, OBJ_HASH_VALUE)) {
            hashTypeReleaseIterator(hi);
            return 0;
        }
        if (++count == AOF_REWRITE_ITEMS_PER_CMD) count = 0;
        items--;
    }

    hashTypeReleaseIterator(hi);

    return 1;
}

/* Helper for rewriteStreamObject() that generates a bulk string into the
 * AOF representing the ID 'id'. */
int rioWriteBulkStreamID(rio *r, streamID *id) {
    int retval;

    sds replyid = sdscatfmt(sdsempty(), "%U-%U", id->ms, id->seq);
    retval = rioWriteBulkString(r, replyid, sdslen(replyid));
    sdsfree(replyid);
    return retval;
}

/* Helper for rewriteStreamObject(): emit the XCLAIM needed in order to
 * add the message described by 'nack' having the id 'rawid', into the pending
 * list of the specified consumer. All this in the context of the specified
 * key and group. */
int rioWriteStreamPendingEntry(rio *r, robj *key, const char *groupname, size_t groupname_len, streamConsumer *consumer,
                               unsigned char *rawid, streamNACK *nack) {
    /* XCLAIM <key> <group> <consumer> 0 <id> TIME <milliseconds-unix-time>
              RETRYCOUNT <count> JUSTID FORCE. */
    streamID id;
    streamDecodeID(rawid, &id);
    if (rioWriteBulkCount(r, '*', 12) == 0) return 0;
    if (rioWriteBulkString(r, "XCLAIM", 6) == 0) return 0;
    if (rioWriteBulkObject(r, key) == 0) return 0;
    if (rioWriteBulkString(r, groupname, groupname_len) == 0) return 0;
    if (rioWriteBulkString(r, consumer->name, sdslen(consumer->name)) == 0) return 0;
    if (rioWriteBulkString(r, "0", 1) == 0) return 0;
    if (rioWriteBulkStreamID(r, &id) == 0) return 0;
    if (rioWriteBulkString(r, "TIME", 4) == 0) return 0;
    if (rioWriteBulkLongLong(r, nack->delivery_time) == 0) return 0;
    if (rioWriteBulkString(r, "RETRYCOUNT", 10) == 0) return 0;
    if (rioWriteBulkLongLong(r, nack->delivery_count) == 0) return 0;
    if (rioWriteBulkString(r, "JUSTID", 6) == 0) return 0;
    if (rioWriteBulkString(r, "FORCE", 5) == 0) return 0;
    return 1;
}

/* Helper for rewriteStreamObject(): emit the XGROUP CREATECONSUMER is
 * needed in order to create consumers that do not have any pending entries.
 * All this in the context of the specified key and group. */
int
rioWriteStreamEmptyConsumer(rio *r, robj *key, const char *groupname, size_t groupname_len, streamConsumer *consumer) {
    /* XGROUP CREATECONSUMER <key> <group> <consumer> */
    if (rioWriteBulkCount(r, '*', 5) == 0) return 0;
    if (rioWriteBulkString(r, "XGROUP", 6) == 0) return 0;
    if (rioWriteBulkString(r, "CREATECONSUMER", 14) == 0) return 0;
    if (rioWriteBulkObject(r, key) == 0) return 0;
    if (rioWriteBulkString(r, groupname, groupname_len) == 0) return 0;
    if (rioWriteBulkString(r, consumer->name, sdslen(consumer->name)) == 0) return 0;
    return 1;
}

/* Emit the commands needed to rebuild a stream object.
 * The function returns 0 on error, 1 on success. */
int rewriteStreamObject(rio *r, robj *key, robj *o) {
    stream *s = o->ptr;
    streamIterator si;
    streamIteratorStart(&si, s, NULL, NULL, 0);
    streamID id;
    int64_t numfields;

    if (s->length) {
        /* Reconstruct the stream data using XADD commands. */
        while (streamIteratorGetID(&si, &id, &numfields)) {
            /* Emit a two elements array for each item. The first is
             * the ID, the second is an array of field-value pairs. */

            /* Emit the XADD <key> <id> ...fields... command. */
            if (!rioWriteBulkCount(r, '*', 3 + numfields * 2) ||
                !rioWriteBulkString(r, "XADD", 4) ||
                !rioWriteBulkObject(r, key) ||
                !rioWriteBulkStreamID(r, &id)) {
                streamIteratorStop(&si);
                return 0;
            }
            while (numfields--) {
                unsigned char *field, *value;
                int64_t field_len, value_len;
                streamIteratorGetField(&si, &field, &value, &field_len, &value_len);
                if (!rioWriteBulkString(r, (char *) field, field_len) ||
                    !rioWriteBulkString(r, (char *) value, value_len)) {
                    streamIteratorStop(&si);
                    return 0;
                }
            }
        }
    } else {
        /* Use the XADD MAXLEN 0 trick to generate an empty stream if
         * the key we are serializing is an empty string, which is possible
         * for the Stream type. */
        id.ms = 0;
        id.seq = 1;
        if (!rioWriteBulkCount(r, '*', 7) ||
            !rioWriteBulkString(r, "XADD", 4) ||
            !rioWriteBulkObject(r, key) ||
            !rioWriteBulkString(r, "MAXLEN", 6) ||
            !rioWriteBulkString(r, "0", 1) ||
            !rioWriteBulkStreamID(r, &id) ||
            !rioWriteBulkString(r, "x", 1) ||
            !rioWriteBulkString(r, "y", 1)) {
            streamIteratorStop(&si);
            return 0;
        }
    }

    /* Append XSETID after XADD, make sure lastid is correct,
     * in case of XDEL lastid. */
    if (!rioWriteBulkCount(r, '*', 3) ||
        !rioWriteBulkString(r, "XSETID", 6) ||
        !rioWriteBulkObject(r, key) ||
        !rioWriteBulkStreamID(r, &s->last_id)) {
        streamIteratorStop(&si);
        return 0;
    }


    /* Create all the stream consumer groups. */
    if (s->cgroups) {
        raxIterator ri;
        raxStart(&ri, s->cgroups);
        raxSeek(&ri, "^", NULL, 0);
        while (raxNext(&ri)) {
            streamCG *group = ri.data;
            /* Emit the XGROUP CREATE in order to create the group. */
            if (!rioWriteBulkCount(r, '*', 5) ||
                !rioWriteBulkString(r, "XGROUP", 6) ||
                !rioWriteBulkString(r, "CREATE", 6) ||
                !rioWriteBulkObject(r, key) ||
                !rioWriteBulkString(r, (char *) ri.key, ri.key_len) ||
                !rioWriteBulkStreamID(r, &group->last_id)) {
                raxStop(&ri);
                streamIteratorStop(&si);
                return 0;
            }

            /* Generate XCLAIMs for each consumer that happens to
             * have pending entries. Empty consumers would be generated with
             * XGROUP CREATECONSUMER. */
            raxIterator ri_cons;
            raxStart(&ri_cons, group->consumers);
            raxSeek(&ri_cons, "^", NULL, 0);
            while (raxNext(&ri_cons)) {
                streamConsumer *consumer = ri_cons.data;
                /* If there are no pending entries, just emit XGROUP CREATECONSUMER */
                if (raxSize(consumer->pel) == 0) {
                    if (rioWriteStreamEmptyConsumer(r, key, (char *) ri.key,
                                                    ri.key_len, consumer) == 0) {
                        raxStop(&ri_cons);
                        raxStop(&ri);
                        streamIteratorStop(&si);
                        return 0;
                    }
                    continue;
                }
                /* For the current consumer, iterate all the PEL entries
                 * to emit the XCLAIM protocol. */
                raxIterator ri_pel;
                raxStart(&ri_pel, consumer->pel);
                raxSeek(&ri_pel, "^", NULL, 0);
                while (raxNext(&ri_pel)) {
                    streamNACK *nack = ri_pel.data;
                    if (rioWriteStreamPendingEntry(r, key, (char *) ri.key,
                                                   ri.key_len, consumer,
                                                   ri_pel.key, nack) == 0) {
                        raxStop(&ri_pel);
                        raxStop(&ri_cons);
                        raxStop(&ri);
                        streamIteratorStop(&si);
                        return 0;
                    }
                }
                raxStop(&ri_pel);
            }
            raxStop(&ri_cons);
        }
        raxStop(&ri);
    }

    streamIteratorStop(&si);
    return 1;
}

/* Call the module type callback in order to rewrite a data type
 * that is exported by a module and is not handled by Redis itself.
 * The function returns 0 on error, 1 on success. */
int rewriteModuleObject(rio *r, robj *key, robj *o) {
    RedisModuleIO io;
    moduleValue *mv = o->ptr;
    moduleType *mt = mv->type;
    moduleInitIOContext(io, mt, r, key);
    mt->aof_rewrite(&io, key, mv->value);
    if (io.ctx) {
        moduleFreeContext(io.ctx);
        zfree(io.ctx);
    }
    return io.error ? 0 : 1;
}

/* This function is called by the child rewriting the AOF file to read
 * the difference accumulated from the parent into a buffer, that is
 * concatenated at the end of the rewrite. */
ssize_t aofReadDiffFromParent(void) {
    char buf[65536]; /* Default pipe buffer size on most Linux systems. */
    ssize_t nread, total = 0;

    while ((nread =
                    read(server.aof_pipe_read_data_from_parent, buf, sizeof(buf))) > 0) {
        server.aof_child_diff = sdscatlen(server.aof_child_diff, buf, nread);
        total += nread;
    }
    return total;
}

int rewriteAppendOnlyFileRio(rio *aof) {
    dictIterator *di = NULL;
    dictEntry *de;
    size_t processed = 0;
    int j;
    long key_count = 0;
    long long updated_time = 0;
    // 遍历所有的数据库
    for (j = 0; j < server.dbnum; j++) {
        char selectcmd[] = "*2\r\n$6\r\nSELECT\r\n";
        redisDb *db = server.db + j;
        // 指向键空间
        dict *d = db->dict;
        if (dictSize(d) == 0) continue;
        // 创建键空间迭代器
        di = dictGetSafeIterator(d);

        /* SELECT the new DB 
        *
         * 首先写入 SELECT 命令，确保之后的数据会被插入到正确的数据库上
         */
        if (rioWrite(aof, selectcmd, sizeof(selectcmd) - 1) == 0) goto werr;
        if (rioWriteBulkLongLong(aof, j) == 0) goto werr;
        // 遍历dict
        /* Iterate this DB writing every entry 
         * 遍历数据库所有键，并通过命令将它们的当前状态（值）记录到新 AOF 文件中
         */
        while ((de = dictNext(di)) != NULL) {
            sds keystr;
            robj key, *o;
            long long expiretime;

            // 取出键
            keystr = dictGetKey(de);

            // 取出值
            o = dictGetVal(de);
            initStaticStringObject(key, keystr);

            // 取出过期时间
            expiretime = getExpire(db, &key);
            // 根据value类型，进行对应的重写逻辑
            /* Save the key and associated value */
            if (o->type == OBJ_STRING) {
                /* Emit a SET command */
                char cmd[] = "*3\r\n$3\r\nSET\r\n";
                if (rioWrite(aof, cmd, sizeof(cmd) - 1) == 0) goto werr;
                /* Key and value */
                if (rioWriteBulkObject(aof, &key) == 0) goto werr;
                if (rioWriteBulkObject(aof, o) == 0) goto werr;
            } else if (o->type == OBJ_LIST) {
                if (rewriteListObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_SET) {
                if (rewriteSetObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_ZSET) {
                if (rewriteSortedSetObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_HASH) {
                if (rewriteHashObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_STREAM) {
                if (rewriteStreamObject(aof, &key, o) == 0) goto werr;
            } else if (o->type == OBJ_MODULE) {
                if (rewriteModuleObject(aof, &key, o) == 0) goto werr;
            } else {
                serverPanic("Unknown object type");
            }
            /* Save the expire time */
            /* Save the expire time 
             *
             * 保存键的过期时间
             */
            if (expiretime != -1) {
                char cmd[] = "*3\r\n$9\r\nPEXPIREAT\r\n";
                // 写入 PEXPIREAT expiretime 命令
                if (rioWrite(aof, cmd, sizeof(cmd) - 1) == 0) goto werr;
                if (rioWriteBulkObject(aof, &key) == 0) goto werr;
                if (rioWriteBulkLongLong(aof, expiretime) == 0) goto werr;
            }
            /* Read some diff from the parent process from time to time. */
            if (aof->processed_bytes > processed + AOF_READ_DIFF_INTERVAL_BYTES) {
                processed = aof->processed_bytes;
                aofReadDiffFromParent();
            }

            /* Update info every 1 second (approximately).
             * in order to avoid calling mstime() on each iteration, we will
             * check the diff every 1024 keys */
            if ((key_count++ & 1023) == 0) {
                long long now = mstime();
                if (now - updated_time >= 1000) {
                    sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, "AOF rewrite");
                    updated_time = now;
                }
            }
        }
        // 释放迭代器
        dictReleaseIterator(di);
        di = NULL;
    }
    return C_OK;

    werr:
    if (di) dictReleaseIterator(di);
    return C_ERR;
}

/* Write a sequence of commands able to fully rebuild the dataset into
 * "filename". Used both by REWRITEAOF and BGREWRITEAOF.
 *
 * 将一集足以还原当前数据集的命令写入到 filename 指定的文件中。
 *
 * 这个函数被 REWRITEAOF 和 BGREWRITEAOF 两个命令调用。
 * （REWRITEAOF 似乎已经是一个废弃的命令）
 *
 * In order to minimize the number of commands needed in the rewritten
 * log Redis uses variadic commands when possible, such as RPUSH, SADD
 * and ZADD. However at max AOF_REWRITE_ITEMS_PER_CMD items per time
 * are inserted using a single command. 
 *
 * 为了最小化重建数据集所需执行的命令数量，
 * Redis 会尽可能地使用接受可变参数数量的命令，比如 RPUSH 、SADD 和 ZADD 等。
 *
 * 不过单个命令每次处理的元素数量不能超过 REDIS_AOF_REWRITE_ITEMS_PER_CMD 。
 */
int rewriteAppendOnlyFile(char *filename) {
    rio aof;
    FILE *fp = NULL;
    char tmpfile[256];
    char byte;

    /* Note that we have to use a different temp name here compared to the
     * one used by rewriteAppendOnlyFileBackground() function. 
     * 创建临时文件
     *
     * 注意这里创建的文件名和 rewriteAppendOnlyFileBackground() 创建的文件名稍有不同
     */
    snprintf(tmpfile, 256, "temp-rewriteaof-%d.aof", (int) getpid());
    fp = fopen(tmpfile, "w");
    if (!fp) {
        serverLog(LL_WARNING, "Opening the temp file for AOF rewrite in rewriteAppendOnlyFile(): %s", strerror(errno));
        return C_ERR;
    }

    server.aof_child_diff = sdsempty();
    // 初始化文件 io
    rioInitWithFile(&aof, fp);

    // 设置每写入 REDIS_AOF_AUTOSYNC_BYTES 字节 32M
    // 就执行一次 FSYNC 
    // 防止缓存中积累太多命令内容，造成 I/O 阻塞时间过长
    if (server.aof_rewrite_incremental_fsync)
        rioSetAutoSync(&aof, REDIS_AUTOSYNC_BYTES);

    startSaving(RDBFLAGS_AOF_PREAMBLE);

    if (server.aof_use_rdb_preamble) {
        int error;
        // 写RDB是人类不可读的
        if (rdbSaveRio(&aof, &error, RDBFLAGS_AOF_PREAMBLE, NULL) == C_ERR) {
            errno = error;
            goto werr;
        }
    } else {
        // 执行重写操作到临时aof
        // 写AOF 可读，虽然都是从当前内存中整理出来
        if (rewriteAppendOnlyFileRio(&aof) == C_ERR) goto werr;
    }

    /* Do an initial slow fsync here while the parent is still sending
     * data, in order to make the next final fsync faster. */
    if (fflush(fp) == EOF) goto werr;
    if (fsync(fileno(fp)) == -1) goto werr;
    // 重写期间，从父进程的重写缓冲区获取部分写命令

    /* Read again a few times to get more data from the parent.
     * We can't read forever (the server may receive data from clients
     * faster than it is able to send data to the child), so we try to read
     * some more data in a loop as soon as there is a good chance more data
     * will come. If it looks like we are wasting time, we abort (this
     * happens after 20 ms without new data). */
    int nodata = 0;
    mstime_t start = mstime();
    // 最多等待1秒。 nodata < 20; 以小批量读取父进程数据
    while (mstime() - start < 1000 && nodata < 20) {
        if (aeWait(server.aof_pipe_read_data_from_parent, AE_READABLE, 1) <= 0) {
            nodata++;
            continue;
        }
        nodata = 0; /* Start counting from zero, we stop on N *contiguous*
                       timeouts. */
        // 增量日志
        aofReadDiffFromParent();
    }

    /* Ask the master to stop sending diffs. */
    if (write(server.aof_pipe_write_ack_to_parent, "!", 1) != 1) goto werr;
    if (anetNonBlock(NULL, server.aof_pipe_read_ack_from_parent) != ANET_OK)
        goto werr;
    /* We read the ACK from the server using a 5 seconds timeout. Normally
     * it should reply ASAP, but just in case we lose its reply, we are sure
     * the child will eventually get terminated. */
    // 询问父进程是否可通信
    if (syncRead(server.aof_pipe_read_ack_from_parent, &byte, 1, 5000) != 1 ||
        byte != '!')
        goto werr;
    serverLog(LL_NOTICE, "Parent agreed to stop sending diffs. Finalizing AOF...");

    /* Read the final diff if any. */
    // 读取增量缓冲
    aofReadDiffFromParent();

    /* Write the received diff to the file. */
    // 打印写入增量的缓冲大小
    serverLog(LL_NOTICE,
              "Concatenating %.2f MB of AOF diff received from parent.",
              (double) sdslen(server.aof_child_diff) / (1024 * 1024));

    /* Now we write the entire AOF buffer we received from the parent
     * via the pipe during the life of this fork child.
     * once a second, we'll take a break and send updated COW info to the parent */
    size_t bytes_to_write = sdslen(server.aof_child_diff);
    const char *buf = server.aof_child_diff;
    long long cow_updated_time = mstime();
    long long key_count = dbTotalServerKeyCount();
    // 写增量日志到 临时aof
    while (bytes_to_write) {
        /* We write the AOF buffer in chunk of 8MB so that we can check the time in between them */
        size_t chunk_size = bytes_to_write < (8 << 20) ? bytes_to_write : (8 << 20);

        if (rioWrite(&aof, buf, chunk_size) == 0)
            goto werr;

        bytes_to_write -= chunk_size;
        buf += chunk_size;

        /* Update COW info */
        long long now = mstime();
        if (now - cow_updated_time >= 1000) {
            sendChildInfo(CHILD_INFO_TYPE_CURRENT_INFO, key_count, "AOF rewrite");
            cow_updated_time = now;
        }
    }

    // 马上刷新到磁盘
    /* Make sure data will not remain on the OS's output buffers */
    if (fflush(fp)) goto werr;
    if (fsync(fileno(fp))) goto werr;
    if (fclose(fp)) {
        fp = NULL;
        goto werr;
    }
    fp = NULL;

    /* Use RENAME to make sure the DB file is changed atomically only
     * if the generate DB file is ok. */
    //  重命名aof文件 "temp-rewriteaof-%d.aof" => "temp-rewriteaof-bg-%d.aof"
    if (rename(tmpfile, filename) == -1) {
        serverLog(LL_WARNING, "Error moving temp append only file on the final destination: %s", strerror(errno));
        unlink(tmpfile);
        stopSaving(0);
        return C_ERR;
    }
    // 写aof完成
    serverLog(LL_NOTICE, "SYNC append only file rewrite performed");
    stopSaving(1);
    return C_OK;

    werr:
    serverLog(LL_WARNING, "Write error writing append only file on disk: %s", strerror(errno));
    if (fp) fclose(fp);
    unlink(tmpfile);
    stopSaving(0);
    return C_ERR;
}

/* ----------------------------------------------------------------------------
 * AOF rewrite pipes for IPC
 * -------------------------------------------------------------------------- */

/* This event handler is called when the AOF rewriting child sends us a
 * single '!' char to signal we should stop sending buffer diffs. The
 * parent sends a '!' as well to acknowledge. */
void aofChildPipeReadable(aeEventLoop *el, int fd, void *privdata, int mask) {
    char byte;
    UNUSED(el);
    UNUSED(privdata);
    UNUSED(mask);

    if (read(fd, &byte, 1) == 1 && byte == '!') {
        serverLog(LL_NOTICE, "AOF rewrite child asks to stop sending diffs.");
        server.aof_stop_sending_diff = 1;
        if (write(server.aof_pipe_write_ack_to_child, "!", 1) != 1) {
            /* If we can't send the ack, inform the user, but don't try again
             * since in the other side the children will use a timeout if the
             * kernel can't buffer our write, or, the children was
             * terminated. */
            serverLog(LL_WARNING, "Can't send ACK to AOF child: %s",
                      strerror(errno));
        }
    }
    /* Remove the handler since this can be called only one time during a
     * rewrite. */
    aeDeleteFileEvent(server.el, server.aof_pipe_read_ack_from_child, AE_READABLE);
}

/* Create the pipes used for parent - child process IPC during rewrite.
 * We have a data pipe used to send AOF incremental diffs to the child,
 * and two other pipes used by the children to signal it finished with
 * the rewrite so no more data should be written, and another for the
 * parent to acknowledge it understood this new condition. */
int aofCreatePipes(void) {
    int fds[6] = {-1, -1, -1, -1, -1, -1};
    int j;

    if (pipe(fds) == -1) goto error; /* parent -> children data. */
    if (pipe(fds + 2) == -1) goto error; /* children -> parent ack. */
    if (pipe(fds + 4) == -1) goto error; /* parent -> children ack. */
    /* Parent -> children data is non blocking. */
    if (anetNonBlock(NULL, fds[0]) != ANET_OK) goto error;
    if (anetNonBlock(NULL, fds[1]) != ANET_OK) goto error;
    if (aeCreateFileEvent(server.el, fds[2], AE_READABLE, aofChildPipeReadable, NULL) == AE_ERR) goto error;

    server.aof_pipe_write_data_to_child = fds[1];
    server.aof_pipe_read_data_from_parent = fds[0];
    server.aof_pipe_write_ack_to_parent = fds[3];
    server.aof_pipe_read_ack_from_child = fds[2];
    server.aof_pipe_write_ack_to_child = fds[5];
    server.aof_pipe_read_ack_from_parent = fds[4];
    server.aof_stop_sending_diff = 0;
    return C_OK;

    error:
    serverLog(LL_WARNING, "Error opening /setting AOF rewrite IPC pipes: %s",
              strerror(errno));
    for (j = 0; j < 6; j++) if (fds[j] != -1) close(fds[j]);
    return C_ERR;
}

void aofClosePipes(void) {
    aeDeleteFileEvent(server.el, server.aof_pipe_read_ack_from_child, AE_READABLE);
    aeDeleteFileEvent(server.el, server.aof_pipe_write_data_to_child, AE_WRITABLE);
    close(server.aof_pipe_write_data_to_child);
    close(server.aof_pipe_read_data_from_parent);
    close(server.aof_pipe_write_ack_to_parent);
    close(server.aof_pipe_read_ack_from_child);
    close(server.aof_pipe_write_ack_to_child);
    close(server.aof_pipe_read_ack_from_parent);
}

/* ----------------------------------------------------------------------------
 * AOF background rewrite
 * ------------------------------------------------------------------------- */

/* This is how rewriting of the append only file in background works:
 * 
 * 以下是后台重写 AOF 文件（BGREWRITEAOF）的工作步骤：
 *
 * 1) The user calls BGREWRITEAOF
 *    用户调用 BGREWRITEAOF
 *
 * 2) Redis calls this function, that forks():
 *    Redis 调用这个函数，它执行 fork() ：
 *
 *    2a) the child rewrite the append only file in a temp file.
 *        子进程在临时文件中对 AOF 文件进行重写
 *
 *    2b) the parent accumulates differences in server.aof_rewrite_buf.
 *        父进程将新输入的写命令追加到 server.aof_rewrite_buf 中
 *
 * 3) When the child finished '2a' exists.
 *    当步骤 2a 执行完之后，子进程结束
 *
 * 4) The parent will trap the exit code, if it's OK, will append the
 *    data accumulated into server.aof_rewrite_buf into the temp file, and
 *    finally will rename(2) the temp file in the actual file name.
 *    The the new file is reopened as the new append only file. Profit!
 *
 *    父进程会捕捉子进程的退出信号，
 *    如果子进程的退出状态是 OK 的话，
 *    那么父进程将新输入命令的缓存追加到临时文件，
 *    然后使用 rename(2) 对临时文件改名，用它代替旧的 AOF 文件，
 *    至此，后台 AOF 重写完成。
 */
int rewriteAppendOnlyFileBackground(void) {
    pid_t childpid;
    // 已经有进程在进行 AOF 重写了
    if (hasActiveChildProcess()) return C_ERR;

    // 记录 fork 开始前的时间，计算 fork 耗时用
    if (aofCreatePipes() != C_OK) return C_ERR;
    if ((childpid = redisFork(CHILD_TYPE_AOF)) == 0) {
        char tmpfile[256];

        /* Child */
        // 关闭网络连接 fd
        // 为进程设置名字，方便记认
        redisSetProcTitle("redis-aof-rewrite");
        redisSetCpuAffinity(server.aof_rewrite_cpulist);
        // 创建临时文件，并进行 AOF 重写
        snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int) getpid());
        // 开始重写
        if (rewriteAppendOnlyFile(tmpfile) == C_OK) {
            // 发送子进程 Cow 数据给父进程
            sendChildCowInfo(CHILD_INFO_TYPE_AOF_COW_SIZE, "AOF rewrite");
            // 发送重写成功信号
            exitFromChild(0);
        } else {
            // 发送重写失败信号
            exitFromChild(1);
        }
    } else {
        /* Parent */
        // 记录执行 fork 所消耗的时间
        if (childpid == -1) {
            serverLog(LL_WARNING,
                      "Can't rewrite append only file in background: fork: %s",
                      strerror(errno));
            aofClosePipes();
            return C_ERR;
        }
        serverLog(LL_NOTICE,
                  "Background append only file rewriting started by pid %ld", (long) childpid);
        // 记录 AOF 重写的信息
        server.aof_rewrite_scheduled = 0;
        server.aof_rewrite_time_start = time(NULL);

        /* We set appendseldb to -1 in order to force the next call to the
         * feedAppendOnlyFile() to issue a SELECT command, so the differences
         * accumulated by the parent into server.aof_rewrite_buf will start
         * with a SELECT statement and it will be safe to merge. 
         *
         * 将 aof_selected_db 设为 -1 ，
         * 强制让 feedAppendOnlyFile() 下次执行时引发一个 SELECT 命令，
         * 从而确保之后新添加的命令会设置到正确的数据库中
         */
        server.aof_selected_db = -1;
        replicationScriptCacheFlush();
        return C_OK;
    }
    return C_OK; /* unreached */
}

void bgrewriteaofCommand(client *c) {

    // 不能重复运行 BGREWRITEAOF
    if (server.child_type == CHILD_TYPE_AOF) {
        addReplyError(c, "Background append only file rewriting already in progress");


        // 如果正在执行 BGSAVE ，那么预定 BGREWRITEAOF
        // 等 BGSAVE 完成之后， BGREWRITEAOF 就会开始执行
    } else if (hasActiveChildProcess()) {
        server.aof_rewrite_scheduled = 1;
        addReplyStatus(c, "Background append only file rewriting scheduled");


        // 执行 BGREWRITEAOF
    } else if (rewriteAppendOnlyFileBackground() == C_OK) {
        addReplyStatus(c, "Background append only file rewriting started");
    } else {
        addReplyError(c, "Can't execute an AOF background rewriting. "
                         "Please check the server logs for more information.");
    }
}

/*
 * 删除 AOF 重写所产生的临时文件
 */
void aofRemoveTempFile(pid_t childpid) {
    char tmpfile[256];

    snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof", (int) childpid);
    bg_unlink(tmpfile);

    snprintf(tmpfile, 256, "temp-rewriteaof-%d.aof", (int) childpid);
    bg_unlink(tmpfile);
}

/* Update the server.aof_current_size field explicitly using stat(2)
 * to check the size of the file. This is useful after a rewrite or after
 * a restart, normally the size is updated just adding the write length
 * to the current length, that is much faster. 
 *
 * 将 aof 文件的当前大小记录到服务器状态中。
 *
 * 通常用于 BGREWRITEAOF 执行之后，或者服务器重启之后。
 */
void aofUpdateCurrentSize(void) {
    struct redis_stat sb;
    mstime_t latency;

    // 读取文件状态
    latencyStartMonitor(latency);
    if (redis_fstat(server.aof_fd, &sb) == -1) {
        serverLog(LL_WARNING, "Unable to obtain the AOF file length. stat: %s",
                  strerror(errno));
    } else {
        // 设置到服务器
        server.aof_current_size = sb.st_size;
    }
    latencyEndMonitor(latency);
    latencyAddSampleIfNeeded("aof-fstat", latency);
}

/* A background append only file rewriting (BGREWRITEAOF) terminated its work.
 * Handle this. 
 *
 * 当子线程完成 AOF 重写时，父进程调用这个函数。
 */
void backgroundRewriteDoneHandler(int exitcode, int bysignal) {
    if (!bysignal && exitcode == 0) {
        int newfd, oldfd;
        char tmpfile[256];
        long long now = ustime();
        mstime_t latency;

        serverLog(LL_NOTICE,
                  "Background AOF rewrite terminated with success");

        /* Flush the differences accumulated by the parent to the
         * rewritten AOF. */
        // 打开保存新 AOF 文件内容的临时文件
        latencyStartMonitor(latency);
        snprintf(tmpfile, 256, "temp-rewriteaof-bg-%d.aof",
                 (int) server.child_pid);
        newfd = open(tmpfile, O_WRONLY | O_APPEND);
        if (newfd == -1) {
            serverLog(LL_WARNING,
                      "Unable to open the temporary AOF produced by the child: %s", strerror(errno));
            goto cleanup;
        }
        // 开始将重写缓冲区的数据写入到重写AOF文件
        // 这个函数调用的 write 操作会阻塞主进程
        if (aofRewriteBufferWrite(newfd) == -1) {
            serverLog(LL_WARNING,
                      "Error trying to flush the parent diff to the rewritten AOF: %s", strerror(errno));
            close(newfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        latencyAddSampleIfNeeded("aof-rewrite-diff-write", latency);

        if (server.aof_fsync == AOF_FSYNC_EVERYSEC) {
            // 起个后台进程 异步 刷新到磁盘
            aof_background_fsync(newfd);
        } else if (server.aof_fsync == AOF_FSYNC_ALWAYS) {
            latencyStartMonitor(latency);
            if (redis_fsync(newfd) == -1) {
                serverLog(LL_WARNING,
                          "Error trying to fsync the parent diff to the rewritten AOF: %s", strerror(errno));
                close(newfd);
                goto cleanup;
            }
            latencyEndMonitor(latency);
            latencyAddSampleIfNeeded("aof-rewrite-done-fsync", latency);
        }

        serverLog(LL_NOTICE,
                  "Residual parent diff successfully flushed to the rewritten AOF (%.2f MB)",
                  (double) aofRewriteBufferSize() / (1024 * 1024));

        /* The only remaining thing to do is to rename the temporary file to
         * the configured file and switch the file descriptor used to do AOF
         * writes. We don't want close(2) or rename(2) calls to block the
         * server on old file deletion.
         *
         * 剩下的工作就是将临时文件改名为 AOF 程序指定的文件名，
         * 并将新文件的 fd 设为 AOF 程序的写目标。
         *
         * 不过这里有一个问题 ——
         * 我们不想 close(2) 或者 rename(2) 在删除旧文件时阻塞。
         *
         * There are two possible scenarios:
         *
         * 以下是两个可能的场景：
         * 1) AOF is DISABLED and this was a one time rewrite. The temporary
         * file will be renamed to the configured file. When this file already
         * exists, it will be unlinked, which may block the server.
         *
         * AOF 被关闭，这个是一次单次的写操作。
         * 临时文件会被改名为 AOF 文件。
         * 本来已经存在的 AOF 文件会被 unlink ，这可能会阻塞服务器。
         *
         * 2) AOF is ENABLED and the rewritten AOF will immediately start
         * receiving writes. After the temporary file is renamed to the
         * configured file, the original AOF file descriptor will be closed.
         * Since this will be the last reference to that file, closing it
         * causes the underlying file to be unlinked, which may block the
         * server.
         *
         * AOF 被开启，并且重写后的 AOF 文件会立即被用于接收新的写入命令。
         * 当临时文件被改名为 AOF 文件时，原来的 AOF 文件描述符会被关闭。
         * 因为 Redis 会是最后一个引用这个文件的进程，
         * 所以关闭这个文件会引起 unlink ，这可能会阻塞服务器。
         *
         * To mitigate the blocking effect of the unlink operation (either
         * caused by rename(2) in scenario 1, or by close(2) in scenario 2), we
         * use a background thread to take care of this. First, we
         * make scenario 1 identical to scenario 2 by opening the target file
         * when it exists. The unlink operation after the rename(2) will then
         * be executed upon calling close(2) for its descriptor. Everything to
         * guarantee atomicity for this switch has already happened by then, so
         * we don't care what the outcome or duration of that close operation
         * is, as long as the file descriptor is released again. 
         * 为了避免出现阻塞现象，程序会将 close(2) 放到后台线程执行，
         * 这样服务器就可以持续处理请求，不会被中断。
         */
        if (server.aof_fd == -1) {
            /* AOF disabled */

            /* Don't care if this fails: oldfd will be -1 and we handle that.
             * One notable case of -1 return is if the old file does
             * not exist. */
            oldfd = open(server.aof_filename, O_RDONLY | O_NONBLOCK);
        } else {
            /* AOF enabled */
            oldfd = -1; /* We'll set this to the current AOF filedes later. */
        }

        /* Rename the temporary file. This will not unlink the target file if
         * it exists, because we reference it with "oldfd". */
        latencyStartMonitor(latency);
        // 开始覆盖旧的AOF文件
        // 旧的 AOF 文件不会在这里被 unlink ，因为 oldfd 引用了它
        if (rename(tmpfile, server.aof_filename) == -1) {
            serverLog(LL_WARNING,
                      "Error trying to rename the temporary AOF file %s into %s: %s",
                      tmpfile,
                      server.aof_filename,
                      strerror(errno));
            close(newfd);
            if (oldfd != -1) close(oldfd);
            goto cleanup;
        }
        latencyEndMonitor(latency);
        // 延迟采样
        latencyAddSampleIfNeeded("aof-rename", latency);

        if (server.aof_fd == -1) {
            /* AOF disabled, we don't need to set the AOF file descriptor
             * to this new file, so we can close it. 
             *
             * AOF 被关闭，直接关闭 AOF 文件，
             * 因为关闭 AOF 本来就会引起阻塞，所以这里就算 close 被阻塞也无所谓
             */
            close(newfd);
        } else {
            /* AOF enabled, replace the old fd with the new one. 
             *
             * 用新 AOF 文件的 fd 替换原来 AOF 文件的 fd
             */
            oldfd = server.aof_fd;
            server.aof_fd = newfd;
            // 强制引发 SELECT
            server.aof_selected_db = -1; /* Make sure SELECT is re-issued */

            // 更新 AOF 文件的大小
            aofUpdateCurrentSize();
            // 记录前一次重写时的大小
            server.aof_rewrite_base_size = server.aof_current_size;
            server.aof_fsync_offset = server.aof_current_size;
            server.aof_last_fsync = server.unixtime;

            /* Clear regular AOF buffer since its contents was just written to
             * the new AOF from the background rewrite buffer. 
             *
             * 清空 AOF 缓存，因为它的内容已经被写入过了，没用了
             */
            sdsfree(server.aof_buf);
            server.aof_buf = sdsempty();
        }

        server.aof_lastbgrewrite_status = C_OK;

        serverLog(LL_NOTICE, "Background AOF rewrite finished successfully");
        /* Change state from WAIT_REWRITE to ON if needed 
         *
         * 如果是第一次创建 AOF 文件，那么更新 AOF 状态
         */
        if (server.aof_state == AOF_WAIT_REWRITE)
            server.aof_state = AOF_ON;

        /* Asynchronously close the overwritten AOF. 
         *
         * 异步关闭旧 AOF 文件
         */
        if (oldfd != -1) bioCreateCloseJob(oldfd);

        serverLog(LL_VERBOSE,
                  "Background AOF rewrite signal handler took %lldus", ustime() - now);

        // BGREWRITEAOF 重写出错    } else if (!bysignal && exitcode != 0) {
        server.aof_lastbgrewrite_status = C_ERR;

        serverLog(LL_WARNING,
                  "Background AOF rewrite terminated with error");
    } else {
        /* SIGUSR1 is whitelisted, so we have a way to kill a child without
         * triggering an error condition. */
        if (bysignal != SIGUSR1)
            server.aof_lastbgrewrite_status = C_ERR;

        serverLog(LL_WARNING,
                  "Background AOF rewrite terminated by signal %d", bysignal);
    }

    cleanup:
    aofClosePipes();
    // 清空 AOF 缓冲区    aofRewriteBufferReset();

    // 移除临时文件
    aofRemoveTempFile(server.child_pid);

    // 重置默认属性    server.aof_rewrite_time_last = time(NULL)-server.aof_rewrite_time_start;
    server.aof_rewrite_time_start = -1;
    /* Schedule a new rewrite if we are waiting for it to switch the AOF ON. */
    if (server.aof_state == AOF_WAIT_REWRITE)
        server.aof_rewrite_scheduled = 1;
}
