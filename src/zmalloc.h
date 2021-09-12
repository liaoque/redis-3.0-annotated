/* zmalloc - total amount of allocated memory aware version of malloc()
 *
 * Copyright (c) 2009-2010, Salvatore Sanfilippo <antirez at gmail dot com>
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

#ifndef __ZMALLOC_H
#define __ZMALLOC_H

/* Double expansion needed for stringification of macro values. */
#define __xstr(s) __str(s)
#define __str(s) #s

/**
 *  tcmalloc：由google用于优化C++多线程应用而开发。Redis 需要1.6以上的版本。
 *  jemalloc：第一次用在FreeBSD 的allocator，于2005年释出的版本。强调降低碎片化，可扩展的并行支持。Redis需要2.1以上版本。
 *  libc：最常使用的libc库。GNU libc，默认使用此allocator。
 */
#if defined(USE_TCMALLOC)
#define ZMALLOC_LIB ("tcmalloc-" __xstr(TC_VERSION_MAJOR) "." __xstr(TC_VERSION_MINOR))
#include <google/tcmalloc.h>
#if (TC_VERSION_MAJOR == 1 && TC_VERSION_MINOR >= 6) || (TC_VERSION_MAJOR > 1)
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) tc_malloc_size(p)
#else
#error "Newer version of tcmalloc required"
#endif

#elif defined(USE_JEMALLOC)
#define ZMALLOC_LIB ("jemalloc-" __xstr(JEMALLOC_VERSION_MAJOR) "." __xstr(JEMALLOC_VERSION_MINOR) "." __xstr(JEMALLOC_VERSION_BUGFIX))
#include <jemalloc/jemalloc.h>
#if (JEMALLOC_VERSION_MAJOR == 2 && JEMALLOC_VERSION_MINOR >= 1) || (JEMALLOC_VERSION_MAJOR > 2)
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) je_malloc_usable_size(p)
#else
#error "Newer version of jemalloc required"
#endif

// 苹果os系统
#elif defined(__APPLE__)
#include <malloc/malloc.h>
#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) malloc_size(p)
#endif

/* On native libc implementations, we should still do our best to provide a
 * HAVE_MALLOC_SIZE capability. This can be set explicitly as well:
 *
 * NO_MALLOC_USABLE_SIZE disables it on all platforms, even if they are
 *      known to support it.
 * USE_MALLOC_USABLE_SIZE forces use of malloc_usable_size() regardless
 *      of platform.
 */
#ifndef ZMALLOC_LIB
#define ZMALLOC_LIB "libc"

// linux各种版本
#if !defined(NO_MALLOC_USABLE_SIZE) && \
    (defined(__GLIBC__) || defined(__FreeBSD__) || \
     defined(USE_MALLOC_USABLE_SIZE))

/* Includes for malloc_usable_size() */
#ifdef __FreeBSD__
#include <malloc_np.h>
#else
#include <malloc.h>
#endif

#define HAVE_MALLOC_SIZE 1
#define zmalloc_size(p) malloc_usable_size(p)

#endif
#endif

/* We can enable the Redis defrag capabilities only if we are using Jemalloc
 * and the version used is our special version modified for Redis having
 * the ability to return per-allocation fragmentation hints. */
#if defined(USE_JEMALLOC) && defined(JEMALLOC_FRAG_HINT)
#define HAVE_DEFRAG
#endif

void *zmalloc(size_t size); /* 申请size个大小的空间，失败会报错停止退出 */
void *zcalloc(size_t size); /* 调用系统函数calloc函数申请空间，失败会报错停止退出  */
void *zrealloc(void *ptr, size_t size); /* 原内存重新调整空间为size的大小，失败会报错停止退出  */
void *ztrymalloc(size_t size); /* 调用zmalloc申请size个大小的空间 */
void *ztrycalloc(size_t size);
void *ztryrealloc(void *ptr, size_t size); /* 原内存重新调整空间为size的大小 */
void zfree(void *ptr); /* 释放空间方法，并更新used_memory的值 */
void *zmalloc_usable(size_t size, size_t *usable);  /* 能返回实际申请内存大小的zmalloc，失败会报错停止退出  */
void *zcalloc_usable(size_t size, size_t *usable);
void *zrealloc_usable(void *ptr, size_t size, size_t *usable);  /* 能返回实际申请内存大小的zrealloc，失败会报错停止退出  */
void *ztrymalloc_usable(size_t size, size_t *usable); /* 申请size个大小的空间 内部最终*/
void *ztrycalloc_usable(size_t size, size_t *usable); /* 调用系统函数calloc函数申请空间 内部最终*/
void *ztryrealloc_usable(void *ptr, size_t size, size_t *usable);/*  内存调整  内部最终*/
void zfree_usable(void *ptr, size_t *usable); /* 释放并返回 ，释放大小 */
char *zstrdup(const char *s);/* 字符串复制方法 */
size_t zmalloc_used_memory(void); /* 获取当前已经占用的内存大小 */
void zmalloc_set_oom_handler(void (*oom_handler)(size_t));/* 自定义设置内存溢出的处理方法 */
void zlibc_free(void *ptr); /* 原始系统free释放方法 */
/**
 * jemalloc 数据采样
 * 以下不支持的平台数据是伪造
 */
size_t zmalloc_get_rss(void);/* 当前进程的获取驻留内存大小 */
int zmalloc_get_allocator_info(size_t *allocated, size_t *active, size_t *resident);
/*
 * 如果使用的是 JEMALLOC
 * https://docs.rs/jemalloc-ctl/0.3.3/jemalloc_ctl/stats/index.html
 * http://jemalloc.net/jemalloc.3.html#background_thread
 * active 应用程序分配的活动页面中的总字节数。
 * allocated 应用程序分配的总字节数。
 * resident 分配器映射的物理驻留数据页中的总字节数。
 * */
void set_jemalloc_bg_thread(int enable);/* 异步定期清理脏数据 */
int jemalloc_purge();/* 清除的脏数据 */

size_t zmalloc_get_private_dirty(long pid);/* 获取私有的脏数据大小 */
size_t zmalloc_get_smap_bytes_by_field(char *field, long pid); /* 从 libproc api 调用中获取指定字段的总和。 */
size_t zmalloc_get_memory_size(void);/* 以字节为单位返回物理内存 (RAM) 的大小。 */

#ifdef HAVE_DEFRAG
void zfree_no_tcache(void *ptr);
void *zmalloc_no_tcache(size_t size);
#endif

#ifndef HAVE_MALLOC_SIZE
size_t zmalloc_size(void *ptr);
size_t zmalloc_usable_size(void *ptr);
#else
#define zmalloc_usable_size(p) zmalloc_size(p)
#endif

#ifdef REDIS_TEST
int zmalloc_test(int argc, char **argv, int accurate);
#endif

#endif /* __ZMALLOC_H */
