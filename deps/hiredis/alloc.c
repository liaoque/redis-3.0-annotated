/*
 * Copyright (c) 2020, Michael Grunder <michael dot grunder at gmail dot com>
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
#include "alloc.h"
#include <string.h>
#include <stdlib.h>

hiredisAllocFuncs hiredisAllocFns = {
    .mallocFn = malloc,
    .callocFn = calloc,
//    先判断当前的指针是否有足够的连续空间，如果有，扩大mem_address指向的地址，
//    并且将mem_address返回，如果空间不够，先按照newsize指定的大小分配空间，
//    将原有数据从头到尾拷贝到新分配的内存区域，而后释放原来mem_address所指内存区域
//    （注意：原来指针是自动释放，不需要使用free），同时返回新分配的内存区域的首地址。即重新分配存储器块的地址。
    .reallocFn = realloc,
    .strdupFn = strdup,
    .freeFn = free,
};

/* Override hiredis' allocators with ones supplied by the user */
hiredisAllocFuncs hiredisSetAllocators(hiredisAllocFuncs *override) {
    hiredisAllocFuncs orig = hiredisAllocFns;

    hiredisAllocFns = *override;

    return orig;
}

/* Reset allocators to use libc defaults */
void hiredisResetAllocators(void) {
    hiredisAllocFns = (hiredisAllocFuncs) {
        .mallocFn = malloc,
        .callocFn = calloc,
        .reallocFn = realloc,
        .strdupFn = strdup,
        .freeFn = free,
    };
}

#ifdef _WIN32

void *hi_malloc(size_t size) {
    return hiredisAllocFns.mallocFn(size);
}

void *hi_calloc(size_t nmemb, size_t size) {
    return hiredisAllocFns.callocFn(nmemb, size);
}

void *hi_realloc(void *ptr, size_t size) {
    return hiredisAllocFns.reallocFn(ptr, size);
}

char *hi_strdup(const char *str) {
    return hiredisAllocFns.strdupFn(str);
}

void hi_free(void *ptr) {
    hiredisAllocFns.freeFn(ptr);
}

#endif
