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

/*-----------------------------------------------------------------------------
 * Set Commands
 *----------------------------------------------------------------------------*/

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op);

/* Factory method to return a set that *can* hold "value".
 *
 * 返回一个可以保存值 value 的集合。
 *
 * When the object has an integer-encodable value,
 * an intset will be returned. Otherwise a regular hash table.
 *
 * 当对象的值可以被编码为整数时，返回 intset ，
 * 否则，返回普通的哈希表。
 */
robj *setTypeCreate(sds value) {
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK)
        return createIntsetObject();
    return createSetObject();
}

/* Add the specified value into a set.
 * 多态 add 操作
 *
 * 添加成功返回 1 ，如果元素已经存在，返回 0 。
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
int setTypeAdd(robj *subject, sds value) {
    long long llval;

    // 字典
    if (subject->encoding == OBJ_ENCODING_HT) {
        // 将 value 作为键， NULL 作为值，将元素添加到字典中
        dict *ht = subject->ptr;
        dictEntry *de = dictAddRaw(ht,value,NULL);
        if (de) {
            dictSetKey(ht,de,sdsdup(value));
            dictSetVal(ht,de,NULL);
            return 1;
        }


        // intset
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {


        // 如果对象的值可以编码为整数的话，那么将对象的值添加到 intset 中
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            uint8_t success = 0;
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                // 添加成功
                // 检查集合在添加新元素之后是否需要转换为字典
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject,OBJ_ENCODING_HT);
                return 1;
            }
            // 如果对象的值不能编码为整数，那么将集合从 intset 编码转换为 HT 编码
            // 然后再执行添加操作
        } else {
            /* Failed to get integer from object, convert to regular set. */
            setTypeConvert(subject,OBJ_ENCODING_HT);

            /* The set *was* an intset and this value is not integer
             * encodable, so dictAdd should always work. */
            serverAssert(dictAdd(subject->ptr,sdsdup(value),NULL) == DICT_OK);
            return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    // 添加失败，元素已经存在
    return 0;
}


/*
 * 多态 remove 操作
 *
 * 删除成功返回 1 ，因为元素不存在而导致删除失败返回 0 。
 */
int setTypeRemove(robj *setobj, sds value) {
    long long llval;
    if (setobj->encoding == OBJ_ENCODING_HT) {
        // 从字典中删除键（集合元素）
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            // 看是否有必要在删除之后缩小字典的大小
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }

        // INTSET
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {

        // 如果对象的值可以编码为整数的话，那么尝试从 intset 中移除元素
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            int success;
            setobj->ptr = intsetRemove(setobj->ptr,llval,&success);
            if (success) return 1;
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}


/*
 * 多态 ismember 操作
 */
int setTypeIsMember(robj *subject, sds value) {
    long long llval;
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictFind((dict*)subject->ptr,value) != NULL;
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            return intsetFind((intset*)subject->ptr,llval);
        }
    } else {
        serverPanic("Unknown set encoding");
    }
    return 0;
}

/*
 * 创建并返回一个多态集合迭代器
 *
 * setTypeIterator 定义在 redis.h
 */
setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    // 指向被迭代的对象
    si->subject = subject;
    // 记录对象的编码
    si->encoding = subject->encoding;
    if (si->encoding == OBJ_ENCODING_HT) {
        // 字典迭代器
        si->di = dictGetIterator(subject->ptr);


        // INTSET
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        // 索引
        si->ii = 0;
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

/*
 * 释放迭代器
 */
void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * 取出被迭代器指向的当前集合元素。
 *
 * Since set elements can be internally be stored as redis objects or
 * simple arrays of integers, setTypeNext returns the encoding of the
 * set object you are iterating, and will populate the appropriate pointer
 * (sdsele) or (llele) accordingly.
 *
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 *
 * 因为集合即可以编码为 intset ，也可以编码为哈希表，
 * 所以程序会根据集合的编码，选择将值保存到那个参数里：
 *
 *  - 当编码为 intset 时，元素被指向到 llobj 参数
 *
 *  - 当编码为哈希表时，元素被指向到 eobj 参数
 *
 * 并且函数会返回被迭代集合的编码，方便识别。
 *
 * 当集合中的元素全部被迭代完毕时，函数返回 -1 。
 *
 * 因为被返回的对象是没有被增加引用计数的，
 * 所以这个函数是对 copy-on-write 友好的。
 * When there are no longer elements -1 is returned. */
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele) {

    // 从字典中取出对象
    if (si->encoding == OBJ_ENCODING_HT) {
        // 更新迭代器
        dictEntry *de = dictNext(si->di);
        // 字典已迭代完
        if (de == NULL) return -1;

        // 返回节点的键（集合的元素）
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */

        // 从 intset 中取出元素
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
    // 返回编码
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * setTypeNext 的非 copy-on-write 友好版本，
 * 总是返回一个新的、或者已经增加过引用计数的对象。
 *
 * 调用者在使用完对象之后，应该对对象调用 decrRefCount() 。
 *
 * This function is the way to go for write operations where COW is not
 * an issue.
 *
 * 这个函数应该在非 copy-on-write 时调用
*/
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    sds sdsele;
    int encoding;

    // 取出元素
    encoding = setTypeNext(si,&sdsele,&intele);
    // 总是为元素创建对象
    switch(encoding) {
        // 已为空
        case -1:    return NULL;
        // INTSET 返回一个整数值，需要为这个值创建对象
        case OBJ_ENCODING_INTSET:
            return sdsfromlonglong(intele);
            // HT 本身已经返回对象了，只需执行 incrRefCount()
            case OBJ_ENCODING_HT:
                return sdsdup(sdsele);
                default:
                    serverPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 *
 * 从非空集合中随机取出一个元素。
 *
 * The returned element can be an int64_t value if the set is encoded
 * as an "intset" blob of integers, or an SDS string if the set
 * is a regular set.
 *
 * 如果集合的编码为 intset ，那么将元素指向 int64_t 指针 llele 。
 * 如果集合的编码为 HT ，那么将元素对象指向对象指针 objele 。
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 * 函数的返回值为集合的编码方式，通过这个返回值可以知道那个指针保存了元素的值。
 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused.
 * 因为被返回的对象是没有被增加引用计数的，
 * 所以这个函数是对 copy-on-write 友好的。
*/
int setTypeRandomElement(robj *setobj, sds *sdsele, int64_t *llele) {
    if (setobj->encoding == OBJ_ENCODING_HT) {
        dictEntry *de = dictGetFairRandomKey(setobj->ptr);
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {
        *llele = intsetRandom(setobj->ptr);
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Unknown set encoding");
    }
    return setobj->encoding;
}


/*
 * 集合多态 size 函数
 */
unsigned long setTypeSize(const robj *subject) {
    if (subject->encoding == OBJ_ENCODING_HT) {
        return dictSize((const dict*)subject->ptr);
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {
        return intsetLen((const intset*)subject->ptr);
    } else {
        serverPanic("Unknown set encoding");
    }
}

/* Convert the set to specified encoding. The resulting dict (when converting
 *
 * 将集合对象 setobj 的编码转换为 REDIS_ENCODING_HT 。
 *
 * The resulting dict (when converting to a hash table)
 * is presized to hold the number of elements in the original set.
 *
 * 新创建的结果字典会被预先分配为和原来的集合一样大。
 */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;

    // 确认类型和编码正确
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET &&
    setobj->encoding == OBJ_ENCODING_INTSET);

    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
        // 创建新字典
        dict *d = dictCreate(&setDictType,NULL);
        sds element;

        /* Presize the dict to avoid rehashing */
        // 预先扩展空间
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        // 遍历集合，并将元素添加到字典中
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,&element,&intele) != -1) {
            element = sdsfromlonglong(intele);
            serverAssert(dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        // 更新集合的编码
        setobj->encoding = OBJ_ENCODING_HT;
        zfree(setobj->ptr);
        // 更新集合的值对象
        setobj->ptr = d;
    } else {
        serverPanic("Unsupported set conversion");
    }
}

/* This is a helper function for the COPY command.
 * Duplicate a set object, with the guarantee that the returned object
 * has the same encoding as the original one.
 *
 * The resulting object always has refcount set to 1 */
robj *setTypeDup(robj *o) {
    robj *set;
    setTypeIterator *si;
    sds elesds;
    int64_t intobj;

    serverAssert(o->type == OBJ_SET);

    /* Create a new set object that have the same encoding as the original object's encoding */
    if (o->encoding == OBJ_ENCODING_INTSET) {
        intset *is = o->ptr;
        size_t size = intsetBlobLen(is);
        intset *newis = zmalloc(size);
        memcpy(newis,is,size);
        set = createObject(OBJ_SET, newis);
        set->encoding = OBJ_ENCODING_INTSET;
    } else if (o->encoding == OBJ_ENCODING_HT) {
        set = createSetObject();
        dict *d = o->ptr;
        dictExpand(set->ptr, dictSize(d));
        si = setTypeInitIterator(o);
        while (setTypeNext(si, &elesds, &intobj) != -1) {
            setTypeAdd(set, elesds);
        }
        setTypeReleaseIterator(si);
    } else {
        serverPanic("Unknown set encoding");
    }
    return set;
}

void saddCommand(client *c) {
    robj *set;
    int j, added = 0;

    // 取出集合对象
    set = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,set,OBJ_SET)) return;
    // 对象不存在，创建一个新的，并将它关联到数据库
    if (set == NULL) {
        set = setTypeCreate(c->argv[2]->ptr);
        dbAdd(c->db,c->argv[1],set);
        // 对象存在，检查类型
    }

    // 将所有输入元素添加到集合中
    for (j = 2; j < c->argc; j++) {
        // 只有元素未存在于集合时，才算一次成功添加
        if (setTypeAdd(set,c->argv[j]->ptr)) added++;
    }
    // 如果有至少一个元素被成功添加，那么执行以下程序
    if (added) {
        // 发送键修改信号
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
    // 将数据库设为脏
    server.dirty += added;
    // 返回添加元素的数量
    addReplyLongLong(c,added);
}

void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    // 取出集合对象
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
    checkType(c,set,OBJ_SET)) return;

    // 删除输入的所有元素
    for (j = 2; j < c->argc; j++) {

        // 只有元素在集合中时，才算一次成功删除
        if (setTypeRemove(set,c->argv[j]->ptr)) {
            deleted++;
            // 如果集合已经为空，那么删除集合对象
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    // 如果有至少一个元素被成功删除，那么执行以下程序
    if (deleted) {

        // 发送键修改信号
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);
        if (keyremoved)
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],
                                c->db->id);
        server.dirty += deleted;
    }
    addReplyLongLong(c,deleted);
}

void smoveCommand(client *c) {
    robj *srcset, *dstset, *ele;
    // 取出源集合
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    // 取出目标集合
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    // 编码元素
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
    // 源集合不存在，直接返回
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    // 如果源集合的类型错误
    // 或者目标集合存在、但是类型错误
    // 那么直接返回
    if (checkType(c,srcset,OBJ_SET) ||
    checkType(c,dstset,OBJ_SET)) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    // 如果源集合和目标集合相等，那么直接返回
    if (srcset == dstset) {
        addReply(c,setTypeIsMember(srcset,ele->ptr) ?
        shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    // 从源集合中移除目标元素
    // 如果目标元素不存在于源集合中，那么直接返回
    if (!setTypeRemove(srcset,ele->ptr)) {
        addReply(c,shared.czero);
        return;
    }
    // 发送事件通知
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    // 如果源集合已经为空，那么将它从数据库中删除
    if (setTypeSize(srcset) == 0) {
        // 删除集合对象
        dbDelete(c->db,c->argv[1]);

        // 发送事件通知
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Create the destination set when it doesn't exist */
    if (!dstset) {
        dstset = setTypeCreate(ele->ptr);
        dbAdd(c->db,c->argv[2],dstset);
    }

    // 发送键修改信号
    signalModifiedKey(c,c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[2]);
    server.dirty++;

    // 将元素添加到目标集合
    /* An extra key has changed when ele was successfully added to dstset */
    if (setTypeAdd(dstset,ele->ptr)) {
        // 将数据库设为脏
        server.dirty++;
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
    // 回复 1
    addReply(c,shared.cone);
}

void sismemberCommand(client *c) {
    robj *set;

    // 取出集合对象
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
    checkType(c,set,OBJ_SET)) return;

    if (setTypeIsMember(set,c->argv[2]->ptr))
        addReply(c,shared.cone);
    else
        addReply(c,shared.czero);
}

void smismemberCommand(client *c) {
    robj *set;
    int j;

    /* Don't abort when the key cannot be found. Non-existing keys are empty
     * sets, where SMISMEMBER should respond with a series of zeros. */
    set = lookupKeyRead(c->db,c->argv[1]);
    if (set && checkType(c,set,OBJ_SET)) return;

    addReplyArrayLen(c,c->argc - 2);

    for (j = 2; j < c->argc; j++) {
        if (set && setTypeIsMember(set,c->argv[j]->ptr))
            addReply(c,shared.cone);
        else
            addReply(c,shared.czero);
    }
}

void scardCommand(client *c) {
    robj *o;

    // 取出集合
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
    checkType(c,o,OBJ_SET)) return;

    // 返回集合的基数
    addReplyLongLong(c,setTypeSize(o));
}

/* Handle the "SPOP key <count>" variant. The normal version of the
 * command is handled by the spopCommand() function itself. */

/* How many times bigger should be the set compared to the remaining size
 * for us to use the "create new set" strategy? Read later in the
 * implementation for more info. */
#define SPOP_MOVE_STRATEGY_MUL 5

void spopWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    robj *set;

    /* Get the count argument */
    if (getPositiveLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    count = (unsigned long) l;

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set. Otherwise, return nil */
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.emptyset[c->resp]))
    == NULL || checkType(c,set,OBJ_SET)) return;

    /* If count is zero, serve an empty set ASAP to avoid special
     * cases later. */
    if (count == 0) {
        addReply(c,shared.emptyset[c->resp]);
        return;
    }

    size = setTypeSize(set);

    /* Generate an SPOP keyspace notification */
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);
    server.dirty += (count >= size) ? size : count;

    /* CASE 1:
     * The number of requested elements is greater than or equal to
     * the number of elements inside the set: simply return the whole set. */
    if (count >= size) {
        /* We just return the entire set */
        sunionDiffGenericCommand(c,c->argv+1,1,NULL,SET_OP_UNION);

        /* Delete the set as it is now empty */
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);

        /* Propagate this command as a DEL operation */
        rewriteClientCommandVector(c,2,shared.del,c->argv[1]);
        signalModifiedKey(c,c->db,c->argv[1]);
        return;
    }

    /* Case 2 and 3 require to replicate SPOP as a set of SREM commands.
     * Prepare our replication argument vector. Also send the array length
     * which is common to both the code paths. */
    robj *propargv[3];
    propargv[0] = shared.srem;
    propargv[1] = c->argv[1];
    addReplySetLen(c,count);

    /* Common iteration vars. */
    sds sdsele;
    robj *objele;
    int encoding;
    int64_t llele;
    unsigned long remaining = size-count; /* Elements left after SPOP. */

    /* If we are here, the number of requested elements is less than the
     * number of elements inside the set. Also we are sure that count < size.
     * Use two different strategies.
     *
     * CASE 2: The number of elements to return is small compared to the
     * set size. We can just extract random elements and return them to
     * the set. */
    if (remaining*SPOP_MOVE_STRATEGY_MUL > count) {
        while(count--) {
            /* Emit and remove. */
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
                set->ptr = intsetRemove(set->ptr,llele,NULL);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
                setTypeRemove(set,sdsele);
            }

            /* Replicate/AOF this command as an SREM operation */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                          PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
    } else {
        /* CASE 3: The number of elements to return is very big, approaching
         * the size of the set itself. After some time extracting random elements
         * from such a set becomes computationally expensive, so we use
         * a different strategy, we extract random elements that we don't
         * want to return (the elements that will remain part of the set),
         * creating a new set as we do this (that will be stored as the original
         * set). Then we return the elements left in the original set and
         * release it. */
        robj *newset = NULL;

        /* Create a new set with just the remaining elements. */
        while(remaining--) {
            encoding = setTypeRandomElement(set,&sdsele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(sdsele);
            }
            if (!newset) newset = setTypeCreate(sdsele);
            setTypeAdd(newset,sdsele);
            setTypeRemove(set,sdsele);
            sdsfree(sdsele);
        }

        /* Transfer the old set to the client. */
        setTypeIterator *si;
        si = setTypeInitIterator(set);
        while((encoding = setTypeNext(si,&sdsele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
                objele = createStringObjectFromLongLong(llele);
            } else {
                addReplyBulkCBuffer(c,sdsele,sdslen(sdsele));
                objele = createStringObject(sdsele,sdslen(sdsele));
            }

            /* Replicate/AOF this command as an SREM operation */
            propargv[2] = objele;
            alsoPropagate(server.sremCommand,c->db->id,propargv,3,
                          PROPAGATE_AOF|PROPAGATE_REPL);
            decrRefCount(objele);
        }
        setTypeReleaseIterator(si);

        /* Assign the new set as the key value. */
        dbOverwrite(c->db,c->argv[1],newset);
    }

    /* Don't propagate the command itself even if we incremented the
     * dirty counter. We don't want to propagate an SPOP command since
     * we propagated the command as a set of SREMs operations using
     * the alsoPropagate() API. */
    preventCommandPropagation(c);
    signalModifiedKey(c,c->db,c->argv[1]);
}

void spopCommand(client *c) {
    robj *set, *ele;
    sds sdsele;
    int64_t llele;
    int encoding;

    if (c->argc == 3) {
        spopWithCountCommand(c);
        return;
    } else if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Make sure a key with the name inputted exists, and that it's type is
     * indeed a set */

    // 取出集合
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
    == NULL || checkType(c,set,OBJ_SET)) return;

    /* Get a random element from the set */
    // 从集合中随机取出一个元素
    encoding = setTypeRandomElement(set,&sdsele,&llele);

    /* Remove the element from the set */
    // 将被取出元素从集合中删除
    if (encoding == OBJ_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        ele = createStringObject(sdsele,sdslen(sdsele));
        setTypeRemove(set,ele->ptr);
    }
    // 发送事件通知
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */

    // 将这个命令当作 SREM 来传播，防止产生有害的随机性，导致数据不一致
    // （不同的服务器随机删除不同的元素）
    rewriteClientCommandVector(c,3,shared.srem,c->argv[1],ele);

    /* Add the element to the reply */
    addReplyBulk(c,ele);
    decrRefCount(ele);

    /* Delete the set if it's empty */
    if (setTypeSize(set) == 0) {
        dbDelete(c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Set has been modified */
    signalModifiedKey(c,c->db,c->argv[1]);
    server.dirty++;
}

/* handle the "SRANDMEMBER key <count>" variant. The normal version of the
 * command is handled by the srandmemberCommand() function itself.
 *
 * 实现 SRANDMEMBER key <count> 变种，
 * 原本的 SRANDMEMBER key 由 srandmemberCommand() 实现
 */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info.
 *
 * 如果 count 参数乘以这个常量所得的积，比集合的基数要大，
 * 那么程序就不使用“删除元素”的策略。
 *
 * 更多信息请参考后面的函数定义。
 */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    // 默认在集合中不包含重复元素
    int uniq = 1;
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    dict *d;

    // 取出 l 参数
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    if (l >= 0) {
        // l 为正数，表示返回元素各不相同
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
        // 如果 l 为负数，那么表示返回的结果中可以有重复元素
        count = -l;
        uniq = 0;
    }

    // 取出集合对象
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
    == NULL || checkType(c,set,OBJ_SET)) return;
    // 取出集合基数
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    // count 为 0 ，直接返回
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures.
     *
     * 情形 1：count 为负数，结果集可以带有重复元素
     * 直接从集合中取出并返回 N 个随机元素就可以了
     *
     * 这种情形不需要额外的结构来保存结果集
     * This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        addReplyArrayLen(c,count);
        while(count--) {
            // 取出随机元素
            encoding = setTypeRandomElement(set,&ele,&llele);
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
        }
        return;
    }

    /* CASE 2:
     * The number of requested elements is greater than the number of
     * elements inside the set: simply return the whole set.
     *
     * 如果 count 比集合的基数要大，那么直接返回整个集合
     */
    if (count >= size) {
        setTypeIterator *si;
        addReplyArrayLen(c,size);
        si = setTypeInitIterator(set);
        while ((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            if (encoding == OBJ_ENCODING_INTSET) {
                addReplyBulkLongLong(c,llele);
            } else {
                addReplyBulkCBuffer(c,ele,sdslen(ele));
            }
            size--;
        }
        setTypeReleaseIterator(si);
        serverAssert(size==0);
        return;
    }

    /* For CASE 3 and CASE 4 we need an auxiliary dictionary. */
    // 对于情形 3 和情形 4 ，需要一个额外的字典
    d = dictCreate(&sdsReplyDictType,NULL);

    /* CASE 3:
     *
     * 情形 3：
     *
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     *
     * count 参数乘以 SRANDMEMBER_SUB_STRATEGY_MUL 的积比集合的基数要大。
     *
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * 在这种情况下，程序创建一个集合的副本，
     * 并从集合中删除元素，直到集合的基数等于 count 参数指定的数量为止。
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient.
     *
     * 使用这种做法的原因是，当 count 的数量接近于集合的基数时，
     * 从集合中随机取出 count 个参数的方法是非常低效的。
     */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        // 遍历集合，将所有元素添加到临时字典中
        si = setTypeInitIterator(set);
        dictExpand(d, size);
        while ((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            // 为元素创建对象，并添加到字典中
            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,sdsfromlonglong(llele),NULL);
            } else {
                retval = dictAdd(d,sdsdup(ele),NULL);
            }
            serverAssert(retval == DICT_OK);
            /* 上面的代码可以重构为

            robj *elem_obj;
            if (encoding == REDIS_ENCODING_INTSET) {
                elem_obj = createStringObjectFromLongLong(...)
            } else if () {
                ...
            } else if () {
                ...
            }
            redisAssert(dictAdd(d, elem_obj) == DICT_OK)
            */
        }
        setTypeReleaseIterator(si);
        serverAssert(dictSize(d) == size);

        /* Remove random elements to reach the right count. */
        // 随机删除元素，直到集合基数等于 count 参数的值
        while (size > count) {
            dictEntry *de;
            // 取出并删除随机元素
            de = dictGetRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     *
     * 情形 4 ： count 参数要比集合基数小很多。
     *
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count.
     *
     * 在这种情况下，我们可以直接从集合中随机地取出元素，
     * 并将它添加到结果集合中，直到结果集的基数等于 count 为止。
     */
    else {
        unsigned long added = 0;
        sds sdsele;

        dictExpand(d, count);
        while (added < count) {
            // 随机地从目标集合中取出元素
            encoding = setTypeRandomElement(set,&ele,&llele);

            // 将元素转换为对象
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(ele);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            // 尝试将元素添加到字典中
            // dictAdd 只有在元素不存在于字典时，才会返回 1
            // 如果如果结果集已经有同样的元素，那么程序会执行 else 部分
            // 只有元素不存在于结果集时，添加才会成功
            if (dictAdd(d,sdsele,NULL) == DICT_OK)
                added++;
            else
                sdsfree(sdsele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    {
        // 情形 3 和 4 ：将结果集回复给客户端
        dictIterator *di;
        dictEntry *de;

        addReplyArrayLen(c,count);
        // 遍历结果集元素,
        di = dictGetIterator(d);
        // 回复
        while((de = dictNext(di)) != NULL)
            addReplyBulkSds(c,dictGetKey(de));
        dictReleaseIterator(di);
        dictRelease(d);
    }
}

/* SRANDMEMBER [<count>] */
void srandmemberCommand(client *c) {
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    // 如果带有 count 参数，那么调用 srandmemberWithCountCommand 来处理
    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
        // 参数错误
    } else if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    // 随机取出单个元素就可以了
    // 取出集合对象
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))
    == NULL || checkType(c,set,OBJ_SET)) return;

    // 随机取出一个元素
    encoding = setTypeRandomElement(set,&ele,&llele);
    // 回复
    if (encoding == OBJ_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulkCBuffer(c,ele,sdslen(ele));
    }
}

/*
 * 计算集合 s1 的基数减去集合 s2 的基数之差
 */
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj**)s1) > setTypeSize(*(robj**)s2)) return 1;
    if (setTypeSize(*(robj**)s1) < setTypeSize(*(robj**)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets.
 *
 * 计算集合 s2 的基数减去集合 s1 的基数之差
 */
int qsortCompareSetsByRevCardinality(const void *s1, const void *s2) {
    robj *o1 = *(robj**)s1, *o2 = *(robj**)s2;
    unsigned long first = o1 ? setTypeSize(o1) : 0;
    unsigned long second = o2 ? setTypeSize(o2) : 0;

    if (first < second) return 1;
    if (first > second) return -1;
    return 0;
}

void sinterGenericCommand(client *c, robj **setkeys,
                          unsigned long setnum, robj *dstkey) {
    // 集合数组
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds elesds;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    for (j = 0; j < setnum; j++) {
        // 取出对象
        // 第一次执行时，取出的是 dest 集合
        // 之后执行时，取出的都是 source 集合
        robj *setobj = dstkey ?
                lookupKeyWrite(c->db,setkeys[j]) :
                lookupKeyRead(c->db,setkeys[j]);
        // 对象不存在，放弃执行，进行清理
        if (!setobj) {
            zfree(sets);
            if (dstkey) {
                if (dbDelete(c->db,dstkey)) {
                    signalModifiedKey(c,c->db,dstkey);
                    notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
                    server.dirty++;
                }
                addReply(c,shared.czero);
            } else {
                addReply(c,shared.emptyset[c->resp]);
            }
            return;
        }

        // 检查对象的类型
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        // 将数组指针指向集合对象
        sets[j] = setobj;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    // 按基数对集合进行排序，这样提升算法的效率
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    // 因为不知道结果集会有多少个元素，所有没有办法直接设置回复的数量
    // 这里使用了一个小技巧，直接使用一个 BUFF 列表，
    // 然后将之后的回复都添加到列表中
     if (!dstkey) {
    replylen = addReplyDeferredLen(c);
} else {
    /* If we have a target key where to store the resulting set
     * create this key with an empty set inside */
    dstset = createIntsetObject();
}

/* Iterate all the elements of the first (smallest) set, and test
 * the element against all the other sets, if at least one set does
 * not include the element it is discarded */
// 遍历基数最小的第一个集合
// 并将它的元素和所有其他集合进行对比
// 如果有至少一个集合不包含这个元素，那么这个元素不属于交集
si = setTypeInitIterator(sets[0]);
while((encoding = setTypeNext(si,&elesds,&intobj)) != -1) {
    // 遍历其他集合，检查元素是否在这些集合中存在
    for (j = 1; j < setnum; j++) {
        // 跳过第一个集合，因为它是结果集的起始值
        if (sets[j] == sets[0]) continue;


        // 元素的编码为 INTSET
        // 在其他集合中查找这个对象是否存在
        if (encoding == OBJ_ENCODING_INTSET) {
            /* intset with intset is simple... and fast */
            if (sets[j]->encoding == OBJ_ENCODING_INTSET &&
            !intsetFind((intset*)sets[j]->ptr,intobj))
            {
                break;
                /* in order to compare an integer with an object we
                 * have to use the generic function, creating an object
                 * for this */
            } else if (sets[j]->encoding == OBJ_ENCODING_HT) {
                elesds = sdsfromlonglong(intobj);
                if (!setTypeIsMember(sets[j],elesds)) {
                    sdsfree(elesds);
                    break;
                }
                sdsfree(elesds);
            }


            // 元素的编码为 字典
            // 在其他集合中查找这个对象是否存在
        } else if (encoding == OBJ_ENCODING_HT) {
            if (!setTypeIsMember(sets[j],elesds)) {
                break;
            }
        }
    }

    /* Only take action when all sets contain the member */
    // 如果所有集合都带有目标元素的话，那么执行以下代码
    if (j == setnum) {
        // SINTER 命令，直接返回结果集元素
        if (!dstkey) {
            if (encoding == OBJ_ENCODING_HT)
                addReplyBulkCBuffer(c,elesds,sdslen(elesds));
            else
                addReplyBulkLongLong(c,intobj);
            cardinality++;
            // SINTERSTORE 命令，将结果添加到结果集中
        } else {
            if (encoding == OBJ_ENCODING_INTSET) {
                elesds = sdsfromlonglong(intobj);
                setTypeAdd(dstset,elesds);
                sdsfree(elesds);
            } else {
                setTypeAdd(dstset,elesds);
            }
        }
    }
}
setTypeReleaseIterator(si);

// SINTERSTORE 命令，将结果集关联到数据库
if (dstkey) {
    /* Store the resulting set into the target, if the intersection
     * is not an empty set. */
    // 如果结果集非空，那么将它关联到数据库中
    if (setTypeSize(dstset) > 0) {
        setKey(c,c->db,dstkey,dstset);
        addReplyLongLong(c,setTypeSize(dstset));
        notifyKeyspaceEvent(NOTIFY_SET,"sinterstore",
                            dstkey,c->db->id);
        server.dirty++;
    } else {
        addReply(c,shared.czero);
        if (dbDelete(c->db,dstkey)) {
            server.dirty++;
            signalModifiedKey(c,c->db,dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
        }
    }
    decrRefCount(dstset);
    // SINTER 命令，回复结果集的基数
} else {
    setDeferredSetLen(c,replylen,cardinality);
}
zfree(sets);
}

void sinterCommand(client *c) {
    sinterGenericCommand(c,c->argv+1,c->argc-1,NULL);
}

void sinterstoreCommand(client *c) {
    sinterGenericCommand(c,c->argv+2,c->argc-2,c->argv[1]);
}



/*
 * 命令的类型
 */
#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op) {
    // 集合数组
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds ele;
    int j, cardinality = 0;
    int diff_algo = 1;

    // 取出所有集合对象，并添加到集合数组中
    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
                lookupKeyWrite(c->db,setkeys[j]) :
                lookupKeyRead(c->db,setkeys[j]);
        // 不存在的集合当作 NULL 来处理
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }


        // 有对象不是集合，停止执行，进行清理
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        // 记录对象
        sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
     *
     * 选择使用那个算法来执行计算
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * 算法 1 的复杂度为 O(N*M) ，其中 N 为第一个集合的基数，
     * 而 M 则为其他集合的数量。
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * 算法 2 的复杂度为 O(N) ，其中 N 为所有集合中的元素数量总数。
     *
     * We compute what is the best bet with the current input here.
     *
     * 程序通过考察输入来决定使用那个算法
     */
    if (op == SET_OP_DIFF && sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        // 遍历所有集合
        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            // 计算 setnum 乘以 sets[0] 的基数之积
            algo_one_work += setTypeSize(sets[0]);
            // 计算所有集合的基数之和
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        // 算法 1 的常数比较低，优先考虑算法 1
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            // 如果使用的是算法 1 ，那么最好对 sets[0] 以外的其他集合进行排序
            // 这样有助于优化算法的性能
            qsort(sets+1,setnum-1,sizeof(robj*),
                  qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key
     *
     * 使用一个临时集合来保存结果集，如果程序执行的是 SUNIONSTORE 命令，
     * 那么这个结果将会成为将来的集合值对象。
     */
    dstset = createIntsetObject();

    // 执行的是并集计算
    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        // 遍历所有集合，将元素添加到结果集里就可以了
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                // setTypeAdd 只在集合不存在时，才会将元素添加到集合，并返回 1
                if (setTypeAdd(dstset,ele)) cardinality++;
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);
        }


        // 执行的是差集计算，并且使用算法 1
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * 差集算法 1 ：
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * 程序遍历 sets[0] 集合中的所有元素，
         * 并将这个元素和其他集合的所有元素进行对比，
         * 只有这个元素不存在于其他所有集合时，
         * 才将这个元素添加到结果集。
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets.
        * 这个算法执行最多 N*M 步， N 是第一个集合的基数，
        * 而 M 是其他集合的数量。
        */
        si = setTypeInitIterator(sets[0]);
        while((ele = setTypeNextObject(si)) != NULL) {
            // 检查元素在其他集合是否存在
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue; /* no key is an empty set. */
                if (sets[j] == sets[0]) break; /* same set! */
                if (setTypeIsMember(sets[j],ele)) break;
            }
            // 只有元素在所有其他集合中都不存在时，才将它添加到结果集中
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);


        // 执行的是差集计算，并且使用算法 2
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * 差集算法 2 ：
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * 将 sets[0] 的所有元素都添加到结果集中，
         * 然后遍历其他所有集合，将相同的元素从结果集中删除。
         *
         * This is O(N) where N is the sum of all the elements in every set.
         *
         * 算法复杂度为 O(N) ，N 为所有集合的基数之和。*/
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

        si = setTypeInitIterator(sets[j]);
        while((ele = setTypeNextObject(si)) != NULL) {
            // sets[0] 时，将所有元素添加到集合
            if (j == 0) {
                if (setTypeAdd(dstset,ele)) cardinality++;
                // 不是 sets[0] 时，将所有集合从结果集中移除
            } else {
                if (setTypeRemove(dstset,ele)) cardinality--;
            }
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);

        /* Exit if result set is empty as any additional removal
         * of elements will have no effect. */
        if (cardinality == 0) break;
    }
}

/* Output the content of the resulting set, if not in STORE mode */
// 执行的是 SDIFF 或者 SUNION
// 打印结果集中的所有元素
if (!dstkey) {
    addReplySetLen(c,cardinality);
    // 遍历并回复结果集中的元素
    si = setTypeInitIterator(dstset);
    while((ele = setTypeNextObject(si)) != NULL) {
        addReplyBulkCBuffer(c,ele,sdslen(ele));
        sdsfree(ele);
    }
    setTypeReleaseIterator(si);
    server.lazyfree_lazy_server_del ? freeObjAsync(NULL, dstset) :
    decrRefCount(dstset);
    // 执行的是 SDIFFSTORE 或者 SUNIONSTORE
} else {
    /* If we have a target key where to store the resulting set
     * create this key with the result set inside */
    // 如果结果集不为空，将它关联到数据库中
    if (setTypeSize(dstset) > 0) {
        setKey(c,c->db,dstkey,dstset);
        // 返回结果集的基数
        addReplyLongLong(c,setTypeSize(dstset));
        notifyKeyspaceEvent(NOTIFY_SET,
                            op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                            dstkey,c->db->id);
        server.dirty++;


        // 结果集为空
    } else {
        addReply(c,shared.czero);
        if (dbDelete(c->db,dstkey)) {
            server.dirty++;
            signalModifiedKey(c,c->db,dstkey);
            notifyKeyspaceEvent(NOTIFY_GENERIC,"del",dstkey,c->db->id);
        }
    }
    decrRefCount(dstset);
}
zfree(sets);
}

void sunionCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_UNION);
}

void sunionstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_UNION);
}

void sdiffCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+1,c->argc-1,NULL,SET_OP_DIFF);
}

void sdiffstoreCommand(client *c) {
    sunionDiffGenericCommand(c,c->argv+2,c->argc-2,c->argv[1],SET_OP_DIFF);
}

void sscanCommand(client *c) {
    robj *set;
    unsigned long cursor;

    if (parseScanCursorOrReply(c,c->argv[2],&cursor) == C_ERR) return;
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyscan)) == NULL ||
    checkType(c,set,OBJ_SET)) return;
    scanGenericCommand(c,set,cursor);
}
