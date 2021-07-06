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
 * ����һ�����Ա���ֵ value �ļ��ϡ�
 *
 * When the object has an integer-encodable value, 
 * an intset will be returned. Otherwise a regular hash table. 
 *
 * �������ֵ���Ա�����Ϊ����ʱ������ intset ��
 * ���򣬷�����ͨ�Ĺ�ϣ��
 */
robj *setTypeCreate(sds value) {
    if (isSdsRepresentableAsLongLong(value,NULL) == C_OK)
        return createIntsetObject();
    return createSetObject();
}

/* Add the specified value into a set.
 * ��̬ add ����
 *
 * ��ӳɹ����� 1 �����Ԫ���Ѿ����ڣ����� 0 ��
 * If the value was already member of the set, nothing is done and 0 is
 * returned, otherwise the new element is added and 1 is returned. */
int setTypeAdd(robj *subject, sds value) {
    long long llval;

    // �ֵ�
    if (subject->encoding == OBJ_ENCODING_HT) {
        // �� value ��Ϊ���� NULL ��Ϊֵ����Ԫ����ӵ��ֵ���
        dict *ht = subject->ptr;
        dictEntry *de = dictAddRaw(ht,value,NULL);
        if (de) {
            dictSetKey(ht,de,sdsdup(value));
            dictSetVal(ht,de,NULL);
            return 1;
        }


    // intset
    } else if (subject->encoding == OBJ_ENCODING_INTSET) {


        // ��������ֵ���Ա���Ϊ�����Ļ�����ô�������ֵ��ӵ� intset ��
        if (isSdsRepresentableAsLongLong(value,&llval) == C_OK) {
            uint8_t success = 0;
            subject->ptr = intsetAdd(subject->ptr,llval,&success);
            if (success) {
                /* Convert to regular set when the intset contains
                 * too many entries. */
                // ��ӳɹ�
                // ��鼯���������Ԫ��֮���Ƿ���Ҫת��Ϊ�ֵ�
                if (intsetLen(subject->ptr) > server.set_max_intset_entries)
                    setTypeConvert(subject,OBJ_ENCODING_HT);
                return 1;
            }
        // ��������ֵ���ܱ���Ϊ��������ô�����ϴ� intset ����ת��Ϊ HT ����
        // Ȼ����ִ����Ӳ���
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
    // ���ʧ�ܣ�Ԫ���Ѿ�����
    return 0;
}


/*
 * ��̬ remove ����
 *
 * ɾ���ɹ����� 1 ����ΪԪ�ز����ڶ�����ɾ��ʧ�ܷ��� 0 ��
 */
int setTypeRemove(robj *setobj, sds value) {
    long long llval;
    if (setobj->encoding == OBJ_ENCODING_HT) {
        // ���ֵ���ɾ����������Ԫ�أ�
        if (dictDelete(setobj->ptr,value) == DICT_OK) {
            // ���Ƿ��б�Ҫ��ɾ��֮����С�ֵ�Ĵ�С
            if (htNeedsResize(setobj->ptr)) dictResize(setobj->ptr);
            return 1;
        }

    // INTSET
    } else if (setobj->encoding == OBJ_ENCODING_INTSET) {

        // ��������ֵ���Ա���Ϊ�����Ļ�����ô���Դ� intset ���Ƴ�Ԫ��
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
 * ��̬ ismember ����
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
 * ����������һ����̬���ϵ�����
 *
 * setTypeIterator ������ redis.h
 */
setTypeIterator *setTypeInitIterator(robj *subject) {
    setTypeIterator *si = zmalloc(sizeof(setTypeIterator));
    // ָ�򱻵����Ķ���
    si->subject = subject;
    // ��¼����ı���
    si->encoding = subject->encoding;
    if (si->encoding == OBJ_ENCODING_HT) {
        // �ֵ������
        si->di = dictGetIterator(subject->ptr);


    // INTSET
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        // ����
        si->ii = 0;
    } else {
        serverPanic("Unknown set encoding");
    }
    return si;
}

/*
 * �ͷŵ�����
 */
void setTypeReleaseIterator(setTypeIterator *si) {
    if (si->encoding == OBJ_ENCODING_HT)
        dictReleaseIterator(si->di);
    zfree(si);
}

/* Move to the next entry in the set. Returns the object at the current
 * position.
 *
 * ȡ����������ָ��ĵ�ǰ����Ԫ�ء�
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
 * ��Ϊ���ϼ����Ա���Ϊ intset ��Ҳ���Ա���Ϊ��ϣ��
 * ���Գ������ݼ��ϵı��룬ѡ��ֵ���浽�Ǹ������
 *
 *  - ������Ϊ intset ʱ��Ԫ�ر�ָ�� llobj ����
 *
 *  - ������Ϊ��ϣ��ʱ��Ԫ�ر�ָ�� eobj ����
 *
 * ���Һ����᷵�ر��������ϵı��룬����ʶ��
 *
 * �������е�Ԫ��ȫ�����������ʱ���������� -1 ��
 *
 * ��Ϊ�����صĶ�����û�б��������ü����ģ�
 * ������������Ƕ� copy-on-write �Ѻõġ�
 * When there are no longer elements -1 is returned. */
int setTypeNext(setTypeIterator *si, sds *sdsele, int64_t *llele) {

    // ���ֵ���ȡ������
    if (si->encoding == OBJ_ENCODING_HT) {
    	// ���µ�����
        dictEntry *de = dictNext(si->di);
        // �ֵ��ѵ�����
        if (de == NULL) return -1;

        // ���ؽڵ�ļ������ϵ�Ԫ�أ�
        *sdsele = dictGetKey(de);
        *llele = -123456789; /* Not needed. Defensive. */

    // �� intset ��ȡ��Ԫ��
    } else if (si->encoding == OBJ_ENCODING_INTSET) {
        if (!intsetGet(si->subject->ptr,si->ii++,llele))
            return -1;
        *sdsele = NULL; /* Not needed. Defensive. */
    } else {
        serverPanic("Wrong set encoding in setTypeNext");
    }
    // ���ر���
    return si->encoding;
}

/* The not copy on write friendly version but easy to use version
 * of setTypeNext() is setTypeNextObject(), returning new SDS
 * strings. So if you don't retain a pointer to this object you should call
 * sdsfree() against it.
 *
 * setTypeNext �ķ� copy-on-write �Ѻð汾��
 * ���Ƿ���һ���µġ������Ѿ����ӹ����ü����Ķ���
 *
 * ��������ʹ�������֮��Ӧ�öԶ������ decrRefCount() ��
 *
 * This function is the way to go for write operations where COW is not
 * an issue. 
 *
 * �������Ӧ���ڷ� copy-on-write ʱ����
*/
sds setTypeNextObject(setTypeIterator *si) {
    int64_t intele;
    sds sdsele;
    int encoding;

    // ȡ��Ԫ��
    encoding = setTypeNext(si,&sdsele,&intele);
    // ����ΪԪ�ش�������
    switch(encoding) {
        // ��Ϊ��
        case -1:    return NULL;
        // INTSET ����һ������ֵ����ҪΪ���ֵ��������
        case OBJ_ENCODING_INTSET:
            return sdsfromlonglong(intele);
        // HT �����Ѿ����ض����ˣ�ֻ��ִ�� incrRefCount()
        case OBJ_ENCODING_HT:
            return sdsdup(sdsele);
        default:
            serverPanic("Unsupported encoding");
    }
    return NULL; /* just to suppress warnings */
}

/* Return random element from a non empty set.
 *
 * �ӷǿռ��������ȡ��һ��Ԫ�ء�
 *
 * The returned element can be an int64_t value if the set is encoded
 * as an "intset" blob of integers, or an SDS string if the set
 * is a regular set.
 *
 * ������ϵı���Ϊ intset ����ô��Ԫ��ָ�� int64_t ָ�� llele ��
 * ������ϵı���Ϊ HT ����ô��Ԫ�ض���ָ�����ָ�� objele ��
 *
 * The caller provides both pointers to be populated with the right
 * object. The return value of the function is the object->encoding
 * field of the object and is used by the caller to check if the
 * int64_t pointer or the redis object pointer was populated.
 * �����ķ���ֵΪ���ϵı��뷽ʽ��ͨ���������ֵ����֪���Ǹ�ָ�뱣����Ԫ�ص�ֵ��

 * Note that both the sdsele and llele pointers should be passed and cannot
 * be NULL since the function will try to defensively populate the non
 * used field with values which are easy to trap if misused. 
 * ��Ϊ�����صĶ�����û�б��������ü����ģ�
 * ������������Ƕ� copy-on-write �Ѻõġ�
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
 * ���϶�̬ size ����
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
 * �����϶��� setobj �ı���ת��Ϊ REDIS_ENCODING_HT ��
 *
 * The resulting dict (when converting to a hash table)
 * is presized to hold the number of elements in the original set.
 *
 * �´����Ľ���ֵ�ᱻԤ�ȷ���Ϊ��ԭ���ļ���һ����
 */
void setTypeConvert(robj *setobj, int enc) {
    setTypeIterator *si;

    // ȷ�����ͺͱ�����ȷ
    serverAssertWithInfo(NULL,setobj,setobj->type == OBJ_SET &&
                             setobj->encoding == OBJ_ENCODING_INTSET);

    if (enc == OBJ_ENCODING_HT) {
        int64_t intele;
        // �������ֵ�
        dict *d = dictCreate(&setDictType,NULL);
        sds element;

        /* Presize the dict to avoid rehashing */
        // Ԥ����չ�ռ�
        dictExpand(d,intsetLen(setobj->ptr));

        /* To add the elements we extract integers and create redis objects */
        // �������ϣ�����Ԫ����ӵ��ֵ���
        si = setTypeInitIterator(setobj);
        while (setTypeNext(si,&element,&intele) != -1) {
            element = sdsfromlonglong(intele);
            serverAssert(dictAdd(d,element,NULL) == DICT_OK);
        }
        setTypeReleaseIterator(si);

        // ���¼��ϵı���
        setobj->encoding = OBJ_ENCODING_HT;
        zfree(setobj->ptr);
        // ���¼��ϵ�ֵ����
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

    // ȡ�����϶���
    set = lookupKeyWrite(c->db,c->argv[1]);
    if (checkType(c,set,OBJ_SET)) return;
    // ���󲻴��ڣ�����һ���µģ����������������ݿ�
    if (set == NULL) {
        set = setTypeCreate(c->argv[2]->ptr);
        dbAdd(c->db,c->argv[1],set);
    // ������ڣ��������
    }

    // ����������Ԫ����ӵ�������
    for (j = 2; j < c->argc; j++) {
        // ֻ��Ԫ��δ�����ڼ���ʱ������һ�γɹ����
        if (setTypeAdd(set,c->argv[j]->ptr)) added++;
    }
    // ���������һ��Ԫ�ر��ɹ���ӣ���ôִ�����³���
    if (added) {
        // ���ͼ��޸��ź�
        signalModifiedKey(c,c->db,c->argv[1]);
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[1],c->db->id);
    }
    // �����ݿ���Ϊ��
    server.dirty += added;
    // �������Ԫ�ص�����
    addReplyLongLong(c,added);
}

void sremCommand(client *c) {
    robj *set;
    int j, deleted = 0, keyremoved = 0;

    // ȡ�����϶���
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,set,OBJ_SET)) return;

    // ɾ�����������Ԫ��
    for (j = 2; j < c->argc; j++) {

        // ֻ��Ԫ���ڼ�����ʱ������һ�γɹ�ɾ��
        if (setTypeRemove(set,c->argv[j]->ptr)) {
            deleted++;
            // ��������Ѿ�Ϊ�գ���ôɾ�����϶���
            if (setTypeSize(set) == 0) {
                dbDelete(c->db,c->argv[1]);
                keyremoved = 1;
                break;
            }
        }
    }
    // ���������һ��Ԫ�ر��ɹ�ɾ������ôִ�����³���
    if (deleted) {

        // ���ͼ��޸��ź�
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
    // ȡ��Դ����
    srcset = lookupKeyWrite(c->db,c->argv[1]);
    // ȡ��Ŀ�꼯��
    dstset = lookupKeyWrite(c->db,c->argv[2]);
    // ����Ԫ��
    ele = c->argv[3];

    /* If the source key does not exist return 0 */
    // Դ���ϲ����ڣ�ֱ�ӷ���
    if (srcset == NULL) {
        addReply(c,shared.czero);
        return;
    }

    /* If the source key has the wrong type, or the destination key
     * is set and has the wrong type, return with an error. */
    // ���Դ���ϵ����ʹ���
    // ����Ŀ�꼯�ϴ��ڡ��������ʹ���
    // ��ôֱ�ӷ���
    if (checkType(c,srcset,OBJ_SET) ||
        checkType(c,dstset,OBJ_SET)) return;

    /* If srcset and dstset are equal, SMOVE is a no-op */
    // ���Դ���Ϻ�Ŀ�꼯����ȣ���ôֱ�ӷ���
    if (srcset == dstset) {
        addReply(c,setTypeIsMember(srcset,ele->ptr) ?
            shared.cone : shared.czero);
        return;
    }

    /* If the element cannot be removed from the src set, return 0. */
    // ��Դ�������Ƴ�Ŀ��Ԫ��
    // ���Ŀ��Ԫ�ز�������Դ�����У���ôֱ�ӷ���
    if (!setTypeRemove(srcset,ele->ptr)) {
        addReply(c,shared.czero);
        return;
    }
    // �����¼�֪ͨ
    notifyKeyspaceEvent(NOTIFY_SET,"srem",c->argv[1],c->db->id);

    /* Remove the src set from the database when empty */
    // ���Դ�����Ѿ�Ϊ�գ���ô���������ݿ���ɾ��
    if (setTypeSize(srcset) == 0) {
        // ɾ�����϶���
        dbDelete(c->db,c->argv[1]);

        // �����¼�֪ͨ
        notifyKeyspaceEvent(NOTIFY_GENERIC,"del",c->argv[1],c->db->id);
    }

    /* Create the destination set when it doesn't exist */
    if (!dstset) {
        dstset = setTypeCreate(ele->ptr);
        dbAdd(c->db,c->argv[2],dstset);
    }

    // ���ͼ��޸��ź�
    signalModifiedKey(c,c->db,c->argv[1]);
    signalModifiedKey(c,c->db,c->argv[2]);
    server.dirty++;

    // ��Ԫ����ӵ�Ŀ�꼯��
    /* An extra key has changed when ele was successfully added to dstset */
    if (setTypeAdd(dstset,ele->ptr)) {
        // �����ݿ���Ϊ��
        server.dirty++;
        notifyKeyspaceEvent(NOTIFY_SET,"sadd",c->argv[2],c->db->id);
    }
    // �ظ� 1 
    addReply(c,shared.cone);
}

void sismemberCommand(client *c) {
    robj *set;

    // ȡ�����϶���
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

    // ȡ������
    if ((o = lookupKeyReadOrReply(c,c->argv[1],shared.czero)) == NULL ||
        checkType(c,o,OBJ_SET)) return;

    // ���ؼ��ϵĻ���
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

    // ȡ������
    if ((set = lookupKeyWriteOrReply(c,c->argv[1],shared.null[c->resp]))
         == NULL || checkType(c,set,OBJ_SET)) return;

    /* Get a random element from the set */
    // �Ӽ��������ȡ��һ��Ԫ��
    encoding = setTypeRandomElement(set,&sdsele,&llele);

    /* Remove the element from the set */
    // ����ȡ��Ԫ�شӼ�����ɾ��
    if (encoding == OBJ_ENCODING_INTSET) {
        ele = createStringObjectFromLongLong(llele);
        set->ptr = intsetRemove(set->ptr,llele,NULL);
    } else {
        ele = createStringObject(sdsele,sdslen(sdsele));
        setTypeRemove(set,ele->ptr);
    }
    // �����¼�֪ͨ
    notifyKeyspaceEvent(NOTIFY_SET,"spop",c->argv[1],c->db->id);

    /* Replicate/AOF this command as an SREM operation */

    // ���������� SREM ����������ֹ�����к�������ԣ��������ݲ�һ��
    // ����ͬ�ķ��������ɾ����ͬ��Ԫ�أ�
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
 * ʵ�� SRANDMEMBER key <count> ���֣�
 * ԭ���� SRANDMEMBER key �� srandmemberCommand() ʵ��
 */

/* How many times bigger should be the set compared to the requested size
 * for us to don't use the "remove elements" strategy? Read later in the
 * implementation for more info. 
 *
 * ��� count ������������������õĻ����ȼ��ϵĻ���Ҫ��
 * ��ô����Ͳ�ʹ�á�ɾ��Ԫ�ء��Ĳ��ԡ�
 *
 * ������Ϣ��ο�����ĺ������塣
 */
#define SRANDMEMBER_SUB_STRATEGY_MUL 3

void srandmemberWithCountCommand(client *c) {
    long l;
    unsigned long count, size;
    // Ĭ���ڼ����в������ظ�Ԫ��
    int uniq = 1;
    robj *set;
    sds ele;
    int64_t llele;
    int encoding;

    dict *d;

    // ȡ�� l ����
    if (getLongFromObjectOrReply(c,c->argv[2],&l,NULL) != C_OK) return;
    if (l >= 0) {
        // l Ϊ��������ʾ����Ԫ�ظ�����ͬ
        count = (unsigned long) l;
    } else {
        /* A negative count means: return the same elements multiple times
         * (i.e. don't remove the extracted element after every extraction). */
      // ��� l Ϊ��������ô��ʾ���صĽ���п������ظ�Ԫ��
        count = -l;
        uniq = 0;
    }

    // ȡ�����϶���
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.emptyarray))
        == NULL || checkType(c,set,OBJ_SET)) return;
    // ȡ�����ϻ���
    size = setTypeSize(set);

    /* If count is zero, serve it ASAP to avoid special cases later. */
    // count Ϊ 0 ��ֱ�ӷ���
    if (count == 0) {
        addReply(c,shared.emptyarray);
        return;
    }

    /* CASE 1: The count was negative, so the extraction method is just:
     * "return N random elements" sampling the whole set every time.
     * This case is trivial and can be served without auxiliary data
     * structures. 
     *
     * ���� 1��count Ϊ��������������Դ����ظ�Ԫ��
     * ֱ�ӴӼ�����ȡ�������� N �����Ԫ�ؾͿ�����
     *
     * �������β���Ҫ����Ľṹ����������
     * This case is the only one that also needs to return the
     * elements in random order. */
    if (!uniq || count == 1) {
        addReplyArrayLen(c,count);
        while(count--) {
            // ȡ�����Ԫ��
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
     * ��� count �ȼ��ϵĻ���Ҫ����ôֱ�ӷ�����������
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
    // �������� 3 ������ 4 ����Ҫһ��������ֵ�
    d = dictCreate(&sdsReplyDictType,NULL);

    /* CASE 3:
     * 
     * ���� 3��
     *
     * The number of elements inside the set is not greater than
     * SRANDMEMBER_SUB_STRATEGY_MUL times the number of requested elements.
     *
     * count �������� SRANDMEMBER_SUB_STRATEGY_MUL �Ļ��ȼ��ϵĻ���Ҫ��
     *
     * In this case we create a set from scratch with all the elements, and
     * subtract random elements to reach the requested number of elements.
     *
     * ����������£����򴴽�һ�����ϵĸ�����
     * ���Ӽ�����ɾ��Ԫ�أ�ֱ�����ϵĻ������� count ����ָ��������Ϊֹ��
     *
     * This is done because if the number of requsted elements is just
     * a bit less than the number of elements in the set, the natural approach
     * used into CASE 3 is highly inefficient. 
     *
     * ʹ������������ԭ���ǣ��� count �������ӽ��ڼ��ϵĻ���ʱ��
     * �Ӽ��������ȡ�� count �������ķ����Ƿǳ���Ч�ġ�
     */
    if (count*SRANDMEMBER_SUB_STRATEGY_MUL > size) {
        setTypeIterator *si;

        /* Add all the elements into the temporary dictionary. */
        // �������ϣ�������Ԫ����ӵ���ʱ�ֵ���
        si = setTypeInitIterator(set);
        dictExpand(d, size);
        while ((encoding = setTypeNext(si,&ele,&llele)) != -1) {
            int retval = DICT_ERR;

            // ΪԪ�ش������󣬲���ӵ��ֵ���
            if (encoding == OBJ_ENCODING_INTSET) {
                retval = dictAdd(d,sdsfromlonglong(llele),NULL);
            } else {
                retval = dictAdd(d,sdsdup(ele),NULL);
            }
            serverAssert(retval == DICT_OK);
            /* ����Ĵ�������ع�Ϊ
            
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
        // ���ɾ��Ԫ�أ�ֱ�����ϻ������� count ������ֵ
        while (size > count) {
            dictEntry *de;
            // ȡ����ɾ�����Ԫ��
            de = dictGetRandomKey(d);
            dictUnlink(d,dictGetKey(de));
            sdsfree(dictGetKey(de));
            dictFreeUnlinkedEntry(d,de);
            size--;
        }
    }

    /* CASE 4: We have a big set compared to the requested number of elements.
     *
     * ���� 4 �� count ����Ҫ�ȼ��ϻ���С�ܶࡣ
     *
     * In this case we can simply get random elements from the set and add
     * to the temporary set, trying to eventually get enough unique elements
     * to reach the specified count. 
     *
     * ����������£����ǿ���ֱ�ӴӼ����������ȡ��Ԫ�أ�
     * ��������ӵ���������У�ֱ��������Ļ������� count Ϊֹ��
     */
    else {
        unsigned long added = 0;
        sds sdsele;

        dictExpand(d, count);
        while (added < count) {
            // ����ش�Ŀ�꼯����ȡ��Ԫ��
            encoding = setTypeRandomElement(set,&ele,&llele);

            // ��Ԫ��ת��Ϊ����
            if (encoding == OBJ_ENCODING_INTSET) {
                sdsele = sdsfromlonglong(llele);
            } else {
                sdsele = sdsdup(ele);
            }
            /* Try to add the object to the dictionary. If it already exists
             * free it, otherwise increment the number of objects we have
             * in the result dictionary. */
            // ���Խ�Ԫ����ӵ��ֵ���
            // dictAdd ֻ����Ԫ�ز��������ֵ�ʱ���Ż᷵�� 1
            // ������������Ѿ���ͬ����Ԫ�أ���ô�����ִ�� else ����
            // ֻ��Ԫ�ز������ڽ����ʱ����ӲŻ�ɹ�
            if (dictAdd(d,sdsele,NULL) == DICT_OK)
                added++;
            else
                sdsfree(sdsele);
        }
    }

    /* CASE 3 & 4: send the result to the user. */
    {
        // ���� 3 �� 4 ����������ظ����ͻ���
        dictIterator *di;
        dictEntry *de;

        addReplyArrayLen(c,count);
        // ���������Ԫ��,
        di = dictGetIterator(d);
           // �ظ�
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

    // ������� count ��������ô���� srandmemberWithCountCommand ������
    if (c->argc == 3) {
        srandmemberWithCountCommand(c);
        return;
    // ��������
    } else if (c->argc > 3) {
        addReplyErrorObject(c,shared.syntaxerr);
        return;
    }

    /* Handle variant without <count> argument. Reply with simple bulk string */
    // ���ȡ������Ԫ�ؾͿ�����
    // ȡ�����϶���
    if ((set = lookupKeyReadOrReply(c,c->argv[1],shared.null[c->resp]))
        == NULL || checkType(c,set,OBJ_SET)) return;

    // ���ȡ��һ��Ԫ��
    encoding = setTypeRandomElement(set,&ele,&llele);
    // �ظ�
    if (encoding == OBJ_ENCODING_INTSET) {
        addReplyBulkLongLong(c,llele);
    } else {
        addReplyBulkCBuffer(c,ele,sdslen(ele));
    }
}

/*
 * ���㼯�� s1 �Ļ�����ȥ���� s2 �Ļ���֮��
 */
int qsortCompareSetsByCardinality(const void *s1, const void *s2) {
    if (setTypeSize(*(robj**)s1) > setTypeSize(*(robj**)s2)) return 1;
    if (setTypeSize(*(robj**)s1) < setTypeSize(*(robj**)s2)) return -1;
    return 0;
}

/* This is used by SDIFF and in this case we can receive NULL that should
 * be handled as empty sets. 
 *
 * ���㼯�� s2 �Ļ�����ȥ���� s1 �Ļ���֮��
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
    // ��������
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds elesds;
    int64_t intobj;
    void *replylen = NULL;
    unsigned long j, cardinality = 0;
    int encoding;

    for (j = 0; j < setnum; j++) {
        // ȡ������
        // ��һ��ִ��ʱ��ȡ������ dest ����
        // ֮��ִ��ʱ��ȡ���Ķ��� source ����
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        // ���󲻴��ڣ�����ִ�У���������
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

     // �����������
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        // ������ָ��ָ�򼯺϶���
        sets[j] = setobj;
    }
    /* Sort sets from the smallest to largest, this will improve our
     * algorithm's performance */
    // �������Լ��Ͻ����������������㷨��Ч��
    qsort(sets,setnum,sizeof(robj*),qsortCompareSetsByCardinality);

    /* The first thing we should output is the total number of elements...
     * since this is a multi-bulk write, but at this stage we don't know
     * the intersection set size, so we use a trick, append an empty object
     * to the output list and save the pointer to later modify it with the
     * right length */
    // ��Ϊ��֪����������ж��ٸ�Ԫ�أ�����û�а취ֱ�����ûظ�������
    // ����ʹ����һ��С���ɣ�ֱ��ʹ��һ�� BUFF �б�
    // Ȼ��֮��Ļظ�����ӵ��б���    if (!dstkey) {
        replylen = addReplyDeferredLen(c);
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with an empty set inside */
        dstset = createIntsetObject();
    }

    /* Iterate all the elements of the first (smallest) set, and test
     * the element against all the other sets, if at least one set does
     * not include the element it is discarded */
    // ����������С�ĵ�һ������
    // ��������Ԫ�غ������������Ͻ��жԱ�
    // ���������һ�����ϲ��������Ԫ�أ���ô���Ԫ�ز����ڽ���
    si = setTypeInitIterator(sets[0]);
    while((encoding = setTypeNext(si,&elesds,&intobj)) != -1) {
        // �����������ϣ����Ԫ���Ƿ�����Щ�����д���
        for (j = 1; j < setnum; j++) {
            // ������һ�����ϣ���Ϊ���ǽ��������ʼֵ
            if (sets[j] == sets[0]) continue;


            // Ԫ�صı���Ϊ INTSET 
            // �����������в�����������Ƿ����
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


            // Ԫ�صı���Ϊ �ֵ�
            // �����������в�����������Ƿ����
            } else if (encoding == OBJ_ENCODING_HT) {
                if (!setTypeIsMember(sets[j],elesds)) {
                    break;
                }
            }
        }

        /* Only take action when all sets contain the member */
        // ������м��϶�����Ŀ��Ԫ�صĻ�����ôִ�����´���
        if (j == setnum) {
           // SINTER ���ֱ�ӷ��ؽ����Ԫ��
            if (!dstkey) {
                if (encoding == OBJ_ENCODING_HT)
                    addReplyBulkCBuffer(c,elesds,sdslen(elesds));
                else
                    addReplyBulkLongLong(c,intobj);
                cardinality++;
            // SINTERSTORE ����������ӵ��������
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

    // SINTERSTORE �������������������ݿ�
    if (dstkey) {
        /* Store the resulting set into the target, if the intersection
         * is not an empty set. */
        // ���������ǿգ���ô�������������ݿ���
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
    // SINTER ����ظ�������Ļ���
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
 * ���������
 */
#define SET_OP_UNION 0
#define SET_OP_DIFF 1
#define SET_OP_INTER 2

void sunionDiffGenericCommand(client *c, robj **setkeys, int setnum,
                              robj *dstkey, int op) {
    // ��������
    robj **sets = zmalloc(sizeof(robj*)*setnum);
    setTypeIterator *si;
    robj *dstset = NULL;
    sds ele;
    int j, cardinality = 0;
    int diff_algo = 1;

    // ȡ�����м��϶��󣬲���ӵ�����������
    for (j = 0; j < setnum; j++) {
        robj *setobj = dstkey ?
            lookupKeyWrite(c->db,setkeys[j]) :
            lookupKeyRead(c->db,setkeys[j]);
        // �����ڵļ��ϵ��� NULL ������
        if (!setobj) {
            sets[j] = NULL;
            continue;
        }


        // �ж����Ǽ��ϣ�ִֹͣ�У���������
        if (checkType(c,setobj,OBJ_SET)) {
            zfree(sets);
            return;
        }
        // ��¼����
        sets[j] = setobj;
    }

    /* Select what DIFF algorithm to use.
     *
     * ѡ��ʹ���Ǹ��㷨��ִ�м���
     *
     * Algorithm 1 is O(N*M) where N is the size of the element first set
     * and M the total number of sets.
     *
     * �㷨 1 �ĸ��Ӷ�Ϊ O(N*M) ������ N Ϊ��һ�����ϵĻ�����
     * �� M ��Ϊ�������ϵ�������
     *
     * Algorithm 2 is O(N) where N is the total number of elements in all
     * the sets.
     *
     * �㷨 2 �ĸ��Ӷ�Ϊ O(N) ������ N Ϊ���м����е�Ԫ������������
     *
     * We compute what is the best bet with the current input here. 
     *
     * ����ͨ����������������ʹ���Ǹ��㷨
     */
    if (op == SET_OP_DIFF && sets[0]) {
        long long algo_one_work = 0, algo_two_work = 0;

        // �������м���
        for (j = 0; j < setnum; j++) {
            if (sets[j] == NULL) continue;

            // ���� setnum ���� sets[0] �Ļ���֮��
            algo_one_work += setTypeSize(sets[0]);
            // �������м��ϵĻ���֮��
            algo_two_work += setTypeSize(sets[j]);
        }

        /* Algorithm 1 has better constant times and performs less operations
         * if there are elements in common. Give it some advantage. */
        // �㷨 1 �ĳ����Ƚϵͣ����ȿ����㷨 1
        algo_one_work /= 2;
        diff_algo = (algo_one_work <= algo_two_work) ? 1 : 2;

        if (diff_algo == 1 && setnum > 1) {
            /* With algorithm 1 it is better to order the sets to subtract
             * by decreasing size, so that we are more likely to find
             * duplicated elements ASAP. */
            // ���ʹ�õ����㷨 1 ����ô��ö� sets[0] ������������Ͻ�������
            // �����������Ż��㷨������
            qsort(sets+1,setnum-1,sizeof(robj*),
                qsortCompareSetsByRevCardinality);
        }
    }

    /* We need a temp set object to store our union. If the dstkey
     * is not NULL (that is, we are inside an SUNIONSTORE operation) then
     * this set object will be the resulting object to set into the target key
     *
     * ʹ��һ����ʱ�����������������������ִ�е��� SUNIONSTORE ���
     * ��ô�����������Ϊ�����ļ���ֵ����
     */
    dstset = createIntsetObject();

    // ִ�е��ǲ�������
    if (op == SET_OP_UNION) {
        /* Union is trivial, just add every element of every set to the
         * temporary set. */
        // �������м��ϣ���Ԫ����ӵ��������Ϳ�����
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                // setTypeAdd ֻ�ڼ��ϲ�����ʱ���ŻὫԪ����ӵ����ϣ������� 1 
                if (setTypeAdd(dstset,ele)) cardinality++;
                sdsfree(ele);
            }
            setTypeReleaseIterator(si);
        }


    // ִ�е��ǲ���㣬����ʹ���㷨 1
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 1) {
        /* DIFF Algorithm 1:
         *
         * ��㷨 1 ��
         *
         * We perform the diff by iterating all the elements of the first set,
         * and only adding it to the target set if the element does not exist
         * into all the other sets.
         *
         * ������� sets[0] �����е�����Ԫ�أ�
         * �������Ԫ�غ��������ϵ�����Ԫ�ؽ��жԱȣ�
         * ֻ�����Ԫ�ز��������������м���ʱ��
         * �Ž����Ԫ����ӵ��������
         *
         * This way we perform at max N*M operations, where N is the size of
         * the first set, and M the number of sets. */
         * ����㷨ִ����� N*M ���� N �ǵ�һ�����ϵĻ�����
         * �� M ���������ϵ�������
         */
        si = setTypeInitIterator(sets[0]);
        while((ele = setTypeNextObject(si)) != NULL) {
            // ���Ԫ�������������Ƿ����
            for (j = 1; j < setnum; j++) {
                if (!sets[j]) continue; /* no key is an empty set. */
                if (sets[j] == sets[0]) break; /* same set! */
                if (setTypeIsMember(sets[j],ele)) break;
            }
            // ֻ��Ԫ�����������������ж�������ʱ���Ž�����ӵ��������
            if (j == setnum) {
                /* There is no other set with this element. Add it. */
                setTypeAdd(dstset,ele);
                cardinality++;
            }
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);


    // ִ�е��ǲ���㣬����ʹ���㷨 2
    } else if (op == SET_OP_DIFF && sets[0] && diff_algo == 2) {
        /* DIFF Algorithm 2:
         *
         * ��㷨 2 ��
         * Add all the elements of the first set to the auxiliary set.
         * Then remove all the elements of all the next sets from it.
         *
         * �� sets[0] ������Ԫ�ض���ӵ�������У�
         * Ȼ������������м��ϣ�����ͬ��Ԫ�شӽ������ɾ����
         *
         * This is O(N) where N is the sum of all the elements in every set. 
         *
         * �㷨���Ӷ�Ϊ O(N) ��N Ϊ���м��ϵĻ���֮�͡�
        for (j = 0; j < setnum; j++) {
            if (!sets[j]) continue; /* non existing keys are like empty sets */

            si = setTypeInitIterator(sets[j]);
            while((ele = setTypeNextObject(si)) != NULL) {
                // sets[0] ʱ��������Ԫ����ӵ�����
                if (j == 0) {
                    if (setTypeAdd(dstset,ele)) cardinality++;
                // ���� sets[0] ʱ�������м��ϴӽ�������Ƴ�
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
    // ִ�е��� SDIFF ���� SUNION
    // ��ӡ������е�����Ԫ��
    if (!dstkey) {
        addReplySetLen(c,cardinality);
        // �������ظ�������е�Ԫ��
        si = setTypeInitIterator(dstset);
        while((ele = setTypeNextObject(si)) != NULL) {
            addReplyBulkCBuffer(c,ele,sdslen(ele));
            sdsfree(ele);
        }
        setTypeReleaseIterator(si);
        server.lazyfree_lazy_server_del ? freeObjAsync(NULL, dstset) :
                                          decrRefCount(dstset);
    // ִ�е��� SDIFFSTORE ���� SUNIONSTORE
    } else {
        /* If we have a target key where to store the resulting set
         * create this key with the result set inside */
        // ����������Ϊ�գ��������������ݿ���
        if (setTypeSize(dstset) > 0) {
            setKey(c,c->db,dstkey,dstset);
            // ���ؽ�����Ļ���
            addReplyLongLong(c,setTypeSize(dstset));
            notifyKeyspaceEvent(NOTIFY_SET,
                op == SET_OP_UNION ? "sunionstore" : "sdiffstore",
                dstkey,c->db->id);
            server.dirty++;


        // �����Ϊ��
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
