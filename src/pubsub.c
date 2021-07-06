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

int clientSubscriptionsCount(client *c);

/*-----------------------------------------------------------------------------
 * Pubsub client replies API
 *----------------------------------------------------------------------------*/

/* Send a pubsub message of type "message" to the client.
 * Normally 'msg' is a Redis object containing the string to send as
 * message. However if the caller sets 'msg' as NULL, it will be able
 * to send a special message (for instance an Array type) by using the
 * addReply*() API family. */
void addReplyPubsubMessage(client *c, robj *channel, robj *msg) {
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.messagebulk);
    addReplyBulk(c,channel);
    if (msg) addReplyBulk(c,msg);
}

/* Send a pubsub message of type "pmessage" to the client. The difference
 * with the "message" type delivered by addReplyPubsubMessage() is that
 * this message format also includes the pattern that matched the message. */
void addReplyPubsubPatMessage(client *c, robj *pat, robj *channel, robj *msg) {
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[4]);
    else
        addReplyPushLen(c,4);
    addReply(c,shared.pmessagebulk);
    addReplyBulk(c,pat);
    addReplyBulk(c,channel);
    addReplyBulk(c,msg);
}

/* Send the pubsub subscription notification to the client. */
void addReplyPubsubSubscribed(client *c, robj *channel) {
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.subscribebulk);
    addReplyBulk(c,channel);
    addReplyLongLong(c,clientSubscriptionsCount(c));
}

/* Send the pubsub unsubscription notification to the client.
 * Channel can be NULL: this is useful when the client sends a mass
 * unsubscribe command but there are no channels to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubUnsubscribed(client *c, robj *channel) {
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.unsubscribebulk);
    if (channel)
        addReplyBulk(c,channel);
    else
        addReplyNull(c);
    addReplyLongLong(c,clientSubscriptionsCount(c));
}

/* Send the pubsub pattern subscription notification to the client. */
void addReplyPubsubPatSubscribed(client *c, robj *pattern) {
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.psubscribebulk);
    addReplyBulk(c,pattern);
    addReplyLongLong(c,clientSubscriptionsCount(c));
}

/* Send the pubsub pattern unsubscription notification to the client.
 * Pattern can be NULL: this is useful when the client sends a mass
 * punsubscribe command but there are no pattern to unsubscribe from: we
 * still send a notification. */
void addReplyPubsubPatUnsubscribed(client *c, robj *pattern) {
    if (c->resp == 2)
        addReply(c,shared.mbulkhdr[3]);
    else
        addReplyPushLen(c,3);
    addReply(c,shared.punsubscribebulk);
    if (pattern)
        addReplyBulk(c,pattern);
    else
        addReplyNull(c);
    addReplyLongLong(c,clientSubscriptionsCount(c));
}

/*-----------------------------------------------------------------------------
 * Pubsub low level API
 *----------------------------------------------------------------------------*/

/* Return the number of channels + patterns a client is subscribed to. */
int clientSubscriptionsCount(client *c) {
    return dictSize(c->pubsub_channels)+
           listLength(c->pubsub_patterns);
}

/* Subscribe a client to a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was already subscribed to that channel. 
 *
 * 设置客户端 c 订阅频道 channel 。
 *
 * 订阅成功返回 1 ，如果客户端已经订阅了该频道，那么返回 0 。
 */
int pubsubSubscribeChannel(client *c, robj *channel) {
    dictEntry *de;
    list *clients = NULL;
    int retval = 0;

    /* Add the channel to the client -> channels hash table */
    // 将 channels 填接到 c->pubsub_channels 的集合中（值为 NULL 的字典视为集合）
    if (dictAdd(c->pubsub_channels,channel,NULL) == DICT_OK) {
        retval = 1;
        incrRefCount(channel);
        // 关联示意图
        // {
        //  频道名        订阅频道的客户端
        //  'channel-a' : [c1, c2, c3],
        //  'channel-b' : [c5, c2, c1],
        //  'channel-c' : [c10, c2, c1]
        // }
        /* Add the client to the channel -> list of clients hash table */
        // 从 pubsub_channels 字典中取出保存着所有订阅了 channel 的客户端的链表
        // 如果 channel 不存在于字典，那么添加进去
        de = dictFind(server.pubsub_channels,channel);
        if (de == NULL) {
            clients = listCreate();
            dictAdd(server.pubsub_channels,channel,clients);
            incrRefCount(channel);
        } else {
            clients = dictGetVal(de);
        }
        // before:
        // 'channel' : [c1, c2]
        // after:
        // 'channel' : [c1, c2, c3]
        // 将客户端添加到链表的末尾
        listAddNodeTail(clients,c);
    }
    /* Notify the client */
  // 回复客户端。
    // 示例：
    // redis 127.0.0.1:6379> SUBSCRIBE xxx
    // Reading messages... (press Ctrl-C to quit)
    // 1) "subscribe"
    // 2) "xxx"
    // 3) (integer) 1
 	// "subscribe\n" 字符串
    // 被订阅的客户端
    // 客户端订阅的频道和模式总数
    addReplyPubsubSubscribed(c,channel);
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. 
 *
 * 客户端 c 退订频道 channel 。
 *
 * 如果取消成功返回 1 ，如果因为客户端未订阅频道，而造成取消失败，返回 0 。
 */
int pubsubUnsubscribeChannel(client *c, robj *channel, int notify) {
    dictEntry *de;
    list *clients;
    listNode *ln;
    int retval = 0;

    /* Remove the channel from the client -> channels hash table */
    // 将频道 channel 从 client->channels 字典中移除
    incrRefCount(channel); /* channel may be just a pointer to the same object
                            we have in the hash tables. Protect it... */
    // 示意图：
    // before:
    // {
    //  'channel-x': NULL,
    //  'channel-y': NULL,
    //  'channel-z': NULL,
    // }
    // after unsubscribe channel-y ：
    // {
    //  'channel-x': NULL,
    //  'channel-z': NULL,
    // }
    if (dictDelete(c->pubsub_channels,channel) == DICT_OK) {
        // channel 移除成功，表示客户端订阅了这个频道，执行以下代码
        retval = 1;
        /* Remove the client from the channel -> clients list hash table */
        // 从 channel->clients 的 clients 链表中，移除 client 
        // 示意图：
        // before:
        // {
        //  'channel-x' : [c1, c2, c3],
        // }
        // after c2 unsubscribe channel-x:
        // {
        //  'channel-x' : [c1, c3]
        // }
        de = dictFind(server.pubsub_channels,channel);
        serverAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        ln = listSearchKey(clients,c);
        serverAssertWithInfo(c,NULL,ln != NULL);
        listDelNode(clients,ln);
        // 如果移除 client 之后链表为空，那么删除这个 channel 键
        // 示意图：
        // before
        // {
        //  'channel-x' : [c1]
        // }
        // after c1 ubsubscribe channel-x
        // then also delete 'channel-x' key in dict
        // {
        //  // nothing here
        // }
        if (listLength(clients) == 0) {
            /* Free the list and associated hash entry at all if this was
             * the latest client, so that it will be possible to abuse
             * Redis PUBSUB creating millions of channels. */
            dictDelete(server.pubsub_channels,channel);
        }
    }
    /* Notify the client */
 	// 回复客户端
       // "ubsubscribe" 字符串
        // 被退订的频道
  // 退订频道之后客户端仍在订阅的频道和模式的总数
    if (notify) addReplyPubsubUnsubscribed(c,channel);
    decrRefCount(channel); /* it is finally safe to release it */
    return retval;
}

/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the client was already subscribed to that pattern. */
/* Subscribe a client to a pattern. Returns 1 if the operation succeeded, or 0 if the client was already subscribed to that pattern. 
 *
 * 设置客户端 c 订阅模式 pattern 。
 *
 * 订阅成功返回 1 ，如果客户端已经订阅了该模式，那么返回 0 。
 */
int pubsubSubscribePattern(client *c, robj *pattern) {
    dictEntry *de;
    list *clients;
    int retval = 0;

    // 在链表中查找模式，看客户端是否已经订阅了这个模式
    // 这里为什么不像 channel 那样，用字典来进行检测呢？
    // 虽然 pattern 的数量一般来说并不多
    if (listSearchKey(c->pubsub_patterns,pattern) == NULL) {
        retval = 1;
        // 将 pattern 添加到 c->pubsub_patterns 链表中
        listAddNodeTail(c->pubsub_patterns,pattern);
        incrRefCount(pattern);
        /* Add the client to the pattern -> list of clients hash table */
        de = dictFind(server.pubsub_patterns,pattern);
        if (de == NULL) {
       		// 创建并设置新的 pubsubPattern 结构
            clients = listCreate();
            dictAdd(server.pubsub_patterns,pattern,clients);
            incrRefCount(pattern);
        } else {
            clients = dictGetVal(de);
        }

        // 添加到末尾
        listAddNodeTail(clients,c);
    }
    /* Notify the client */
    // 回复客户端。
    // 示例：
    // redis 127.0.0.1:6379> PSUBSCRIBE xxx*
    // Reading messages... (press Ctrl-C to quit)
    // 1) "psubscribe"
    // 2) "xxx*"
    // 3) (integer) 1
    // 回复 "psubscribe" 字符串
    // 回复被订阅的模式
    // 回复客户端订阅的频道和模式的总数
    addReplyPubsubPatSubscribed(c,pattern);
    return retval;
}

/* Unsubscribe a client from a channel. Returns 1 if the operation succeeded, or
 * 0 if the client was not subscribed to the specified channel. 
 *
 * 取消客户端 c 对模式 pattern 的订阅。
 *
 * 取消成功返回 1 ，因为客户端未订阅 pattern 而造成取消失败，返回 0 。
 */
int pubsubUnsubscribePattern(client *c, robj *pattern, int notify) {
    dictEntry *de;
    list *clients;
    listNode *ln;
    int retval = 0;

    incrRefCount(pattern); /* Protect the object. May be the same we remove */
    // 先确认一下，客户端是否订阅了这个模式
    if ((ln = listSearchKey(c->pubsub_patterns,pattern)) != NULL) {
        retval = 1;
        // 将模式从客户端的订阅列表中删除
        listDelNode(c->pubsub_patterns,ln);
        /* Remove the client from the pattern -> clients list hash table */
        // 在服务器中查找
        de = dictFind(server.pubsub_patterns,pattern);
        serverAssertWithInfo(c,NULL,de != NULL);
        clients = dictGetVal(de);
        ln = listSearchKey(clients,c);
        serverAssertWithInfo(c,NULL,ln != NULL);
        listDelNode(clients,ln);
        if (listLength(clients) == 0) {
            /* Free the list and associated hash entry at all if this was
             * the latest client. */
            dictDelete(server.pubsub_patterns,pattern);
        }
    }
    /* Notify the client */
    // 回复客户端
        // "punsubscribe" 字符串
        // 被退订的模式
        // 退订频道之后客户端仍在订阅的频道和模式的总数
    if (notify) addReplyPubsubPatUnsubscribed(c,pattern);
    decrRefCount(pattern);
    return retval;
}

/* Unsubscribe from all the channels. Return the number of channels the
 * client was subscribed to. 
 *
 * 退订客户端 c 订阅的所有频道。
 *
 * 返回被退订频道的总数。
 *
int pubsubUnsubscribeAllChannels(client *c, int notify) {
    int count = 0;
    if (dictSize(c->pubsub_channels) > 0) {
    // 频道迭代器
        dictIterator *di = dictGetSafeIterator(c->pubsub_channels);
        dictEntry *de;
   	 // 退订
        while((de = dictNext(di)) != NULL) {
            robj *channel = dictGetKey(de);

            count += pubsubUnsubscribeChannel(c,channel,notify);
        }
        dictReleaseIterator(di);
    }
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) addReplyPubsubUnsubscribed(c,NULL);
    // 被退订的频道的数量
    return count;
}

/* Unsubscribe from all the patterns. Return the number of patterns the
 * client was subscribed from. 
 *
 * 退订客户端 c 订阅的所有模式。
 *
 * 返回被退订模式的数量。
 */
int pubsubUnsubscribeAllPatterns(client *c, int notify) {
    listNode *ln;
    listIter li;
    int count = 0;

    // 迭代客户端订阅模式的链表
    listRewind(c->pubsub_patterns,&li);
    while ((ln = listNext(&li)) != NULL) {
        robj *pattern = ln->value;

        // 退订，并计算退订数
        count += pubsubUnsubscribePattern(c,pattern,notify);
    }

    // 如果在执行这个函数时，客户端没有订阅任何模式，
    // 那么向客户端发送回复
    if (notify && count == 0) addReplyPubsubPatUnsubscribed(c,NULL);
    // 退订总数
    return count;
}

/* Publish a message 
 *
 * 将 message 发送到所有订阅频道 channel 的客户端，
 * 以及所有订阅了和 channel 频道匹配的模式的客户端。
 */
int pubsubPublishMessage(robj *channel, robj *message) {
    int receivers = 0;
    dictEntry *de;
    dictIterator *di;
    listNode *ln;
    listIter li;

    /* Send to clients listening for that channel */
    // 取出包含所有订阅频道 channel 的客户端的链表
    // 并将消息发送给它们
    de = dictFind(server.pubsub_channels,channel);
    if (de) {
        list *list = dictGetVal(de);
        listNode *ln;
        listIter li;

        // 遍历客户端链表，将 message 发送给它们
        listRewind(list,&li);
        while ((ln = listNext(&li)) != NULL) {
            client *c = ln->value;
            addReplyPubsubMessage(c,channel,message);
            // 回复客户端。
            // 示例：
            // 1) "message"
            // 2) "xxx"
            // 3) "hello"
            // "message" 字符串
            // 消息的来源频道
            // 消息内容
            // 接收客户端计数
            receivers++;
        }
    }
    /* Send to clients listening to matching channels */
    // 将消息也发送给那些和频道匹配的模式
    di = dictGetIterator(server.pubsub_patterns);
    if (di) {
        // 遍历模式链表        channel = getDecodedObject(channel);
        while((de = dictNext(di)) != NULL) {
            // 取出 robj
            robj *pattern = dictGetKey(de);
            list *clients = dictGetVal(de);


            if (!stringmatchlen((char*)pattern->ptr,
                                sdslen(pattern->ptr),
                                (char*)channel->ptr,
                                sdslen(channel->ptr),0)) continue;

            // 如果 channel 和 pattern 匹配
            // 就给所有订阅该 pattern 的客户端发送消息
            listRewind(clients,&li);
            while ((ln = listNext(&li)) != NULL) {
                client *c = listNodeValue(ln);

                // 回复客户端
                // 示例：
                // 1) "pmessage"
                // 2) "*"
                // 3) "xxx"
                // 4) "hello"
                addReplyPubsubPatMessage(c,pattern,channel,message);
                // 对接收消息的客户端进行计数
                receivers++;
            }
        }
        decrRefCount(channel);
        dictReleaseIterator(di);
    }
    return receivers;
}

/*-----------------------------------------------------------------------------
 * Pubsub commands implementation
 *----------------------------------------------------------------------------*/

/* SUBSCRIBE channel [channel ...] */
void subscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backword compatibility
         */
        addReplyError(c, "SUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (j = 1; j < c->argc; j++)
        pubsubSubscribeChannel(c,c->argv[j]);
    c->flags |= CLIENT_PUBSUB;
}

/* UNSUBSCRIBE [channel [channel ...]] */
void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribeChannel(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~CLIENT_PUBSUB;
}

/* PSUBSCRIBE pattern [pattern ...] */
void psubscribeCommand(client *c) {
    int j;
    if ((c->flags & CLIENT_DENY_BLOCKING) && !(c->flags & CLIENT_MULTI)) {
        /**
         * A client that has CLIENT_DENY_BLOCKING flag on
         * expect a reply per command and so can not execute subscribe.
         *
         * Notice that we have a special treatment for multi because of
         * backword compatibility
         */
        addReplyError(c, "PSUBSCRIBE isn't allowed for a DENY BLOCKING client");
        return;
    }

    for (j = 1; j < c->argc; j++)
        pubsubSubscribePattern(c,c->argv[j]);
    c->flags |= CLIENT_PUBSUB;
}

/* PUNSUBSCRIBE [pattern [pattern ...]] */
void punsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllPatterns(c,1);
    } else {
        int j;

        for (j = 1; j < c->argc; j++)
            pubsubUnsubscribePattern(c,c->argv[j],1);
    }
    if (clientSubscriptionsCount(c) == 0) c->flags &= ~CLIENT_PUBSUB;
}

/* PUBLISH <channel> <message> */
void publishCommand(client *c) {
    int receivers = pubsubPublishMessage(c->argv[1],c->argv[2]);
    if (server.cluster_enabled)
        clusterPropagatePublish(c->argv[1],c->argv[2]);
    else
        forceCommandPropagation(c,PROPAGATE_REPL);
    addReplyLongLong(c,receivers);
}

/* PUBSUB command for Pub/Sub introspection. */
void pubsubCommand(client *c) {
    if (c->argc == 2 && !strcasecmp(c->argv[1]->ptr,"help")) {
        const char *help[] = {
"CHANNELS [<pattern>]",
"    Return the currently active channels matching a <pattern> (default: '*').",
"NUMPAT",
"    Return number of subscriptions to patterns.",
"NUMSUB [<channel> ...]",
"    Return the number of subscribers for the specified channels, excluding",
"    pattern subscriptions(default: no channels).",
NULL
        };
        addReplyHelp(c, help);
    } else if (!strcasecmp(c->argv[1]->ptr,"channels") &&
        (c->argc == 2 || c->argc == 3))
    {
        /* PUBSUB CHANNELS [<pattern>] */
        // 检查命令请求是否给定了 pattern 参数
        // 如果没有给定的话，就设为 NULL
        sds pat = (c->argc == 2) ? NULL : c->argv[2]->ptr;
        // 创建 pubsub_channels 的字典迭代器
        // 该字典的键为频道，值为链表
        // 链表中保存了所有订阅键所对应的频道的客户端
        dictIterator *di = dictGetIterator(server.pubsub_channels);
        dictEntry *de;
        long mblen = 0;
        void *replylen;

        replylen = addReplyDeferredLen(c);
        // 从迭代器中获取一个客户端
        while((de = dictNext(di)) != NULL) {
            // 从字典中取出客户端所订阅的频道
            robj *cobj = dictGetKey(de);
            sds channel = cobj->ptr;

            // 顺带一提
            // 因为 Redis 的字典实现只能遍历字典的值（客户端）
            // 所以这里才会有遍历字典值然后通过字典值取出字典键（频道）的蹩脚用法

            // 如果没有给定 pattern 参数，那么打印所有找到的频道
            // 如果给定了 pattern 参数，那么只打印和 pattern 相匹配的频道
            if (!pat || stringmatchlen(pat, sdslen(pat),
                                       channel, sdslen(channel),0))
            {
                // 向客户端输出频道
                addReplyBulk(c,cobj);
                mblen++;
            }
        }
        // 释放字典迭代器
        dictReleaseIterator(di);
        setDeferredArrayLen(c,replylen,mblen);
    } else if (!strcasecmp(c->argv[1]->ptr,"numsub") && c->argc >= 2) {
        /* PUBSUB NUMSUB [Channel_1 ... Channel_N] */
        int j;

        addReplyArrayLen(c,(c->argc-2)*2);
        for (j = 2; j < c->argc; j++) {
            // c->argv[j] 也即是客户端输入的第 N 个频道名字
            // pubsub_channels 的字典为频道名字
            // 而值则是保存了 c->argv[j] 频道所有订阅者的链表
            // 而调用 dictFetchValue 也就是取出所有订阅给定频道的客户端
            list *l = dictFetchValue(server.pubsub_channels,c->argv[j]);

            addReplyBulk(c,c->argv[j]);
            // 向客户端返回链表的长度属性
            // 这个属性就是某个频道的订阅者数量
            // 例如：如果一个频道有三个订阅者，那么链表的长度就是 3
            // 而返回给客户端的数字也是三
            addReplyLongLong(c,l ? listLength(l) : 0);
        }
    } else if (!strcasecmp(c->argv[1]->ptr,"numpat") && c->argc == 2) {
        /* PUBSUB NUMPAT */
        // pubsub_patterns 链表保存了服务器中所有被订阅的模式
        // pubsub_patterns 的长度就是服务器中被订阅模式的数量
        addReplyLongLong(c,dictSize(server.pubsub_patterns));
    // 错误处理
    } else {
        addReplySubcommandSyntaxError(c);
    }
}
