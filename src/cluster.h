#ifndef __CLUSTER_H
#define __CLUSTER_H

/*-----------------------------------------------------------------------------
 * Redis cluster data structures, defines, exported API.
 *----------------------------------------------------------------------------*/

// ������
#define CLUSTER_SLOTS 16384
// ��Ⱥ����
#define CLUSTER_OK 0          /* Everything looks ok */
// ��Ⱥ����
#define CLUSTER_FAIL 1        /* The cluster can't work */
// �ڵ����ֵĳ���
#define CLUSTER_NAMELEN 40    /* sha1 hex length */

// ��Ⱥ��ʵ�ʶ˿ں� = �û�ָ���Ķ˿ں� + REDIS_CLUSTER_PORT_INCR
#define CLUSTER_PORT_INCR 10000 /* Cluster port = baseport + PORT_INCR */

/* The following defines are amount of time, sometimes expressed as
 * multiplicators of the node timeout value (when ending with MULT). 
 *
 * �����Ǻ�ʱ���йص�һЩ������
 * �� _MULTI ��β�ĳ�������Ϊʱ��ֵ�ĳ˷�������ʹ�á�
 */
// �������߱���ĳ˷�����
#define CLUSTER_FAIL_REPORT_VALIDITY_MULT 2 /* Fail report validity. */
// �������ڵ� FAIL ״̬�ĳ˷�����
#define CLUSTER_FAIL_UNDO_TIME_MULT 2 /* Undo fail if master is back. */
// �������ڵ� FAIL ״̬�ļӷ�����
#define CLUSTER_FAIL_UNDO_TIME_ADD 10 /* Some additional time. */
// ��ִ�й���ת��֮ǰ��Ҫ�ȴ����������ƺ��Ѿ�����
#define CLUSTER_FAILOVER_DELAY 5 /* Seconds */
// �ڽ����ֶ��Ĺ���ת��֮ǰ����Ҫ�ȴ��ĳ�ʱʱ��
#define CLUSTER_MF_TIMEOUT 5000 /* Milliseconds to do a manual failover. */
// δʹ�ã��ƺ��Ѿ�����
#define CLUSTER_MF_PAUSE_MULT 2 /* Master pause manual failover mult. */

#define CLUSTER_SLAVE_MIGRATION_DELAY 5000 /* Delay for slave migration. */

/* Redirection errors returned by getNodeByQuery(). */
/* �� getNodeByQuery() �������ص�ת����� */
// �ڵ���Դ����������
#define CLUSTER_REDIR_NONE 0          /* Node can serve the request. */
// ����������
#define CLUSTER_REDIR_CROSS_SLOT 1    /* -CROSSSLOT request. */
// �������Ĳ����ڽ��� reshard
#define CLUSTER_REDIR_UNSTABLE 2      /* -TRYAGAIN redirection required */
// ��Ҫ���� ASK ת��
#define CLUSTER_REDIR_ASK 3           /* -ASK redirection required. */
// ��Ҫ���� MOVED ת��
#define CLUSTER_REDIR_MOVED 4         /* -MOVED redirection required. */

#define CLUSTER_REDIR_DOWN_STATE 5    /* -CLUSTERDOWN, global state. */

#define CLUSTER_REDIR_DOWN_UNBOUND 6  /* -CLUSTERDOWN, unbound slot. */

#define CLUSTER_REDIR_DOWN_RO_STATE 7 /* -CLUSTERDOWN, allow reads. */

// ǰ�ö��壬��ֹ�������
struct clusterNode;

/* clusterLink encapsulates everything needed to talk with a remote node. */
// clusterLink �������������ڵ����ͨѶ�����ȫ����Ϣ
typedef struct clusterLink {
    // ���ӵĴ���ʱ��
    mstime_t ctime;             /* Link creation time */
    // TCP �׽���������
    connection *conn;           /* Connection to remote node */
    // ����������������ŵȴ����͸������ڵ����Ϣ��message����
    sds sndbuf;                 /* Packet send buffer */
    // ���뻺�����������Ŵ������ڵ���յ�����Ϣ��
    char *rcvbuf;               /* Packet reception buffer */
    size_t rcvbuf_len;          /* Used size of rcvbuf */
    size_t rcvbuf_alloc;        /* Allocated size of rcvbuf */
    // ���������������Ľڵ㣬���û�еĻ���Ϊ NULL
    struct clusterNode *node;   /* Node related to this link if any, or NULL */
} clusterLink;

/* Cluster node flags and macros. */
// �ýڵ�Ϊ���ڵ�
#define CLUSTER_NODE_MASTER 1     /* The node is a master */
// �ýڵ�Ϊ�ӽڵ�
#define CLUSTER_NODE_SLAVE 2      /* The node is a slave */
// �ýڵ��������ߣ���Ҫ������״̬����ȷ��
#define CLUSTER_NODE_PFAIL 4      /* Failure? Need acknowledge */
// �ýڵ�������
#define CLUSTER_NODE_FAIL 8       /* The node is believed to be malfunctioning */
// �ýڵ��ǵ�ǰ�ڵ�����
#define CLUSTER_NODE_MYSELF 16    /* This node is myself */
// �ýڵ㻹δ�뵱ǰ�ڵ���ɵ�һ�� PING - PONG ͨѶ
#define CLUSTER_NODE_HANDSHAKE 32 /* We have still to exchange the first ping */
// �ýڵ�û�е�ַ
#define CLUSTER_NODE_NOADDR   64  /* We don't know the address of this node */
// ��ǰ�ڵ㻹δ��ýڵ���й��Ӵ�
// ���������ʶ���õ�ǰ�ڵ㷢�� MEET ��������� PING ����
#define CLUSTER_NODE_MEET 128     /* Send a MEET message to this node */
// �ýڵ㱻ѡ��Ϊ�µ����ڵ�
#define CLUSTER_NODE_MIGRATE_TO 256 /* Master eligible for replica migration. */

#define CLUSTER_NODE_NOFAILOVER 512 /* Slave will not try to failover. */
// �����֣��ڽڵ�Ϊ���ڵ�ʱ��������Ϣ�е� slaveof ���Ե�ֵ��
#define CLUSTER_NODE_NULL_NAME "\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000\000"
// �����жϽڵ���ݺ�״̬��һϵ�к�
#define nodeIsMaster(n) ((n)->flags & CLUSTER_NODE_MASTER)
#define nodeIsSlave(n) ((n)->flags & CLUSTER_NODE_SLAVE)
#define nodeInHandshake(n) ((n)->flags & CLUSTER_NODE_HANDSHAKE)
#define nodeHasAddr(n) (!((n)->flags & CLUSTER_NODE_NOADDR))
#define nodeWithoutAddr(n) ((n)->flags & CLUSTER_NODE_NOADDR)
#define nodeTimedOut(n) ((n)->flags & CLUSTER_NODE_PFAIL)
#define nodeFailed(n) ((n)->flags & CLUSTER_NODE_FAIL)
#define nodeCantFailover(n) ((n)->flags & CLUSTER_NODE_NOFAILOVER)

/* Reasons why a slave is not able to failover. */
#define CLUSTER_CANT_FAILOVER_NONE 0
#define CLUSTER_CANT_FAILOVER_DATA_AGE 1
#define CLUSTER_CANT_FAILOVER_WAITING_DELAY 2
#define CLUSTER_CANT_FAILOVER_EXPIRED 3
#define CLUSTER_CANT_FAILOVER_WAITING_VOTES 4
#define CLUSTER_CANT_FAILOVER_RELOG_PERIOD (60*5) /* seconds. */

/* clusterState todo_before_sleep flags. */
// ����ÿ�� flag ������һ���������ڿ�ʼ��һ���¼�ѭ��֮ǰ
// Ҫ��������
#define CLUSTER_TODO_HANDLE_FAILOVER (1<<0)
#define CLUSTER_TODO_UPDATE_STATE (1<<1)
#define CLUSTER_TODO_SAVE_CONFIG (1<<2)
#define CLUSTER_TODO_FSYNC_CONFIG (1<<3)
#define CLUSTER_TODO_HANDLE_MANUALFAILOVER (1<<4)

/* Message types.
 *
 * Note that the PING, PONG and MEET messages are actually the same exact
 * kind of packet. PONG is the reply to ping, in the exact format as a PING,
 * while MEET is a special PING that forces the receiver to add the sender
 * as a node (if it is not already in the list). */
// ע�⣬PING �� PONG �� MEET ʵ������ͬһ����Ϣ��
// PONG �Ƕ� PING �Ļظ�������ʵ�ʸ�ʽҲΪ PING ��Ϣ��
// �� MEET ����һ������� PING ��Ϣ������ǿ����Ϣ�Ľ����߽���Ϣ�ķ�������ӵ���Ⱥ��
// ������ڵ���δ�ڽڵ��б��еĻ���
// PING
#define CLUSTERMSG_TYPE_PING 0          /* Ping */
// PONG ���ظ� PING��
#define CLUSTERMSG_TYPE_PONG 1          /* Pong (reply to Ping) */
// ����ĳ���ڵ���ӵ���Ⱥ��
#define CLUSTERMSG_TYPE_MEET 2          /* Meet "let's join" message */
// ��ĳ���ڵ���Ϊ FAIL
#define CLUSTERMSG_TYPE_FAIL 3          /* Mark node xxx as failing */
// ͨ�������붩�Ĺ��ܹ㲥��Ϣ
#define CLUSTERMSG_TYPE_PUBLISH 4       /* Pub/Sub Publish propagation */
// ������й���ת�Ʋ�����Ҫ����Ϣ�Ľ�����ͨ��ͶƱ��֧����Ϣ�ķ�����
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_REQUEST 5 /* May I failover? */
// ��Ϣ�Ľ�����ͬ������Ϣ�ķ�����ͶƱ
#define CLUSTERMSG_TYPE_FAILOVER_AUTH_ACK 6     /* Yes, you have my vote */
// �۲����Ѿ������仯����Ϣ������Ҫ����Ϣ�����߽�����Ӧ�ĸ���
#define CLUSTERMSG_TYPE_UPDATE 7        /* Another node slots configuration */
// Ϊ�˽����ֶ�����ת�ƣ���ͣ�����ͻ���
#define CLUSTERMSG_TYPE_MFSTART 8       /* Pause clients for manual failover */

#define CLUSTERMSG_TYPE_MODULE 9        /* Module cluster API message. */

#define CLUSTERMSG_TYPE_COUNT 10        /* Total number of message types. */


/* Flags that a module can set in order to prevent certain Redis Cluster
 * features to be enabled. Useful when implementing a different distributed
 * system on top of Redis Cluster message bus, using modules. */
#define CLUSTER_MODULE_FLAG_NONE 0
#define CLUSTER_MODULE_FLAG_NO_FAILOVER (1<<1)
#define CLUSTER_MODULE_FLAG_NO_REDIRECTION (1<<2)

/* This structure represent elements of node->fail_reports. */
// ÿ�� clusterNodeFailReport �ṹ������һ�������ڵ��Ŀ��ڵ�����߱���
// ����ΪĿ��ڵ��Ѿ����ߣ�
typedef struct clusterNodeFailReport {
    // ����Ŀ��ڵ��Ѿ����ߵĽڵ�
    struct clusterNode *node;  /* Node reporting the failure condition. */
    // ���һ�δ� node �ڵ��յ����߱����ʱ��
    // ����ʹ�����ʱ�����������߱����Ƿ����
    mstime_t time;             /* Time of the last report from this node. */
} clusterNodeFailReport;


// �ڵ�״̬
typedef struct clusterNode {
    // �����ڵ��ʱ��
    mstime_t ctime; /* Node object creation time. */

    // �ڵ�����֣��� 40 ��ʮ�������ַ����
    // ���� 68eef66df23420a5862208ef5b1a7005b806f2ff
    char name[CLUSTER_NAMELEN]; /* Node name, hex string, sha1-size */

    // �ڵ��ʶ
    // ʹ�ø��ֲ�ͬ�ı�ʶֵ��¼�ڵ�Ľ�ɫ���������ڵ���ߴӽڵ㣩��
    // �Լ��ڵ�Ŀǰ������״̬���������߻������ߣ���
    int flags;      /* CLUSTER_NODE_... */
�ڵ㵱ǰ�����ü�Ԫ������ʵ�ֹ���ת��
    uint64_t configEpoch; /* Last configEpoch observed for this node */

	// ������ڵ㸺����Ĳ�
    // һ���� REDIS_CLUSTER_SLOTS / 8 ���ֽڳ�
    // ÿ���ֽڵ�ÿ��λ��¼��һ���۵ı���״̬
    // λ��ֵΪ 1 ��ʾ�����ɱ��ڵ㴦��ֵΪ 0 ���ʾ�۲��Ǳ��ڵ㴦��
    // ���� slots[0] �ĵ�һ��λ�����˲� 0 �ı������
    // slots[0] �ĵڶ���λ�����˲� 1 �ı���������Դ�����
    unsigned char slots[CLUSTER_SLOTS/8]; /* slots handled by this node */


    // �ýڵ㸺����Ĳ���Ϣ
    sds slots_info; /* Slots info represented by string. */
    // �ýڵ㸺����Ĳ�����
    int numslots;   /* Number of slots handled by this node */
    // ������ڵ������ڵ㣬��ô��������Լ�¼�ӽڵ������
    int numslaves;  /* Number of slave nodes, if this is a master */
    // ָ�����飬ָ������ӽڵ�
    struct clusterNode **slaves; /* pointers to slave nodes */

    // �������һ���ӽڵ㣬��ôָ�����ڵ�
    struct clusterNode *slaveof; /* pointer to the master node. Note that it
                                    may be NULL even if the node is a slave
                                    if we don't have the master node in our
                                    tables. */

    // ���һ�η��� PING �����ʱ��
    mstime_t ping_sent;      /* Unix time we sent latest ping */
    // ���һ�ν��� PONG �ظ���ʱ���
    mstime_t pong_received;  /* Unix time we received the pong */
    mstime_t data_received;  /* Unix time we received any data */

    // ���һ�α�����Ϊ FAIL ״̬��ʱ��
    mstime_t fail_time;      /* Unix time when FAIL flag was set */
    // ���һ�θ�ĳ���ӽڵ�ͶƱ��ʱ��
    mstime_t voted_time;     /* Last time we voted for a slave of this master */
    // ���һ�δ�����ڵ���յ�����ƫ������ʱ��
    mstime_t repl_offset_time;  /* Unix time we received offset for this node */
    mstime_t orphaned_time;     /* Starting time of orphaned master condition */

    // ����ڵ�ĸ���ƫ����
    long long repl_offset;      /* Last known repl offset for this node. */

   // �ڵ�� IP ��ַ
    char ip[NET_IP_STR_LEN];  /* Latest known IP address of this node */

    // �ڵ�Ķ˿ں�
    int port;                   /* Latest known clients port (TLS or plain). */
    int pport;                  /* Latest known clients plaintext port. Only used
                                   if the main clients port is for TLS. */
    int cport;                  /* Latest known cluster port of this node. */
    // �������ӽڵ�������й���Ϣ
    clusterLink *link;          /* TCP/IP link with this node */
    // һ��������¼�����������ڵ�Ըýڵ�����߱���
    list *fail_reports;         /* List of nodes signaling this as failing */
} clusterNode;

// ��Ⱥ״̬��ÿ���ڵ㶼������һ��������״̬����¼���������еļ�Ⱥ�����ӡ�
// ���⣬��Ȼ����ṹ��Ҫ���ڼ�¼��Ⱥ�����ԣ�����Ϊ�˽�Լ��Դ��
// ��Щ��ڵ��йص����ԣ����� slots_to_keys �� failover_auth_count 
// Ҳ���ŵ�������ṹ���档
typedef struct clusterState {
    // ָ��ǰ�ڵ��ָ��
    clusterNode *myself;  /* This node */

    // ��Ⱥ��ǰ�����ü�Ԫ������ʵ�ֹ���ת��
    uint64_t currentEpoch;
    int state;            /* CLUSTER_OK, CLUSTER_FAIL, ... */
    // ��Ⱥ��ǰ��״̬�������߻�������
    int state;            /* REDIS_CLUSTER_OK, REDIS_CLUSTER_FAIL, ... */

    // ��Ⱥ�����ٴ�����һ���۵Ľڵ��������
    int size;             /* Num of master nodes with at least one slot */

    // ��Ⱥ�ڵ����������� myself �ڵ㣩
    // �ֵ�ļ�Ϊ�ڵ�����֣��ֵ��ֵΪ clusterNode �ṹ
    dict *nodes;          /* Hash table of name -> clusterNode structures */

    // �ڵ������������ CLUSTER FORGET ����
    // ��ֹ�� FORGET ���������±���ӵ���Ⱥ����
    // �����������ƺ�û����ʹ�õ����ӣ��ѷ�����������δʵ�֣���
    dict *nodes_black_list; /* Nodes we don't re-add for a few seconds. */


    // ��¼Ҫ�ӵ�ǰ�ڵ�Ǩ�Ƶ�Ŀ��ڵ�Ĳۣ��Լ�Ǩ�Ƶ�Ŀ��ڵ�
    // migrating_slots_to[i] = NULL ��ʾ�� i δ��Ǩ��
    // migrating_slots_to[i] = clusterNode_A ��ʾ�� i Ҫ�ӱ��ڵ�Ǩ�����ڵ� A
    clusterNode *migrating_slots_to[CLUSTER_SLOTS];


    // ��¼Ҫ��Դ�ڵ�Ǩ�Ƶ����ڵ�Ĳۣ��Լ�����Ǩ�Ƶ�Դ�ڵ�
    // importing_slots_from[i] = NULL ��ʾ�� i δ���е���
    // importing_slots_from[i] = clusterNode_A ��ʾ���ӽڵ� A �е���� i
    clusterNode *importing_slots_from[CLUSTER_SLOTS];


    // ����������۵Ľڵ�
    // ���� slots[i] = clusterNode_A ��ʾ�� i �ɽڵ� A ����
    clusterNode *slots[CLUSTER_SLOTS];


    uint64_t slots_keys_count[CLUSTER_SLOTS];



    // ��Ծ�������Բ���Ϊ��ֵ������Ϊ��Ա���Բ۽�����������
    // ����Ҫ��ĳЩ�۽������䣨range������ʱ�������Ծ������ṩ����
    // ������������� db.c ����
    rax *slots_to_keys;
    /* The following fields are used to take the slave state on elections. */
    // ������Щ�����ڽ��й���ת��ѡ��

    // �ϴ�ִ��ѡ�ٻ����´�ִ��ѡ�ٵ�ʱ��
    mstime_t failover_auth_time; /* Time of previous or next election. */

    // �ڵ��õ�ͶƱ����
    int failover_auth_count;    /* Number of votes received so far. */

    // ���ֵΪ 1 ����ʾ���ڵ��Ѿ��������ڵ㷢����ͶƱ����
    int failover_auth_sent;     /* True if we already asked for votes. */

    int failover_auth_rank;     /* This slave rank for current auth request. */
    uint64_t failover_auth_epoch; /* Epoch of the current election. */
    int cant_failover_reason;   /* Why a slave is currently not able to
                                   failover. See the CANT_FAILOVER_* macros. */
    /* Manual failover state in common. */
    /* ���õ��ֶ�����ת��״̬ */

    // �ֶ�����ת��ִ�е�ʱ������
    mstime_t mf_end;            /* Manual failover time limit (ms unixtime).
                                   It is zero if there is no MF in progress. */
    /* Manual failover state of master. */
    /* �����������ֶ�����ת��״̬ */
    clusterNode *mf_slave;      /* Slave performing the manual failover. */
    /* Manual failover state of slave. */
    /* �ӷ��������ֶ�����ת��״̬ */
    long long mf_master_offset; /* Master offset the slave needs to start MF
                                   or -1 if still not received. */
    // ָʾ�ֶ�����ת���Ƿ���Կ�ʼ�ı�־ֵ
    // ֵΪ�� 0 ʱ��ʾ���������������Կ�ʼͶƱ
    int mf_can_start;           /* If non-zero signal that the manual failover
                                   can start requesting masters vote. */

    /* The followign fields are uesd by masters to take state on elections. */
    /* ������Щ������������ʹ�ã����ڼ�¼ѡ��ʱ��״̬ */
    // ��Ⱥ���һ�ν���ͶƱ�ļ�Ԫ
    uint64_t lastVoteEpoch;     /* Epoch of the last vote granted. */

    // �ڽ����¸��¼�ѭ��֮ǰҪ�������飬�Ը��� flag ����¼
    int todo_before_sleep; /* Things to do in clusterBeforeSleep(). */
    /* Messages received and sent by type. */

   // ͨ�� cluster ���ӷ��͵���Ϣ����
    long long stats_bus_messages_sent[CLUSTERMSG_TYPE_COUNT];

    // ͨ�� cluster ���յ�����Ϣ����
    long long stats_bus_messages_received[CLUSTERMSG_TYPE_COUNT];
    long long stats_pfail_nodes;    /* Number of nodes in PFAIL status,
                                       excluding nodes without address. */
} clusterState;

/* Redis cluster messages header */

/* Initially we don't know our "name", but we'll find it once we connect
 * to the first node, using the getsockname() function. Then we'll use this
 * address for all the next messages. */
typedef struct {

    // �ڵ������
    // �ڸտ�ʼ��ʱ�򣬽ڵ�����ֻ��������
    // �� MEET ��Ϣ���Ͳ��õ��ظ�֮�󣬼�Ⱥ�ͻ�Ϊ�ڵ�������ʽ������
    char nodename[CLUSTER_NAMELEN];
    // ���һ����ýڵ㷢�� PING ��Ϣ��ʱ���
    uint32_t ping_sent;
    // ���һ�δӸýڵ���յ� PONG ��Ϣ��ʱ���
    uint32_t pong_received;
    // �ڵ�� IP ��ַ
    char ip[NET_IP_STR_LEN];  /* IP address last time it was seen */
    // �ڵ�Ķ˿ں�
    uint16_t port;              /* base port last time it was seen */

    uint16_t cport;             /* cluster port last time it was seen */

    // �ڵ�ı�ʶֵ
    uint16_t flags;             /* node->flags copy */
    uint16_t pport;             /* plaintext-port, when base port is TLS */

    // �����ֽڣ���ʹ��
    uint16_t notused1;
} clusterMsgDataGossip;

typedef struct {

    // ���߽ڵ������
    char nodename[CLUSTER_NAMELEN];
} clusterMsgDataFail;

typedef struct {
    // Ƶ��������
    uint32_t channel_len;
   // ��Ϣ����
    uint32_t message_len;

    // ��Ϣ���ݣ���ʽΪ Ƶ����+��Ϣ
    // bulk_data[0:channel_len-1] ΪƵ����
    // bulk_data[channel_len:channel_len+message_len-1] Ϊ��Ϣ
    unsigned char bulk_data[8]; /* 8 bytes just as placeholder. */
} clusterMsgDataPublish;

typedef struct {
    // �ڵ�����ü�Ԫ
    uint64_t configEpoch; /* Config epoch of the specified instance. */

    // �ڵ������
    char nodename[CLUSTER_NAMELEN]; /* Name of the slots owner. */

 // �ڵ�Ĳ۲���
    unsigned char slots[CLUSTER_SLOTS/8]; /* Slots bitmap. */
} clusterMsgDataUpdate;

typedef struct {
    uint64_t module_id;     /* ID of the sender module. */
    uint32_t len;           /* ID of the sender module. */
    uint8_t type;           /* Type from 0 to 255. */
    unsigned char bulk_data[3]; /* 3 bytes just as placeholder. */
} clusterMsgModule;

union clusterMsgData {
    /* PING, MEET and PONG */
    struct {
        /* Array of N clusterMsgDataGossip structures */
        // ÿ����Ϣ���������� clusterMsgDataGossip �ṹ
        clusterMsgDataGossip gossip[1];
    } ping;

    /* FAIL */
    struct {
        clusterMsgDataFail about;
    } fail;

    /* PUBLISH */
    struct {
        clusterMsgDataPublish msg;
    } publish;

    /* UPDATE */
    struct {
        clusterMsgDataUpdate nodecfg;
    } update;

    /* MODULE */
    struct {
        clusterMsgModule msg;
    } module;
};

#define CLUSTER_PROTO_VER 1 /* Cluster bus protocol version. */
// ������ʾ��Ⱥ��Ϣ�Ľṹ����Ϣͷ��header��

typedef struct {
    char sig[4];        /* Signature "RCmb" (Redis Cluster message bus). */
    // ��Ϣ�ĳ��ȣ����������Ϣͷ�ĳ��Ⱥ���Ϣ���ĵĳ��ȣ�
    uint32_t totlen;    /* Total length of this message */
    uint16_t ver;       /* Protocol version, currently set to 1. */
    uint16_t port;      /* TCP base port number. */
    // ��Ϣ������
    uint16_t type;      /* Message type */
    // ��Ϣ���İ����Ľڵ���Ϣ����
    // ֻ�ڷ��� MEET �� PING �� PONG ������ Gossip Э����Ϣʱʹ��
    uint16_t count;     /* Only used for some kind of messages. */
    // ��Ϣ�����ߵ����ü�Ԫ
    uint64_t currentEpoch;  /* The epoch accordingly to the sending node. */

    // �����Ϣ��������һ�����ڵ㣬��ô�����¼������Ϣ�����ߵ����ü�Ԫ
    // �����Ϣ��������һ���ӽڵ㣬��ô�����¼������Ϣ���������ڸ��Ƶ����ڵ�����ü�Ԫ
    uint64_t configEpoch;   /* The config epoch if it's a master, or the last
                               epoch advertised by its master if it is a
                               slave. */

    // �ڵ�ĸ���ƫ����
    uint64_t offset;    /* Master replication offset if node is a master or
                           processed replication offset if node is a slave. */


    // ��Ϣ�����ߵ����֣�ID��
    char sender[CLUSTER_NAMELEN]; /* Name of the sender node */



    // ��Ϣ������Ŀǰ�Ĳ�ָ����Ϣ
    unsigned char myslots[CLUSTER_SLOTS/8];


    // �����Ϣ��������һ���ӽڵ㣬��ô�����¼������Ϣ���������ڸ��Ƶ����ڵ������
    // �����Ϣ��������һ�����ڵ㣬��ô�����¼���� REDIS_NODE_NULL_NAME
    // ��һ�� 40 �ֽڳ���ֵȫΪ 0 ���ֽ����飩
    char slaveof[CLUSTER_NAMELEN];
    char myip[NET_IP_STR_LEN];    /* Sender IP, if not all zeroed. */
    char notused1[32];  /* 32 bytes reserved for future usage. */

    // ��Ϣ�����ߵĶ˿ں�
    uint16_t pport;      /* Sender TCP plaintext port, if base port is TLS */
    uint16_t cport;      /* Sender TCP cluster bus port */

    // ��Ϣ�����ߵı�ʶֵ
    uint16_t flags;      /* Sender node flags */
    // ��Ϣ������������Ⱥ��״̬
    unsigned char state; /* Cluster state from the POV of the sender */
    // ��Ϣ��־
    unsigned char mflags[3]; /* Message flags: CLUSTERMSG_FLAG[012]_... */
    // ��Ϣ�����ģ�����˵�����ݣ�
    union clusterMsgData data;
} clusterMsg;

#define CLUSTERMSG_MIN_LEN (sizeof(clusterMsg)-sizeof(union clusterMsgData))

/* Message flags better specify the packet content or are used to
 * provide some information about the node state. */
#define CLUSTERMSG_FLAG0_PAUSED (1<<0) /* Master paused for manual failover. */
#define CLUSTERMSG_FLAG0_FORCEACK (1<<1) /* Give ACK to AUTH_REQUEST even if
                                            master is up. */

/* ---------------------- API exported outside cluster.c -------------------- */
clusterNode *getNodeByQuery(client *c, struct redisCommand *cmd, robj **argv, int argc, int *hashslot, int *ask);
int clusterRedirectBlockedClientIfNeeded(client *c);
void clusterRedirectClient(client *c, clusterNode *n, int hashslot, int error_code);
unsigned long getClusterConnectionsCount(void);

#endif /* __CLUSTER_H */
