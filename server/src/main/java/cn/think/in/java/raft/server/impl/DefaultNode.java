/*
Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
 */
package cn.think.in.java.raft.server.impl;

import cn.think.in.java.raft.common.entity.*;
import cn.think.in.java.raft.server.Consensus;
import cn.think.in.java.raft.server.LogModule;
import cn.think.in.java.raft.server.Node;
import cn.think.in.java.raft.server.StateMachine;
import cn.think.in.java.raft.server.changes.ClusterMembershipChanges;
import cn.think.in.java.raft.server.changes.Result;
import cn.think.in.java.raft.common.RaftRemotingException;
import cn.think.in.java.raft.server.constant.StateMachineSaveType;
import cn.think.in.java.raft.server.current.RaftThreadPool;
import cn.think.in.java.raft.server.rpc.DefaultRpcServiceImpl;
import cn.think.in.java.raft.server.rpc.RpcService;
import cn.think.in.java.raft.server.util.LongConvert;
import cn.think.in.java.raft.common.rpc.DefaultRpcClient;
import cn.think.in.java.raft.common.rpc.Request;
import cn.think.in.java.raft.common.rpc.RpcClient;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

/**
 * 抽象机器节点, 初始为 follower, 角色随时变化.
 *
 * @author 莫那·鲁道
 */
@Getter
@Setter
@Slf4j
public class DefaultNode implements Node, ClusterMembershipChanges {

    /** 选举时间间隔基数 */
    public volatile long electionTime = 1500;
    /** 选举时间间隔波动范围 */
    public volatile long electionTimeout = ThreadLocalRandom.current().nextInt(0, 1000);
    /** 上一次选举时间 */
    public volatile long preElectionTime = 0;

    /** 上次一心跳时间戳 */
    public volatile long preHeartBeatTime = 0;
    /** 心跳间隔基数 */
    public final long heartBeatTick = 5 * 100;


    private HeartBeatTask heartBeatTask = new HeartBeatTask();
    private ElectionTask electionTask = new ElectionTask();
    private ReplicationFailQueueConsumer replicationFailQueueConsumer = new ReplicationFailQueueConsumer();

    private LinkedBlockingQueue<ReplicationFailModel> replicationFailQueue = new LinkedBlockingQueue<>(2048);


    /**
     * 节点当前状态
     *
     * @see NodeStatus
     */
    public volatile int status = NodeStatus.FOLLOWER;

    public PeerSet peerSet;

    volatile boolean running = false;

    /* ============ 所有服务器上持久存在的 ============= */

    /** 服务器最后一次知道的任期号（初始化为 0，持续递增） */
    volatile long currentTerm = 0;
    /** 在当前获得选票的候选人的 Id */
    volatile String votedFor;
    /** 日志条目集；每一个条目包含一个用户状态机执行的指令，和收到时的任期号 */
    LogModule logModule;



    /* ============ 所有服务器上经常变的 ============= */

    /** 已知的最大的已经被提交的日志条目的索引值 */
    volatile long commitIndex;

    /** 最后被应用到状态机的日志条目索引值（初始化为 0，持续递增) */
    volatile long lastApplied = 0;

    /* ========== 在领导人里经常改变的(选举后重新初始化) ================== */

    /** 对于每一个服务器，需要发送给他的下一个日志条目的索引值（初始化为领导人最后索引值加一） */
    Map<Peer, Long> nextIndexs;

    /** 对于每一个服务器，已经复制给他的日志的最高索引值 */
    Map<Peer, Long> matchIndexs;



    /* ============================== */

    public NodeConfig config;

    public RpcService rpcServer;

    public RpcClient rpcClient = new DefaultRpcClient();

    public StateMachine stateMachine;

    /* ============================== */

    /** 一致性模块实现 */
    Consensus consensus;

    ClusterMembershipChanges delegate;


    /* ============================== */

    private DefaultNode() {
    }

    public static DefaultNode getInstance() {
        return DefaultNodeLazyHolder.INSTANCE;
    }


    private static class DefaultNodeLazyHolder {

        private static final DefaultNode INSTANCE = new DefaultNode();
    }

    @Override
    public void init() throws Throwable {
        running = true;
        rpcServer.init();
        rpcClient.init();

        consensus = new DefaultConsensus(this);
        delegate = new ClusterMembershipChangesImpl(this);

        RaftThreadPool.scheduleWithFixedDelay(heartBeatTask, 200);
        RaftThreadPool.scheduleWithFixedDelay(electionTask,  200);
        RaftThreadPool.execute(replicationFailQueueConsumer);

        LogEntry logEntry = logModule.getLast();
        if (logEntry != null) {
            currentTerm = logEntry.getTerm();
        }

        log.info("start success, selfId : {} ", peerSet.getSelf());
    }

    @Override
    public void setConfig(NodeConfig config) {
        this.config = config;
        stateMachine = StateMachineSaveType.getForType(config.getStateMachineSaveType()).getStateMachine();
        logModule = DefaultLogModule.getInstance();

        peerSet = PeerSet.getInstance();
        for (String s : config.getPeerAddrs()) {
            Peer peer = new Peer(s);
            peerSet.addPeer(peer);
            if (s.equals("localhost:" + config.getSelfPort())) {
                peerSet.setSelf(peer);
            }
        }

        rpcServer = new DefaultRpcServiceImpl(config.selfPort, this);
    }

    @Override
    public RvoteResult handlerRequestVote(RvoteParam param) {
        log.warn("handlerRequestVote will be invoke, param info : {}", param);
        return consensus.requestVote(param);
    }

    @Override
    public AentryResult handlerAppendEntries(AentryParam param) {
        if (param.getEntries() != null) {
            log.warn("node receive node {} append entry, entry content = {}", param.getLeaderId(), param.getEntries());
        }
        return consensus.appendEntries(param);
    }

    @Override
    public ClientKVAck redirect(ClientKVReq request) {
        Request r = Request.builder()
                .obj(request)
                .url(peerSet.getLeader().getAddr())
                .cmd(Request.CLIENT_REQ).build();
        return rpcClient.send(r);
    }

    /**
     * 客户端的每一个请求都包含一条被复制状态机执行的指令。
     * 领导人把这条指令作为一条新的日志条目附加到日志中去，然后并行的发起附加条目 RPCs 给其他的服务器，让他们复制这条日志条目。
     * 当这条日志条目被安全的复制（下面会介绍），领导人会应用这条日志条目到它的状态机中然后把执行的结果返回给客户端。
     * 如果跟随者崩溃或者运行缓慢，再或者网络丢包，
     * 领导人会先回复“预提交”，然后不断的重复尝试附加日志条目 RPCs （尽管已经回复了客户端）直到所有的跟随者都最终存储了所有的日志条目。
     */
    @Override
    public synchronized ClientKVAck handlerClientRequest(ClientKVReq request) {
        log.warn("handlerClientRequest handler {} operation,  and key : [{}], value : [{}]",
                ClientKVReq.Type.value(request.getType()), request.getKey(), request.getValue());
        if (status != NodeStatus.LEADER) {
            log.warn("I not am leader , only invoke redirect method, leader addr : {}, my addr : {}",
                    peerSet.getLeader(), peerSet.getSelf().getAddr());
            return redirect(request);
        }
        // 读请求不过日志
        // 【风险】：Leader 已经失去领导权（网络分区），却还没及时感知到自己“落选”
        // TODO Read-Index: 在读前向多数派做一次 lightweight 协商，以确保 Leader 身份有效，然后读取本地状态机
        // TODO Lease-Read: Leader 在“租约期”内无需再跟其他节点确认，就能立即读本地状态机
        if (request.getType() == ClientKVReq.GET) {
            LogEntry logEntry = stateMachine.get(request.getKey());
            return new ClientKVAck(logEntry);
        }
        LogEntry logEntry = LogEntry.builder()
                .command(Command.builder().
                        key(request.getKey()).
                        value(request.getValue()).
                        build())
                .term(currentTerm)
                .build();
        // 预提交到本地日志, TODO 预提交
        logModule.write(logEntry);
        log.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());
        final AtomicInteger success = new AtomicInteger(0);
        List<Future<Boolean>> futureList = new ArrayList<>();
        // 并行复制到其他结点
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            // TODO check self and RaftThreadPool
            futureList.add(replication(peer, logEntry));
        }
        CountDownLatch latch = new CountDownLatch(futureList.size());
        // 保障读写并发时线程安全
        List<Boolean> resultList = new CopyOnWriteArrayList<>();
        getRPCAppendResult(futureList, latch, resultList);
        try {
            latch.await(4000, MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        // resultList 为遍历开始时候的副本
        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }
        // 尝试推进 commitIndex
        tryAdvanceCommitIndex();
        // 只有日志被正式 commit 才允许 apply + 响应客户端，否则响应“预提交”
        // 【可选】 同步写 + 超时放弃（不推荐）
        // 【可选】 同步等待, 让客户端强一致地拿到写成功确认
        // TODO 【可选】 异步返回，提升吞吐，但客户端要能接受“短暂未复制完全”的结果，需要幂等或重试机制
        if (logEntry.getIndex() <= commitIndex) {
            return ClientKVAck.ok();
        } else {
            log.warn("fail apply local state  machine,  logEntry info : {}", logEntry);
            // TODO 不应用到状态机,但已经记录到日志中.由定时任务从重试队列取出,然后重复尝试,当达到条件时,应用到状态机.
            return ClientKVAck.preOk();
        }
    }

    private void getRPCAppendResult(List<Future<Boolean>> futureList, CountDownLatch latch, List<Boolean> resultList) {
        for (Future<Boolean> future : futureList) {
            RaftThreadPool.execute(() -> {
                try {
                    // 等待最多 timeout 毫秒获取结果，如果在此时间内没完成，抛 TimeoutException，并不会自动取消或终止任务。
                    resultList.add(future.get(3000, MILLISECONDS));
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                    resultList.add(false);
                } finally {
                    latch.countDown();
                }
            });
        }
    }


    /** 复制到其他机器 */
    public Future<Boolean> replication(Peer peer, LogEntry entry) {
        return RaftThreadPool.submit(() -> {
            AentryParam aentryParam = AentryParam.builder().build();
            aentryParam.setTerm(currentTerm);
            aentryParam.setServerId(peer.getAddr());
            aentryParam.setLeaderId(peerSet.getSelf().getAddr());
            aentryParam.setLeaderCommit(commitIndex);
            // 按照 nextIndexs, 决定要发送哪些日志
            Long nextIndex = nextIndexs.get(peer);
            LinkedList<LogEntry> logEntries = new LinkedList<>();
            if (entry.getIndex() >= nextIndex) {
                for (long i = nextIndex; i <= entry.getIndex(); i++) {
                    LogEntry l = logModule.read(i);
                    if (l != null) {
                        logEntries.add(l);
                    }
                }
            } else {
                logEntries.add(entry);
            }
            // 最小的那个日志.
            LogEntry preLog = getPreLog(logEntries.getFirst());
            aentryParam.setPreLogTerm(preLog.getTerm());
            aentryParam.setPrevLogIndex(preLog.getIndex());
            aentryParam.setEntries(logEntries.toArray(new LogEntry[0]));
            Request request = Request.builder()
                    .cmd(Request.A_ENTRIES)
                    .obj(aentryParam)
                    .url(peer.getAddr())
                    .build();
            try {
                AentryResult result = getRpcClient().send(request);
                if (result == null) {
                    enqueueFail(peer, entry);
                    return false;
                }
                if (result.isSuccess()) {
                    log.info("append follower entry success , follower=[{}], entry=[{}]", peer, aentryParam.getEntries());
                    // 最大值保护，防止覆盖之前已经成功同步更大 index 的状态
                    nextIndexs.compute(peer, (k, oldVal) -> oldVal == null ? entry.getIndex() + 1 : Math.max(oldVal, entry.getIndex() + 1));
                    matchIndexs.compute(peer, (k, oldVal) -> oldVal == null ? entry.getIndex() : Math.max(oldVal, entry.getIndex()));
                    return true;
                } else {
                    // 对方比我大，认怂，变成跟随者
                    if (result.getTerm() > currentTerm) {
                        log.warn("follower [{}] term [{}] than more self, and my term = [{}], so, I will become follower",
                                peer, result.getTerm(), currentTerm);
                        currentTerm = result.getTerm();
                        status = NodeStatus.FOLLOWER;
                        return false;
                    } else {
                        // 没我大, 却失败了,说明 index 不对.或者 term 不对，递减
                        if (nextIndex == 0) {
                            nextIndex = 1L;
                        }
                        nextIndexs.put(peer, nextIndex - 1);
                        log.warn("follower {} nextIndex not match, will reduce nextIndex and retry RPC append, nextIndex : [{}]", peer.getAddr(),
                                nextIndex);
                        // 重来, 直到成功（为提高效率，队列重试）
                        enqueueFail(peer, entry);
                        return false;
                    }
                }
            } catch (Exception e) {
                log.warn("replication RPC exception: {}", e.getMessage(), e);
                // TODO 到底要不要放队列重试?
                // 发生异常 => 放队列重试
                enqueueFail(peer, entry);
                return false;
            }
        });
    }

    private void enqueueFail(Peer peer, LogEntry entry) {
        ReplicationFailModel model = ReplicationFailModel.builder()
                .callable(() -> {
                    Future<Boolean> f = replication(peer, entry);
                    return f.get(3000, TimeUnit.MILLISECONDS);
                })
                .logEntry(entry)
                .peer(peer)
                .offerTime(System.currentTimeMillis())
                .build();
        replicationFailQueue.offer(model);
    }

    private LogEntry getPreLog(LogEntry logEntry) {
        LogEntry entry = logModule.read(logEntry.getIndex() - 1);
        if (entry == null) {
            log.warn("get perLog is null , parameter logEntry : {}", logEntry);
            entry = LogEntry.builder().index(0L).term(0).command(null).build();
        }
        return entry;
    }

    class ReplicationFailQueueConsumer implements Runnable {

        /** 一分钟 */
        long intervalTime = 1000 * 60;

        @Override
        public void run() {
            while (running) {
                try {
                    ReplicationFailModel model = replicationFailQueue.poll(100, MILLISECONDS);
                    if (model == null) {
                        continue;
                    }
                    if (status != NodeStatus.LEADER) {
                        // 应该清空?
                        replicationFailQueue.clear();
                        continue;
                    }
                    log.warn("replication Fail Queue Consumer take a task, will be retry replication, content detail : [{}]", model.logEntry);
                    long offerTime = model.offerTime;
                    if (System.currentTimeMillis() - offerTime > intervalTime) {
                        log.warn("replication Fail event Queue maybe full or handler slow");
                    }
                    Callable<Boolean> callable = model.callable;
                    Future<Boolean> future = RaftThreadPool.submit(callable);
                    try {
                        Boolean r = future.get(3000, MILLISECONDS);
                        // 重试成功.
                        if (r) {
                            // 尝试推进 commitIndex
                            if (model.logEntry != null) {
                                tryAdvanceCommitIndex();
                            }
                        }
                    } catch (ExecutionException | TimeoutException e) {
                        log.warn(e.getMessage(), e);
                        if (status == NodeStatus.LEADER) {
                            replicationFailQueue.offer(model);
                        }
                    }
                } catch (InterruptedException e) {
                    // ignore
                }
            }
        }
    }

    @Override
    public void destroy() throws Throwable {
        rpcServer.destroy();
        stateMachine.destroy();
        rpcClient.destroy();
        running = false;
        log.info("destroy success");
    }

    /**
     * 1. 在转变成候选人后就立即开始选举过程
     * 自增当前的任期号（currentTerm）
     * 给自己投票
     * 重置选举超时计时器
     * 发送请求投票的 RPC 给其他所有服务器
     * 2. 如果接收到大多数服务器的选票，那么就变成领导人
     * 3. 如果接收到来自新的领导人的附加日志 RPC，转变成跟随者
     * 4. 如果选举过程超时，再次发起一轮选举
     */
    class ElectionTask implements Runnable {

        @Override
        public void run() {
            if (status == NodeStatus.LEADER) {
                return;
            }
            long current = System.currentTimeMillis();
            // 基于 RAFT 的随机时间,解决冲突.
            if (current - preElectionTime < electionTimeout) {
                return;
            }
            status = NodeStatus.CANDIDATE;
            log.error("node {} will become CANDIDATE and start election leader, current term : [{}], LastEntry : [{}]",
                    peerSet.getSelf(), currentTerm, logModule.getLast());
            // 重置选举超时计时器
            resetElectionTimeout();
            // 自增当前的任期号（currentTerm）
            synchronized(this) {
                currentTerm = currentTerm + 1;
            }
            // 给自己投票
            votedFor = peerSet.getSelf().getAddr();
            // 发送请求投票的 RPC 给其他所有服务器
            List<Peer> peers = peerSet.getPeersWithOutSelf();
            ArrayList<Future<RvoteResult>> futureArrayList = new ArrayList<>();
            log.info("peerList size : {}, peer list content : {}", peers.size(), peers);
            // 发送请求
            for (Peer peer : peers) {
                futureArrayList.add(RaftThreadPool.submit(() -> {
                    long lastTerm = 0L;
                    LogEntry last = logModule.getLast();
                    if (last != null) {
                        lastTerm = last.getTerm();
                    }
                    RvoteParam param = RvoteParam.builder().
                            term(currentTerm).
                            candidateId(peerSet.getSelf().getAddr()).
                            lastLogIndex(LongConvert.convert(logModule.getLastIndex())).
                            lastLogTerm(lastTerm).
                            build();
                    Request request = Request.builder()
                            .cmd(Request.R_VOTE)
                            .obj(param)
                            .url(peer.getAddr())
                            .build();
                    try {
                        return getRpcClient().<RvoteResult>send(request);
                    } catch (RaftRemotingException e) {
                        log.error("ElectionTask RPC Fail , URL : " + request.getUrl());
                        return null;
                    }
                }));
            }
            // 接收投票结果
            AtomicInteger success2 = new AtomicInteger(0);
            CountDownLatch latch = new CountDownLatch(futureArrayList.size());
            log.info("futureArrayList.size() : {}", futureArrayList.size());
            // 等待结果.
            for (Future<RvoteResult> future : futureArrayList) {
                RaftThreadPool.submit(() -> {
                    try {
                        RvoteResult result = future.get(3000, MILLISECONDS);
                        if (result == null) {
                            return -1;
                        }
                        boolean isVoteGranted = result.isVoteGranted();
                        if (isVoteGranted) {
                            success2.incrementAndGet();
                        } else {
                            // 更新自己的任期.
                            long resTerm = result.getTerm();
                            if (resTerm > currentTerm) {
                                currentTerm = resTerm;
                                status = NodeStatus.FOLLOWER;
                                votedFor = "";
                                return -1;
                            }
                        }
                        return 0;
                    } catch (Exception e) {
                        log.error("future.get exception , e : ", e);
                        return -1;
                    } finally {
                        latch.countDown();
                    }
                });
            }
            try {
                // 等待选举结果
                latch.await(3500, MILLISECONDS);
            } catch (InterruptedException e) {
                log.warn("InterruptedException By Master election Task");
            }
            int success = success2.get();
            log.info("node {} maybe become leader , success count = {} , status : {}", peerSet.getSelf(), success, NodeStatus.Enum.value(status));
            // 如果接收到来自新的领导人的附加日志RPC，转变成跟随者
            if (status == NodeStatus.FOLLOWER) {
                return;
            }
            // 如果接收到大多数服务器的选票，那么就变成领导人
            if (success + 1 >= (peers.size() + 1) / 2 + 1) {
                log.warn("node {} become leader ", peerSet.getSelf());
                status = NodeStatus.LEADER;
                peerSet.setLeader(peerSet.getSelf());
                votedFor = "";
                becomeLeaderToDoThing();
            } else {
                // 如果选举过程超时/失败，等待再次发起一轮选举
                votedFor = "";
            }
            // 再次更新选举时间
            resetElectionTimeout();
        }
    }

    /**
     * 初始化所有的 nextIndex 值为自己的最后一条日志的 index + 1. 如果下次 RPC 时, 跟随者和leader 不一致,就会失败.
     * 那么 leader 尝试递减 nextIndex 并进行重试.最终将达成一致.
     */
    private void becomeLeaderToDoThing() {
        // 初始化所有的nextIndex值（待追加索引）为自己的最后一条日志的index + 1。如果下次 RPC 时， 跟随者和leader 不一致，就会失败。
        // 初始化所有follower的matchIndex值（已经匹配成功的索引）为 0
        nextIndexs = new ConcurrentHashMap<>();
        matchIndexs = new ConcurrentHashMap<>();
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            nextIndexs.put(peer, logModule.getLastIndex() + 1);
            matchIndexs.put(peer, 0L);
        }
        // 创建[空日志]并提交，用于处理前任领导者未提交的日志
        LogEntry logEntry = LogEntry.builder()
                .command(null)
                .term(currentTerm)
                .build();
        // 预提交到本地日志, TODO 预提交
        logModule.write(logEntry);
        log.info("write logModule success, logEntry info : {}, log index : {}", logEntry, logEntry.getIndex());
        final AtomicInteger success = new AtomicInteger(0);
        List<Future<Boolean>> futureList = new ArrayList<>();
        //  复制到其他机器
        for (Peer peer : peerSet.getPeersWithOutSelf()) {
            // TODO check self and RaftThreadPool
            // 并行发起 RPC 复制.
            futureList.add(replication(peer, logEntry));
        }
        CountDownLatch latch = new CountDownLatch(futureList.size());
        List<Boolean> resultList = new CopyOnWriteArrayList<>();
        getRPCAppendResult(futureList, latch, resultList);
        try {
            latch.await(4000, MILLISECONDS);
        } catch (InterruptedException e) {
            log.error(e.getMessage(), e);
        }
        for (Boolean aBoolean : resultList) {
            if (aBoolean) {
                success.incrementAndGet();
            }
        }
        // 尝试推进 commitIndex
        tryAdvanceCommitIndex();
        //  只有日志被正式 commit 才允许 apply + 响应客户端
        if (logEntry.getIndex() <= commitIndex) {
            log.info("success apply local state machine,  logEntry info : {}", logEntry);
        } else {
            // TODO 空日志 commit 失败是否回滚退位？但是我后台又有重试机制不停重试，这样空日志总会 commit 的吧，不如不退位
            log.warn("fail apply local state  machine,  logEntry info : {}", logEntry);
//            无法提交空日志，回滚已经提交的日志，让出领导者位置
//            logModule.removeOnStartIndex(logEntry.getIndex());
//            log.warn("node {} becomeLeaderToDoThing fail ", peerSet.getSelf());
//            status = NodeStatus.FOLLOWER;
//            peerSet.setLeader(null);
//            votedFor = "";
        }
    }


    class HeartBeatTask implements Runnable {

        @Override
        public void run() {
            // 只有领导者才能发送心跳
            if (status != NodeStatus.LEADER) {
                return;
            }
            long current = System.currentTimeMillis();
            if (current - preHeartBeatTime < heartBeatTick) {
                return;
            }
            log.info("=========== NextIndex =============");
            for (Peer peer : peerSet.getPeersWithOutSelf()) {
                log.info("Peer {} nextIndex={}", peer.getAddr(), nextIndexs.get(peer));
            }
            preHeartBeatTime = System.currentTimeMillis();
            // 心跳只关心 term 和 leaderID
            for (Peer peer : peerSet.getPeersWithOutSelf()) {
                AentryParam param = AentryParam.builder()
                        .entries(null)// 心跳,空日志.
                        .leaderId(peerSet.getSelf().getAddr())
                        .serverId(peer.getAddr())
                        .term(currentTerm)
                        .leaderCommit(commitIndex) // 心跳时与跟随者同步 commit index
                        .build();
                Request request = new Request(
                        Request.A_ENTRIES,
                        param,
                        peer.getAddr());
                // 处理心跳反馈
                RaftThreadPool.execute(() -> {
                    try {
                        AentryResult aentryResult = getRpcClient().send(request);
                        long term = aentryResult.getTerm();
                        // 发现任期更高的结点，退位
                        if (term > currentTerm) {
                            log.error("self will become follower, he's term : {}, my term : {}", term, currentTerm);
                            currentTerm = term;
                            votedFor = "";
                            status = NodeStatus.FOLLOWER;
                        }
                    } catch (Exception e) {
                        log.error("HeartBeatTask RPC Fail, request URL : {} ", request.getUrl());
                    }
                }, false);
            }
        }
    }

    public void resetElectionTimeout() {
        preElectionTime = System.currentTimeMillis();
        electionTimeout = electionTime + ThreadLocalRandom.current().nextInt(0, 1000);
    }

    /**
     * 在复制成功后，Leader 尝试检查是否可以推进 commitIndex
     */
    private synchronized void tryAdvanceCommitIndex() {
        // 如果存在一个满足N > commitIndex的 N，并且大多数的matchIndex[i] ≥ N成立，
        // 并且log[N].term == currentTerm成立，那么令 commitIndex 等于这个 N （5.3 和 5.4 节）
        matchIndexs.put(peerSet.getSelf(), logModule.getLastIndex());
        List<Long> matchIndexList = new ArrayList<>(matchIndexs.values());
//        matchIndexList.add(logEntry.getIndex()); // Leader 自己
        Collections.sort(matchIndexList);
        int median = matchIndexList.size() / 2;
        Long N = matchIndexList.get(median);
        if (N > commitIndex) {
            LogEntry entry = logModule.read(N);
            // Leader 只能推进当前任期日志
            if (entry != null && entry.getTerm() == currentTerm) {
                commitIndex = N;
            }
            while (lastApplied < commitIndex) {
                lastApplied++;
                LogEntry entryToApply = logModule.read(lastApplied);
                if (entryToApply != null) {
                    stateMachine.apply(entryToApply);
                } else {
                    log.warn("log entry to apply is null at index: {}", lastApplied);
                }
            }
        }
    }

    @Override
    public Result addPeer(Peer newPeer) {
        return delegate.addPeer(newPeer);
    }

    @Override
    public Result removePeer(Peer oldPeer) {
        return delegate.removePeer(oldPeer);
    }

}
