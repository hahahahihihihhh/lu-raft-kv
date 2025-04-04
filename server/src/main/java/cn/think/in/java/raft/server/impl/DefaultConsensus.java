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

import cn.think.in.java.raft.server.Consensus;
import cn.think.in.java.raft.common.entity.LogEntry;
import cn.think.in.java.raft.common.entity.NodeStatus;
import cn.think.in.java.raft.common.entity.Peer;
import cn.think.in.java.raft.common.entity.AentryParam;
import cn.think.in.java.raft.common.entity.AentryResult;
import cn.think.in.java.raft.common.entity.RvoteParam;
import cn.think.in.java.raft.common.entity.RvoteResult;
import io.netty.util.internal.StringUtil;
import lombok.Getter;
import lombok.Setter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.locks.ReentrantLock;

/**
 *
 * 默认的一致性模块实现.
 *
 * @author 莫那·鲁道
 */
@Setter
@Getter
public class DefaultConsensus implements Consensus {


    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultConsensus.class);


    public final DefaultNode node;

    public final ReentrantLock voteLock = new ReentrantLock();
    public final ReentrantLock appendLock = new ReentrantLock();

    public DefaultConsensus(DefaultNode node) {
        this.node = node;
    }

    /**
     * 请求投票 RPC
     * 接收者实现：
     *      如果currentTerm > term，任期过旧，投票失败 （5.2 节）
     *      如果 currentTerm < term，清空投票记录，保证每个follower每个任期只会投一次票
     *      如果没有投过票或者已经投过票了，投的就是当前候选人，同时候选人的日志不比自己的旧，投票成功，计入这张票，重置选举时间（5.2 节，5.4 节）
     */
    @Override
    public RvoteResult requestVote(RvoteParam param) {
        try {
            RvoteResult.Builder builder = RvoteResult.newBuilder();
            // 已经有投票操作在执行了
            if (!voteLock.tryLock()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }
            // 新 term 清空 voteFor
            if (param.getTerm() > node.getCurrentTerm()) {
                node.setCurrentTerm(param.getTerm());
                node.setVotedFor("");
                node.status = NodeStatus.FOLLOWER;
            }
            // 对方任期没有自己新
            if (param.getTerm() < node.getCurrentTerm()) {
                return builder.term(node.getCurrentTerm()).voteGranted(false).build();
            }
            LOGGER.info("node {} current vote for [{}], param candidateId : {}", node.peerSet.getSelf(), node.getVotedFor(), param.getCandidateId());
            LOGGER.info("node {} current term {}, peer term : {}", node.peerSet.getSelf(), node.getCurrentTerm(), param.getTerm());
            // (当前节点并没有投票 或者 已经投票过了且是对方节点) && 对方日志和自己一样新
            if ((StringUtil.isNullOrEmpty(node.getVotedFor()) || node.getVotedFor().equals(param.getCandidateId()))) {
                if (node.getLogModule().getLast() != null) {
                    // 优先比较任期，对方没有自己新
                    if (node.getLogModule().getLast().getTerm() > param.getLastLogTerm()) {
                        return RvoteResult.fail();
                    }
                    // 其次比较索引，对方没有自己新
                    if (node.getLogModule().getLastIndex() > param.getLastLogIndex()) {
                        return RvoteResult.fail();
                    }
                }
                // 切换状态
                node.status = NodeStatus.FOLLOWER;
                // 更新
                node.peerSet.setLeader(new Peer(param.getCandidateId()));
                node.setCurrentTerm(param.getTerm());
                node.setVotedFor(param.getServerId());
                // 返回成功，投票成功重置选举时间
                node.resetElectionTimeout();
                return builder.term(node.getCurrentTerm()).voteGranted(true).build();
            }
            return builder.term(node.getCurrentTerm()).voteGranted(false).build();
        } finally {
            voteLock.unlock();
        }
    }

    /**
     * 附加日志(多个日志,为了提高效率) RPC
     * 接收者实现：
     *    【心跳/日志复制】如果 term < currentTerm 就返回 false，说明 leader 不够格，需要退位。 （5.1 节）
     *    【心跳/日志复制】如果日志在 prevLogIndex 位置处的任期号和 params.preLogTerm 不匹配，则返回 false，需要回溯 prevLogIndex。 （5.3 节）
     *    【日志复制】定位到 prevLogIndex 后，如果发现其后续的日志条目和 params 的日志条目产生冲突（索引值相同但是任期号不同），删除这一条及之后所有的日志条目 （5.3 节）
     *    【日志复制】附加所有新携带的日志条目 param.getEntries
     *    【心跳/日志复制】如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
     */
    @Override
    public AentryResult appendEntries(AentryParam param) {
        try {
            AentryResult.Builder builder = AentryResult.newBuilder();
            // 已经有日志复制操作在执行了
            if (!appendLock.tryLock()) {
                return builder.term(node.getCurrentTerm()).success(false).build();
            }
            // 说明leader不够格，需要退位。
            if (param.getTerm() < node.getCurrentTerm()) {
                return builder.term(node.getCurrentTerm()).success(false).build();
            }
            node.preHeartBeatTime = System.currentTimeMillis();
            // apendEntries RPC重置选举时间
            node.resetElectionTimeout();
            node.peerSet.setLeader(new Peer(param.getLeaderId()));
            // 说明leader够格
            if (param.getTerm() >= node.getCurrentTerm()) {
                LOGGER.debug("node {} become FOLLOWER, currentTerm : {}, param Term : {}, param serverId = {}",
                    node.peerSet.getSelf(), node.currentTerm, param.getTerm(), param.getServerId());
                // 认怂，使用对方的term
                node.status = NodeStatus.FOLLOWER;
                node.setCurrentTerm(param.getTerm());
            }
            // 心跳 RPC
            if (param.getEntries() == null || param.getEntries().length == 0) {
                LOGGER.info("node {} append heartbeat success , he's term : {}, my term : {}",
                    param.getLeaderId(), param.getTerm(), node.getCurrentTerm());
                // 处理 leader 已提交但未应用到状态机的日志
                applyCommittedLogs(param.getLeaderCommit());
                return builder.term(node.getCurrentTerm()).success(true).build();
            }
            LogEntry prevEntry = node.getLogModule().read(param.getPrevLogIndex());
            if (param.getPrevLogIndex() != 0 && (prevEntry == null || prevEntry.getTerm() != param.getPreLogTerm())) {
                return builder.term(node.getCurrentTerm()).success(false).build();
            }
            // 强制同步 leader 日志
            int i = 0;
            for (LogEntry incoming : param.getEntries()) {
                long targetIndex = param.getPrevLogIndex() + 1 + i;
                i++;
                LogEntry localLog = node.getLogModule().read(targetIndex);
                // 如果本地有日志，但 term 不同，说明冲突
                if (localLog != null && localLog.getTerm() != incoming.getTerm()) {
                    // 一旦发生冲突，后续都要删除，从 targetIndex 开始覆盖
                    node.getLogModule().removeOnStartIndex(targetIndex);
                    node.getLogModule().write(incoming);
                    continue;
                }
                // 说明这条日志已存在，不必写
                if (localLog != null) {
                    continue;
                }
                // 说明该索引处还没写过，直接写
                node.getLogModule().write(incoming);
            }
            applyCommittedLogs(param.getLeaderCommit());
            node.status = NodeStatus.FOLLOWER;
            // TODO, 是否应当在成功回复之后, 才正式提交? 防止 leader "等待回复"过程中 挂掉.
            return builder.term(node.getCurrentTerm()).success(true).build();
        } finally {
            appendLock.unlock();
        }
    }

    private void applyCommittedLogs(long leaderCommit) {
        // 下一个需要提交的日志的索引（如有）
        long nextCommit = node.getCommitIndex() + 1;
        //如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
        if (leaderCommit > node.getCommitIndex()) {
            int commitIndex = (int) Math.min(leaderCommit, node.getLogModule().getLastIndex());
            node.setCommitIndex(commitIndex);
            node.setLastApplied(commitIndex);
        }
        while (nextCommit <= node.getCommitIndex()){
            // 提交之前的日志
            node.stateMachine.apply(node.logModule.read(nextCommit));
            nextCommit++;
        }
    }

}
