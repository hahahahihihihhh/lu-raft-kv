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
package cn.think.in.java.raft.server;


import cn.think.in.java.raft.common.entity.AentryParam;
import cn.think.in.java.raft.common.entity.AentryResult;
import cn.think.in.java.raft.common.entity.RvoteParam;
import cn.think.in.java.raft.common.entity.RvoteResult;

/**
 *
 * Raft 一致性模块.
 * @author 莫那·鲁道
 */
public interface Consensus {

    /**
     * 请求投票 RPC
     * 接收者实现：
     *      如果currentTerm > term，任期过旧，投票失败 （5.2 节）
     *      如果 currentTerm < term，清空投票记录，保证每个follower每个任期只会投一次票
     *      如果没有投过票或者已经投过票了，投的就是当前候选人，同时候选人的日志不比自己的旧，投票成功，计入这张票，重置选举时间（5.2 节，5.4 节）
     */
    RvoteResult requestVote(RvoteParam param);

    /**
     * 附加日志(多个日志,为了提高效率) RPC
     * 接收者实现：
     *    【心跳/日志复制】如果 term < currentTerm 就返回 false，说明 leader 不够格，需要退位。 （5.1 节）
     *    【心跳/日志复制】如果日志在 prevLogIndex 位置处的任期号和 params.preLogTerm 不匹配，则返回 false，需要回溯 prevLogIndex。 （5.3 节）
     *    【日志复制】定位到 prevLogIndex 后，如果发现其后续的日志条目和 params 的日志条目产生冲突（索引值相同但是任期号不同），删除这一条及之后所有的日志条目 （5.3 节）
     *    【日志复制】附加所有新携带的日志条目 param.getEntries
     *    【心跳/日志复制】如果 leaderCommit > commitIndex，令 commitIndex 等于 leaderCommit 和 新日志条目索引值中较小的一个
     */
    AentryResult appendEntries(AentryParam param);


}
