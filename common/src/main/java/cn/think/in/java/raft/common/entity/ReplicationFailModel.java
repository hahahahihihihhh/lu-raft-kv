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
package cn.think.in.java.raft.common.entity;

import lombok.Builder;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/**
 *
 * @author 莫那·鲁道
 */
@Builder
public class ReplicationFailModel {
    static String count = "_count";
    static String success = "_success";

    public String countKey;
    public String successKey;
    public Callable<Boolean> callable;
    public LogEntry logEntry;
    public Peer peer;
    public Long offerTime;
}
