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
package cn.think.in.java.raft.client;

import cn.think.in.java.raft.common.entity.LogEntry;
import lombok.extern.slf4j.Slf4j;

/**
 * @author 莫那·鲁道
 */
@Slf4j
public class RaftClient2 {
    // 1s 查询一次
    public static void main(String[] args) throws Throwable {

        RaftClientRPC rpc = new RaftClientRPC();

        for (int i = 3; ; i++) {
            try {
                String key = "hello:" + i;

                LogEntry logEntry = rpc.get(key);

                log.info("key={}, get response : {}", key, logEntry);
            } catch (Exception e) {
                // ignore
                e.printStackTrace();
            }
            Thread.sleep(1000);
        }
    }

}
