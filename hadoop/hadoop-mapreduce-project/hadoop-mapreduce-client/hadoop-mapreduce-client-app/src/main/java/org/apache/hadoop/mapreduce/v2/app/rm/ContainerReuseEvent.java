/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/


package org.apache.hadoop.mapreduce.v2.app.rm;

import org.apache.hadoop.mapreduce.v2.api.records.TaskAttemptId;
import org.apache.hadoop.mapreduce.v2.app.job.TaskAttempt;
import org.apache.hadoop.mapreduce.v2.app.rm.ContainerAllocator.EventType;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.Token;

public class ContainerReuseEvent extends ContainerAllocatorEvent {

    private final TaskAttempt previousTaskAttemptReference;
    private final ContainerId containerID;
    private final String containerMgrAddress;
    private final Token containerToken;

    public ContainerReuseEvent(TaskAttemptId attemptID, ContainerId containerID,
        String containerMgrAddress, Token containerToken,
        EventType type, TaskAttempt previousTaskAttemptReference) {
        super(attemptID, type);
        this.containerID = containerID;
        this.containerMgrAddress = containerMgrAddress;
        this.containerToken = containerToken;
        this.previousTaskAttemptReference = previousTaskAttemptReference;
    }

    public TaskAttempt getPreviousTaskAttemptReference() {
        return previousTaskAttemptReference;
    }

    public ContainerId getContainerID() {
        return containerID;
    }

    public String getContainerMgrAddress() {
        return containerMgrAddress;
    }

    public Token getContainerToken() {
        return containerToken;
    }

}