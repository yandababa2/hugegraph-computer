/*
 * Copyright 2017 HugeGraph Authors
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package com.baidu.hugegraph.computer.core.network;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;

import com.baidu.hugegraph.computer.core.common.exception.TransportException;
import com.baidu.hugegraph.computer.core.network.connection.ConnectionManager;
import com.baidu.hugegraph.computer.core.network.message.MessageType;

/**
 * This is used for worker to send buffer to other worker. The whole process
 * contains several iteration. In one iteration {@link #startSession} is
 * called only once. {@link #send} is called zero or more times.
 * {@link #finishSession()} is called only once.
 */
public interface TransportClient {

    /**
     * This method is called before an iteration of sending buffers.
     */
    void startSession() throws TransportException;

    /**
     * Send the buffer to the server.
     * Return false if unable send data immediately.
     * This method is called zero or many times in iteration.
     * @throws TransportException if failed, the job will fail.
     */
    boolean send(MessageType messageType, int partition, ByteBuffer buffer)
                 throws TransportException;

    /**
     * This method is called after an iteration. It will block the caller to
     * make sure the buffers sent be received by target workers.
     */
    void finishSession() throws TransportException;

    /**
     * Get the {@link ConnectionId}
     */
    ConnectionId connectionId();

    /**
     * Get the remote SocketAddress
     */
    InetSocketAddress remoteAddress();

    /**
     * To check whether the connection is active to use
     * @return true if connection is active
     */
    boolean active();

    /**
     * Close the client.
     * NOTE: If the client is created with {@link ConnectionManager}, need to
     * use {@link ConnectionManager#closeClient(ConnectionId)} to close it,
     * otherwise there will be unsafe risks.
     */
    void close();
}