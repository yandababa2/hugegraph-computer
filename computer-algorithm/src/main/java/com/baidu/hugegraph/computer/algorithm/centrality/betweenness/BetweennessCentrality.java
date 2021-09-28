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

package com.baidu.hugegraph.computer.algorithm.centrality.betweenness;

import java.util.Iterator;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdSet;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;
import com.baidu.hugegraph.util.Log;

public class BetweennessCentrality implements Computation<BetweennessMessage> {

    private static final Logger LOG = Log.logger(BetweennessCentrality.class);

    public static final String OPTION_SAMPLE_RATE =
                               "betweenness_centrality.sample_rate";

    private double sampleRate;

    @Override
    public String name() {
        return "closeness_centrality";
    }

    @Override
    public String category() {
        return "centrality";
    }

    @Override
    public void init(Config config) {
        this.sampleRate = config.getDouble(OPTION_SAMPLE_RATE, 1.0D);
        if (this.sampleRate <= 0.0D || this.sampleRate > 1.0D) {
            throw new ComputerException("The param %s must be in (0.0, 1.0], " +
                                        "actual got '%s'",
                                        OPTION_SAMPLE_RATE, this.sampleRate);
        }
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        // First superstep is special, we just send vertex id to its neighbors
        BetweennessValue initialValue = new BetweennessValue(0.0D);
        initialValue.arrivedVertices().add(vertex.id());
        vertex.value(initialValue);
        if (vertex.numEdges() == 0) {
            return;
        }

        IdList sequence = new IdList();
        sequence.add(vertex.id());
        context.sendMessageToAllEdges(vertex, new BetweennessMessage(sequence));
        LOG.info("Finished compute-0 step");
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<BetweennessMessage> messages) {
//        Map<Id, Integer> pathsMap = new HashMap<>();
//        while (messages.hasNext()) {
//            BetweennessMessage message = messages.next();
//            Id sourceVertex = message.sequence().get(0);
//            pathsMap.put(sourceVertex,
//                         pathsMap.getOrDefault(sourceVertex, 0) + 1);
//        }
        boolean targetWorker = false;
        Thread currThread = Thread.currentThread();
        if (currThread.getName().endsWith("thread-1")) {
            targetWorker = true;
        }

        boolean active = false;
        BetweennessValue value = vertex.value();
        // Collect the vertices sent here this time
        IdSet arrivingVertices = new IdSet();
        // The betweenness value to be updated
        DoubleValue betweenness = value.betweenness();
        if (targetWorker) {
            System.out.println("curr vertex: " + vertex.id());
        }
        while (messages.hasNext()) {
            BetweennessMessage message = messages.next();
            // The value contributed to the intermediate node on the path
            DoubleValue vote = message.vote();
            if (vote.value() != 0.0D) {
                betweenness.value(betweenness.value() + vote.value());
            }

            IdList sequence = message.sequence();
            if (targetWorker) {
                System.out.println(sequence);
            }
            if (this.forwardSeq(context, vertex, sequence, arrivingVertices)) {
                active = true;
            }
        }
        value.arrivedVertices().addAll(arrivingVertices);
        if (!active) {
            vertex.inactivate();
        }
    }

    private boolean forwardSeq(ComputationContext context, Vertex vertex,
                               IdList sequence, IdSet arrivingVertices) {
        boolean active = false;
        if (sequence.size() == 0) {
            return active;
        }

        BetweennessValue value = vertex.value();
        IdSet arrivedVertices = value.arrivedVertices();
        Id sourceVertex = sequence.get(0);
        // The source vertex is arriving at first time
        if (!arrivedVertices.contains(sourceVertex)) {
            active = true;
            arrivingVertices.add(sourceVertex);

            // FIXME: This place has to be divided by a denominator.
            // How does the transformation point bring the denominator?
            BetweennessMessage voteMessage = new BetweennessMessage(
                                             new DoubleValue(1.0));
            for (int i = 1; i < sequence.size(); i++) {
                context.sendMessage(sequence.get(i), voteMessage);
            }
            Id selfId = vertex.id();
            sequence.add(selfId);

            BetweennessMessage newMessage = new BetweennessMessage(sequence);
            for (Edge edge : vertex.edges()) {
                Id targetId = edge.targetId();
                if (!sequence.contains(targetId) &&
                    this.sample(selfId, targetId, edge)) {
                    context.sendMessage(targetId, newMessage);
                }
            }
        }
        return active;
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
    }

    private boolean sample(Id sourceId, Id targetId, Edge edge) {
        // Now just use the simplest way
        return Math.random() <= this.sampleRate;
    }
}
