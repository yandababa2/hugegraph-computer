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

package com.baidu.hugegraph.computer.algorithm.centrality.closeness;

import java.util.Iterator;
import java.util.Map;

import org.slf4j.Logger;

import com.baidu.hugegraph.computer.core.common.exception.ComputerException;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.edge.Edge;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.value.MapValue;
import com.baidu.hugegraph.computer.core.graph.value.Value;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;
import com.baidu.hugegraph.util.Log;
import com.baidu.hugegraph.util.NumericUtil;

public class ClosenessCentrality
       implements Computation<MapValue<MapValue<DoubleValue>>> {

    private static final Logger LOG = Log.logger(ClosenessCentrality.class);

    public static final String OPTION_WEIGHT_PROPERTY =
                               "closeness_centrality.weight_property";
    public static final String OPTION_SAMPLE_RATE =
                               "closeness_centrality.sample_rate";

    private String weightProp;
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
        this.weightProp = config.getString(OPTION_WEIGHT_PROPERTY, "");
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
        // Set empty map as initial value
        vertex.value(new MapValue<>());
        // Send messages to adjacent edges
        for (Edge edge : vertex.edges()) {
            Id senderId = vertex.id();
            MapValue<DoubleValue> distance = new MapValue<>();
            // Get property value
            double value = this.weightValue(edge.property(this.weightProp));
            distance.put(senderId, new DoubleValue(value));
            MapValue<MapValue<DoubleValue>> message = new MapValue<>();
            message.put(senderId, distance);
            context.sendMessage(edge.targetId(), message);
        }
        LOG.info("Finished compute-0 step");
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<MapValue<MapValue<DoubleValue>>> messages) {
        // TODO: Merge the received messages first
        // MapValue<MapValue<IntValue>> message = Combiner.combineAll(
        //                                        context.combiner(), messages);
        Id selfId = vertex.id();
        // Save the distance from other vertices to self
        MapValue<DoubleValue> localValue = vertex.value();
        boolean active = false;
        while (messages.hasNext()) {
            active = true;
            /*
             * The Id in the outer layer represents who sent to me
             * The Id:IntValue in the inner layer represents the distance
             * from the other vertices to me.
             */
            MapValue<MapValue<DoubleValue>> message = messages.next();
            for (Map.Entry<Id, MapValue<DoubleValue>> entry :
                message.entrySet()) {
                Id senderId = entry.getKey();
                // In theory, it won't happen, defensive programming
                if (selfId.equals(senderId)) {
                    continue;
                }

                MapValue<DoubleValue> senderValue = entry.getValue();
                for (Map.Entry<Id, DoubleValue> subEntry :
                    senderValue.entrySet()) {
                    Id otherId = subEntry.getKey();
                    if (selfId.equals(otherId)) {
                        continue;
                    }
                    DoubleValue oldValue = localValue.get(otherId);
                    DoubleValue newValue = subEntry.getValue();
                    /*
                     * If the Id already exists and the new value is
                     * greater than or equal to the old value, skip
                     */
                    if (oldValue != null && newValue.compareTo(oldValue) >= 0) {
                        continue;
                    }
                    // Update locally saved values (take smaller value)
                    localValue.put(otherId, newValue);
                    // Send this smaller value to neighbors
                    // TODO: Maybe can combine some message before send
                    this.sendMessageToNeighbor(context, vertex, senderId,
                                               otherId, newValue);
                }
            }
        }
        if (!active) {
            vertex.inactivate();
        }
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
    }

    private void sendMessageToNeighbor(ComputationContext context,
                                       Vertex vertex, Id senderId, Id sourceId,
                                       DoubleValue newValue) {
        Id selfId = vertex.id();
        for (Edge edge : vertex.edges()) {
            Id targetId = edge.targetId();
            if (senderId.equals(targetId) || sourceId.equals(targetId)) {
                continue;
            }
            if (!sample(selfId, targetId, edge)) {
                continue;
            }
            // Update distance information
            double updatedValue = this.weightValue(newValue) +
                                  this.weightValue(edge.property(
                                                   this.weightProp));

            MapValue<MapValue<DoubleValue>> msg = new MapValue<>();
            MapValue<DoubleValue> distance = new MapValue<>();
            distance.put(sourceId, new DoubleValue(updatedValue));
            msg.put(selfId, distance);
            context.sendMessage(targetId, msg);
        }
    }

    private boolean sample(Id sourceId, Id targetId, Edge edge) {
        // Now just use the simplest way
        return Math.random() <= this.sampleRate;
    }

    private double weightValue(Value<?> rawValue) {
        if (rawValue == null) {
            return 1.0D;
        }
        switch (rawValue.valueType()) {
            case LONG:
            case INT:
            case DOUBLE:
            case FLOAT:
                return NumericUtil.convertToNumber(rawValue).doubleValue();
            default:
                throw new ComputerException("The weight property can only be " +
                                            "either Long or Int or Double or " +
                                            "Float, but got %s",
                                            rawValue.valueType());
        }
    }
}
