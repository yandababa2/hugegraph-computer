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

package com.baidu.hugegraph.computer.algorithm.rank.pagerank;

import java.util.Iterator;

import com.baidu.hugegraph.computer.core.aggregator.Aggregator;
import com.baidu.hugegraph.computer.core.combiner.Combiner;
import com.baidu.hugegraph.computer.core.config.Config;
import com.baidu.hugegraph.computer.core.graph.value.DoubleValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.worker.Computation;
import com.baidu.hugegraph.computer.core.worker.ComputationContext;
import com.baidu.hugegraph.computer.core.worker.WorkerContext;

public class PageRank implements Computation<DoubleValue> {

    public static final String CONF_ALPHA_KEY = "pagerank.alpha";

    public static final double CONF_ALPHA_DEFAULT = 0.15D;

    private double alpha;
    private double rankFromDangling;
    private double initialRankInSuperstep;
    private double cumulativeValue;

    private Aggregator l1DiffAggr;
    private Aggregator cumulativeRankAggr;
    private Aggregator danglingVertexNumAggr;
    private Aggregator danglingCumulativeAggr;

    // Initial value in superstep 0.
    private DoubleValue initialValue;
    private DoubleValue contribValue;

    @Override
    public String name() {
        return "pageRank";
    }

    @Override
    public String category() {
        return "rank";
    }

    @Override
    public void compute0(ComputationContext context, Vertex vertex) {
        vertex.value(this.initialValue);
        this.cumulativeRankAggr.aggregateValue(this.initialValue.value());
        int edgeCount = vertex.numEdges();
        if (edgeCount == 0) {
            this.danglingVertexNumAggr.aggregateValue(1L);
            this.danglingCumulativeAggr.aggregateValue(
                                        this.initialValue.value());
        } else {
            this.contribValue.value(this.initialValue.value() / edgeCount);
            context.sendMessageToAllEdges(vertex, this.contribValue);
        }
    }

    @Override
    public void compute(ComputationContext context, Vertex vertex,
                        Iterator<DoubleValue> messages) {
        DoubleValue message = Combiner.combineAll(context.combiner(), messages);
        double rankFromNeighbors = 0.0D;
        if (message != null) {
            rankFromNeighbors = message.value();
        }
        double rank = (this.rankFromDangling + rankFromNeighbors) *
                      (1.0D - this.alpha) + this.initialRankInSuperstep;
        rank /= this.cumulativeValue;
        DoubleValue oldRank = vertex.value();
        vertex.value(new DoubleValue(rank));
        this.l1DiffAggr.aggregateValue(Math.abs(oldRank.value() - rank));
        this.cumulativeRankAggr.aggregateValue(rank);
        int edgeCount = vertex.numEdges();
        if (edgeCount == 0) {
            this.danglingVertexNumAggr.aggregateValue(1L);
            this.danglingCumulativeAggr.aggregateValue(rank);
        } else {
            DoubleValue contribValue = new DoubleValue(rank / edgeCount);
            context.sendMessageToAllEdges(vertex, contribValue);
        }
    }

    @Override
    public void init(Config config) {
        this.alpha = config.getDouble(CONF_ALPHA_KEY, CONF_ALPHA_DEFAULT);
        this.contribValue = new DoubleValue();
    }

    @Override
    public void close(Config config) {
        // pass
    }

    @Override
    public void beforeSuperstep(WorkerContext context) {
        // Get aggregator values for computation
        DoubleValue danglingContribution = context.aggregatedValue(
                    PageRank4Master.AGGR_COMULATIVE_DANGLING_PROBABILITY);

        this.rankFromDangling = danglingContribution.value() /
                                context.totalVertexCount();
        this.initialRankInSuperstep = this.alpha / context.totalVertexCount();
        DoubleValue cumulativeProbability = context.aggregatedValue(
                    PageRank4Master.AGGR_COMULATIVE_PROBABILITY);
        this.cumulativeValue = cumulativeProbability.value();
        this.initialValue = new DoubleValue(1.0D / context.totalVertexCount());

        // Create aggregators
        this.l1DiffAggr = context.createAggregator(
                          PageRank4Master.AGGR_L1_NORM_DIFFERENCE_KEY);
        this.cumulativeRankAggr = context.createAggregator(
                                  PageRank4Master.AGGR_COMULATIVE_PROBABILITY);
        this.danglingVertexNumAggr = context.createAggregator(
             PageRank4Master.AGGR_DANGLING_VERTICES_NUM);
        this.danglingCumulativeAggr = context.createAggregator(
             PageRank4Master.AGGR_COMULATIVE_DANGLING_PROBABILITY);
    }

    @Override
    public void afterSuperstep(WorkerContext context) {
        context.aggregateValue(
                PageRank4Master.AGGR_COMULATIVE_PROBABILITY,
                this.cumulativeRankAggr.aggregatedValue());
        context.aggregateValue(
                PageRank4Master.AGGR_L1_NORM_DIFFERENCE_KEY,
                this.l1DiffAggr.aggregatedValue());
        context.aggregateValue(
                PageRank4Master.AGGR_DANGLING_VERTICES_NUM,
                this.danglingVertexNumAggr.aggregatedValue());
        context.aggregateValue(
                PageRank4Master.AGGR_COMULATIVE_DANGLING_PROBABILITY,
                this.danglingCumulativeAggr.aggregatedValue());
    }
}