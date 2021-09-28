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

import java.util.Map;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;

public class BetweennessCentralityTest extends AlgorithmTestBase {

    private static final Map<String, Double> EXPECT_RESULTS =
                         ImmutableMap.<String, Double>builder()
                                     .put("A", 0D)
                                     .put("B", 10.0D)
                                     .put("C", 4.0D)
                                     .put("D", 10.0D)
                                     .put("E", 2.0D)
                                     .put("F", 0.0D)
                                     .build();

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.vertexLabel("user")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("link")
              .sourceLabel("user")
              .targetLabel("user")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.label, "user", T.id, "A");
        Vertex vB = graph.addVertex(T.label, "user", T.id, "B");
        Vertex vC = graph.addVertex(T.label, "user", T.id, "C");
        Vertex vD = graph.addVertex(T.label, "user", T.id, "D");
        Vertex vE = graph.addVertex(T.label, "user", T.id, "E");
        Vertex vF = graph.addVertex(T.label, "user", T.id, "F");

        vA.addEdge("link", vB);
        vB.addEdge("link", vA);

        vB.addEdge("link", vC);
        vC.addEdge("link", vB);

        vB.addEdge("link", vD);
        vD.addEdge("link", vB);

        vC.addEdge("link", vD);
        vD.addEdge("link", vC);

        vC.addEdge("link", vE);
        vE.addEdge("link", vC);

        vD.addEdge("link", vE);
        vE.addEdge("link", vD);

        vD.addEdge("link", vF);
        vF.addEdge("link", vD);

        vE.addEdge("link", vF);
        vF.addEdge("link", vE);
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void test() throws InterruptedException {
        runAlgorithm(BetweennessCentralityParams.class.getName(),
                     BetweennessCentrality.OPTION_SAMPLE_RATE, "1.0D",
                     ComputerOptions.BSP_MAX_SUPER_STEP.name(), "5",
                     ComputerOptions.OUTPUT_CLASS.name(),
                     BetweennessCentralityTestOutput.class.getName());
    }

    public static class BetweennessCentralityTestOutput
                  extends BetweennessCentralityOutput {

        public Vertex constructHugeVertex(
                com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            Vertex result = super.constructHugeVertex(vertex);
            Double expect = EXPECT_RESULTS.get(result.id());
            Assert.assertNotNull(expect);
            Assert.assertEquals(expect, result.property(super.name()));
            return result;
        }
    }
}
