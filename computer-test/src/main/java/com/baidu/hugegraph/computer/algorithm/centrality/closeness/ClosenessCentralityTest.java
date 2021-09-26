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

public class ClosenessCentralityTest extends AlgorithmTestBase {

    private static final Map<String, Double> EXPECT_RESULTS =
                         ImmutableMap.<String, Double>builder()
                                     .put("A", 2.0595238095238098)
                                     .put("B", 2.4)
                                     .put("C", 2.75)
                                     .put("D", 2.533333333333333)
                                     .put("E", 1.65)
                                     .put("F", 2.15)
                                     .put("G", 2.0095238095238095)
                                     .build();

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey("rate").asInt().ifNotExist().create();

        schema.vertexLabel("user")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("link")
              .sourceLabel("user")
              .targetLabel("user")
              .properties("rate")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.label, "user", T.id, "A");
        Vertex vB = graph.addVertex(T.label, "user", T.id, "B");
        Vertex vC = graph.addVertex(T.label, "user", T.id, "C");
        Vertex vD = graph.addVertex(T.label, "user", T.id, "D");
        Vertex vE = graph.addVertex(T.label, "user", T.id, "E");
        Vertex vF = graph.addVertex(T.label, "user", T.id, "F");
        Vertex vG = graph.addVertex(T.label, "user", T.id, "G");

        vA.addEdge("link", vB, "rate", 1);
        vB.addEdge("link", vA, "rate", 1);

        vB.addEdge("link", vC, "rate", 2);
        vC.addEdge("link", vB, "rate", 2);

        vC.addEdge("link", vD, "rate", 1);
        vD.addEdge("link", vC, "rate", 1);

        vD.addEdge("link", vE, "rate", 2);
        vE.addEdge("link", vD, "rate", 2);

        vC.addEdge("link", vF, "rate", 3);
        vF.addEdge("link", vC, "rate", 3);

        vF.addEdge("link", vG, "rate", 1);
        vG.addEdge("link", vF, "rate", 1);

        vE.addEdge("link", vG, "rate", 4);
        vG.addEdge("link", vE, "rate", 4);
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void test() throws InterruptedException {
        runAlgorithm(ClosenessCentralityParams.class.getName(),
                     ClosenessCentrality.OPTION_WEIGHT_PROPERTY, "rate",
                     ClosenessCentrality.OPTION_SAMPLE_RATE, "1.0D",
                     ComputerOptions.BSP_MAX_SUPER_STEP.name(), "5",
                     ComputerOptions.OUTPUT_CLASS.name(),
                     ClosenessCentralityTestOutput.class.getName());
    }

    public static class ClosenessCentralityTestOutput
                  extends ClosenessCentralityOutput {

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
