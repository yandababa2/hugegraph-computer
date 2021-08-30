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

package com.baidu.hugegraph.computer.algorithm.rings;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.baidu.hugegraph.computer.algorithm.AlgorithmTestBase;
import com.baidu.hugegraph.computer.algorithm.path.rings.RingsDetectionParams;
import com.baidu.hugegraph.computer.core.config.ComputerOptions;
import com.baidu.hugegraph.computer.core.graph.value.IdList;
import com.baidu.hugegraph.computer.core.graph.value.IdListList;
import com.baidu.hugegraph.computer.core.output.LimitedLogOutput;
import com.baidu.hugegraph.driver.GraphManager;
import com.baidu.hugegraph.driver.HugeClient;
import com.baidu.hugegraph.driver.SchemaManager;
import com.baidu.hugegraph.structure.constant.T;
import com.baidu.hugegraph.structure.graph.Vertex;
import com.baidu.hugegraph.testutil.Assert;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

public class RingsDetectionTest extends AlgorithmTestBase {

    private static final Map<String, Set<String>> EXPECT_RINGS =
            ImmutableMap.of(
                    "A", ImmutableSet.of("ACA", "ADCA", "ABCA"),
                    "B", ImmutableSet.of("BCB"),
                    "C", ImmutableSet.of("CDC")
            );

    @BeforeClass
    public static void setup() {
        clearAll();

        HugeClient client = client();
        SchemaManager schema = client.schema();

        schema.propertyKey("weight")
              .asInt()
              .ifNotExist()
              .create();
        schema.vertexLabel("user")
              .properties("weight")
              .useCustomizeStringId()
              .ifNotExist()
              .create();
        schema.edgeLabel("know")
              .sourceLabel("user")
              .targetLabel("user")
              .properties("weight")
              .ifNotExist()
              .create();

        GraphManager graph = client.graph();
        Vertex vA = graph.addVertex(T.label, "user", T.id, "A", "weight", 1);
        Vertex vB = graph.addVertex(T.label, "user", T.id, "B", "weight", 1);
        Vertex vC = graph.addVertex(T.label, "user", T.id, "C", "weight", 1);
        Vertex vD = graph.addVertex(T.label, "user", T.id, "D", "weight", 1);
        Vertex vE = graph.addVertex(T.label, "user", T.id, "E", "weight", 3);

        vA.addEdge("know", vB, "weight", 1);
        vA.addEdge("know", vC, "weight", 1);
        vA.addEdge("know", vD, "weight", 1);
        vB.addEdge("know", vA, "weight", 2);
        vB.addEdge("know", vC, "weight", 1);
        vB.addEdge("know", vE, "weight", 1);
        vC.addEdge("know", vA, "weight", 1);
        vC.addEdge("know", vB, "weight", 1);
        vC.addEdge("know", vD, "weight", 1);
        vD.addEdge("know", vC, "weight", 1);
        vD.addEdge("know", vE, "weight", 1);
        vE.addEdge("know", vC, "weight", 1);
    }

    @AfterClass
    public static void clear() {
        clearAll();
    }

    @Test
    public void test() throws InterruptedException {
        runAlgorithm(RingsDetectionsTestParams.class.getName());
    }

    public static class RingsDetectionsTestParams extends RingsDetectionParams {

        @Override
        public void setAlgorithmParameters(Map<String, String> params) {
            this.setIfAbsent(params, ComputerOptions.OUTPUT_CLASS,
                             RingsDetectionsTestOutput.class.getName());
            String filter = "{" +
                            "    \"vertex_filter\": [" +
                            "        {" +
                            "            \"label\": \"user\"," +
                            "            \"property_filter\": \"$element" +
                            ".weight==1\"" +
                            "        }" +
                            "    ]," +
                            "    \"edge_filter\": [" +
                            "        {" +
                            "            \"label\": \"know\"," +
                            "            \"property_filter\": \"$message" +
                            ".weight==$element.weight\"" +
                            "        }" +
                            "    ]" +
                            "}";
            this.setIfAbsent(params, ComputerOptions.RINGS_DETECTION_FILTER,
                             filter);
            super.setAlgorithmParameters(params);
        }
    }

    public static class RingsDetectionsTestOutput extends LimitedLogOutput {

        @Override
        public void write(
               com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            super.write(vertex);
            this.assertResult(vertex);
        }

        private void assertResult(
                com.baidu.hugegraph.computer.core.graph.vertex.Vertex vertex) {
            IdListList rings = vertex.value();
            Set<String> expect =
                        EXPECT_RINGS.getOrDefault(vertex.id().toString(),
                                                  new HashSet<>());

            Assert.assertEquals(expect.size(), rings.size());
            for (int i = 0; i < rings.size(); i++) {
                IdList ring = rings.get(0);
                StringBuilder ringValue = new StringBuilder();
                for (int j = 0; j < ring.size(); j++) {
                    ringValue.append(ring.get(j).toString());
                }
                Assert.assertTrue(expect.contains(ringValue.toString()));
            }
        }
    }
}
