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

package com.baidu.hugegraph.computer.algorithm.community.cc;

import com.baidu.hugegraph.computer.algorithm.community.trianglecount.TriangleCountValue;
import com.baidu.hugegraph.computer.core.graph.vertex.Vertex;
import com.baidu.hugegraph.computer.core.output.hg.HugeOutput;
import com.baidu.hugegraph.structure.constant.WriteType;

/**
 * Offer 2 ways to output: writeback + hdfs-file(TODO)
 */
public class ClusteringCoefficientOutput extends HugeOutput {

    @Override
    public String name() {
        return "clustering_coefficient";
    }

    @Override
    public void prepareSchema() {
        this.client().schema().propertyKey(this.name())
                     .asFloat()
                     .writeType(WriteType.OLAP_RANGE)
                     .ifNotExist()
                     .create();
    }

    @Override
    public com.baidu.hugegraph.structure.graph.Vertex constructHugeVertex(
                                                      Vertex vertex) {
        com.baidu.hugegraph.structure.graph.Vertex hugeVertex =
                new com.baidu.hugegraph.structure.graph.Vertex(null);
        hugeVertex.id(vertex.id().asObject());
        float triangle = ((TriangleCountValue) vertex.value()).count();
        int degree = ((TriangleCountValue) vertex.value()).idList().size();
        hugeVertex.property(this.name(), 2 * triangle / degree / (degree - 1));
        return hugeVertex;
    }
}
