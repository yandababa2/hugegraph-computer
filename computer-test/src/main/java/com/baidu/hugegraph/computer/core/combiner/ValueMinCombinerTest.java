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

package com.baidu.hugegraph.computer.core.combiner;

import org.junit.Test;

import com.baidu.hugegraph.computer.core.graph.value.LongValue;
import com.baidu.hugegraph.testutil.Assert;

public class ValueMinCombinerTest {

    @Test
    public void testCombine() {
        LongValue min = new LongValue(0L);
        ValueMinCombiner<LongValue> combiner = new ValueMinCombiner<>();
        LongValue value1 = new LongValue(1L);
        min = combiner.combine(min, value1);
        LongValue value2 = new LongValue(2L);
        min = combiner.combine(value2, min);
        Assert.assertEquals(new LongValue(0L), min);
    }

    @Test
    public void testCombineNull() {
        LongValue value1 = new LongValue(1L);
        LongValue value2 = new LongValue(2L);
        ValueMinCombiner<LongValue> combiner = new ValueMinCombiner<>();
        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(null, value2);
        }, e -> {
            Assert.assertEquals("The combine parameter v1 can't be null",
                                e.getMessage());
        });

        Assert.assertThrows(IllegalArgumentException.class, () -> {
            combiner.combine(value1, null);
        }, e -> {
            Assert.assertEquals("The combine parameter v2 can't be null",
                                e.getMessage());
        });
    }
}
