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

package com.baidu.hugegraph.computer.core.graph.value;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

import com.baidu.hugegraph.computer.core.common.ComputerContext;
import com.baidu.hugegraph.computer.core.graph.GraphFactory;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.io.RandomAccessInput;
import com.baidu.hugegraph.computer.core.io.RandomAccessOutput;

public class IdSet implements Value<IdSet> {

    private final GraphFactory graphFactory;
    private Set<Id> values;

    public IdSet() {
        this.graphFactory = ComputerContext.instance().graphFactory();
        this.values = this.graphFactory.createSet();
    }

    public void add(Id id) {
        this.values.add(id);
    }

    public void addAll(IdSet other) {
        this.values.addAll(other.values);
    }

    public boolean contains(Id id) {
        return this.values.contains(id);
    }

    public Set<Id> values() {
        return Collections.unmodifiableSet(this.values);
    }

    @Override
    public ValueType valueType() {
        return ValueType.ID_SET;
    }

    @Override
    public void assign(Value<IdSet> other) {
        this.checkAssign(other);
        this.values = ((IdSet) other).values();
    }

    @Override
    public Value<IdSet> copy() {
        IdSet values = new IdSet();
        for (Id value : this.values) {
            values.add((Id) value.copy());
        }
        return values;
    }

    @Override
    public Set<Id> value() {
        return this.values;
    }

    @Override
    public void read(RandomAccessInput in) throws IOException {
        int size = in.readInt();
        if (size > this.values.size() || size < this.values.size() / 2) {
            this.values = this.graphFactory.createSet(size);
        } else {
            this.values.clear();
        }

        for (int i = 0; i < size; i++) {
            Id id = this.graphFactory.createId();
            id.read(in);
            this.values.add(id);
        }
    }

    @Override
    public void write(RandomAccessOutput out) throws IOException {
        out.writeInt(this.values.size());
        for (Id id : this.values) {
            id.write(out);
        }
    }

    @Override
    public int compareTo(IdSet obj) {
        throw new UnsupportedOperationException();
    }
}
