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

package com.baidu.hugegraph.computer.core.io;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;

import com.baidu.hugegraph.computer.core.common.Constants;
import com.baidu.hugegraph.computer.core.graph.id.Id;
import com.baidu.hugegraph.computer.core.graph.value.Value;

public class StreamGraphOutput implements GraphOutput {

    private final DataOutputStream out;

    public StreamGraphOutput(OutputStream out) {
        this(new DataOutputStream(out));
    }

    public StreamGraphOutput(DataOutputStream out) {
        this.out = out;
    }

    @Override
    public void writeId(Id id) throws IOException {
        this.writeByte(id.type().code());
        id.write(this);
    }

    @Override
    public void writeValue(Value value) throws IOException {
        // TODO: doesn't need write type, fetch value type from config
        this.writeByte(value.type().code());
        value.write(this);
    }

    public void writeVInt(int value) throws IOException {
        // NOTE: negative numbers are not compressed
        if (value > 0x0fffffff || value < 0) {
            this.writeByte(0x80 | ((value >>> 28) & 0x7f));
        }
        if (value > 0x1fffff || value < 0) {
            this.writeByte(0x80 | ((value >>> 21) & 0x7f));
        }
        if (value > 0x3fff || value < 0) {
            this.writeByte(0x80 | ((value >>> 14) & 0x7f));
        }
        if (value > 0x7f || value < 0) {
            this.writeByte(0x80 | ((value >>> 7) & 0x7f));
        }
        this.writeByte(value & 0x7f);
    }

    public void writeVLong(long value) throws IOException {
        if (value < 0) {
            this.writeByte((byte) 0x81);
        }
        if (value > 0xffffffffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 56) & 0x7f));
        }
        if (value > 0x1ffffffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 49) & 0x7f));
        }
        if (value > 0x3ffffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 42) & 0x7f));
        }
        if (value > 0x7ffffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 35) & 0x7f));
        }
        if (value > 0xfffffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 28) & 0x7f));
        }
        if (value > 0x1fffffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 21) & 0x7f));
        }
        if (value > 0x3fffL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 14) & 0x7f));
        }
        if (value > 0x7fL || value < 0L) {
            this.writeByte(0x80 | ((int) (value >>> 7) & 0x7f));
        }
        this.write((int) value & 0x7f);
    }

    public void writeUInt8(int val) throws IOException {
        assert val <= Constants.UINT8_MAX;
        this.write(val);
    }

    public void writeUInt16(int val) throws IOException {
        assert val <= Constants.UINT16_MAX;
        this.writeShort(val);
    }

    public void writeUInt32(long val) throws IOException {
        assert val <= Constants.UINT32_MAX;
        this.writeInt((int) val);
    }

    @Override
    public void write(int b) throws IOException {
        this.out.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        this.out.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        this.out.write(b, off, len);
    }

    @Override
    public void writeBoolean(boolean v) throws IOException {
        this.out.writeBoolean(v);
    }

    @Override
    public void writeByte(int v) throws IOException {
        this.out.writeByte(v);
    }

    @Override
    public void writeShort(int v) throws IOException {
        this.out.writeShort(v);
    }

    @Override
    public void writeChar(int v) throws IOException {
        this.out.writeChar(v);
    }

    @Override
    public void writeInt(int v) throws IOException {
        this.out.writeInt(v);
    }

    @Override
    public void writeLong(long v) throws IOException {
        this.out.writeLong(v);
    }

    @Override
    public void writeFloat(float v) throws IOException {
        this.out.writeFloat(v);
    }

    @Override
    public void writeDouble(double v) throws IOException {
        this.out.writeDouble(v);
    }

    @Override
    public void writeBytes(String s) throws IOException {
        this.out.writeBytes(s);
    }

    @Override
    public void writeChars(String s) throws IOException {
        this.out.writeChars(s);
    }

    @Override
    public void writeUTF(String s) throws IOException {
        this.out.writeUTF(s);
    }
}