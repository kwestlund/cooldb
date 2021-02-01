/*
 * CoolDB: Embedded Database for Java
 *
 * Copyright 2021 Ken Westlund
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cooldb.log;

import java.nio.ByteBuffer;

public class LogData {

    private byte type = 0;
    private ByteBuffer data;
    private LogData next;
    private LogData prev;

    public LogData() {
    }

    public LogData(LogData logData) {
        type = logData.type;
        if (logData.data != null) {
            data = ByteBuffer.allocate(logData.data.capacity());
            data.put(logData.data.array());
        }
        if (next != null)
            add(logData.next.copy());
    }

    public LogData(byte type, int size) {
        this.type = type;
        if (size > 0)
            data = ByteBuffer.allocate(size);
    }

    // LogData
    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public ByteBuffer getData() {
        return data;
    }

    public void setData(ByteBuffer data) {
        this.data = data;
    }

    public LogData copy() {
        return new LogData(this);
    }

    // built-in list functions
    public void add(LogData logData) {
        if (next != null)
            next.add(logData);
        else {
            next = logData;
            logData.prev = this;
        }
    }

    public int size() {
        if (prev != null)
            return prev.size();
        else {
            int sz = 1;
            LogData ld = next;
            while (ld != null) {
                ++sz;
                ld = ld.next();
            }
            return sz;
        }
    }

    public LogData next() {
        return next;
    }

    public LogData prev() {
        return prev;
    }

    public LogData first() {
        if (prev != null)
            return prev.first();
        else
            return this;
    }

    public LogData last() {
        if (next != null)
            return next.last();
        else
            return this;
    }

    // Storable
    public void writeTo(ByteBuffer bb) {
        bb.put(type);
        if (data != null) {
            bb.putInt(data.capacity());
            bb.put(data.array());
        } else
            bb.putInt(0);
    }

    public void readFrom(ByteBuffer bb) {
        type = bb.get();
        int size = bb.getInt();
        if (size > 0) {
            data = ByteBuffer.allocate(size);
            bb.get(data.array());
        } else
            data = null;
    }

    public int storeSize() {
        return 5 + (data != null ? data.capacity() : 0);
    }
}
