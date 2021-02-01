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

package com.cooldb.core;

import com.cooldb.api.Filter;
import com.cooldb.api.TypeException;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;

import java.nio.ByteBuffer;

public class SequenceDescriptor implements DBObject, Filter {

    private static final int DEFAULT_CACHE_SIZE = 100; // Volatile, cached
    private final VarcharColumn name; // User-assigned name of the sequence
    private final FilePage segmentId; // Segment identifier
    private long nextval; // Next number to-be-returned by the sequence
    private int cacheSize; // Size of volatile cache of numbers

    public SequenceDescriptor() {
        name = new VarcharColumn();
        segmentId = new FilePage();
        cacheSize = DEFAULT_CACHE_SIZE;
    }

    public SequenceDescriptor(SequenceDescriptor sequenceDescriptor) {
        name = new VarcharColumn(sequenceDescriptor.name);
        segmentId = new FilePage(sequenceDescriptor.segmentId);
        nextval = sequenceDescriptor.nextval;
        cacheSize = sequenceDescriptor.cacheSize;
    }

    public void assign(DBObject o) {
        SequenceDescriptor sequenceDescriptor = (SequenceDescriptor) o;
        name.assign(sequenceDescriptor.name);
        segmentId.assign(sequenceDescriptor.segmentId);
        nextval = sequenceDescriptor.nextval;
        cacheSize = sequenceDescriptor.cacheSize;
    }

    public String getName() {
        return name.getString();
    }

    public void setName(String name) throws TypeException {
        this.name.setString(name);
    }

    public FilePage getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(FilePage segmentId) {
        this.segmentId.assign(segmentId);
    }

    public long getNextval() {
        return nextval;
    }

    public void setNextval(long nextval) {
        this.nextval = nextval;
    }

    public int getCacheSize() {
        return cacheSize;
    }

    public void setCacheSize(int cacheSize) {
        this.cacheSize = cacheSize;
    }

    // DBObject methods
    public DBObject copy() {
        return new SequenceDescriptor(this);
    }

    public void writeTo(ByteBuffer bb) {
        name.writeTo(bb);
        segmentId.writeTo(bb);
        bb.putLong(nextval);
        bb.putShort((short) cacheSize);
    }

    public void readFrom(ByteBuffer bb) {
        name.readFrom(bb);
        segmentId.readFrom(bb);
        nextval = bb.getLong();
        cacheSize = bb.getShort();
    }

    public int storeSize() {
        return name.storeSize() + FilePage.sizeOf() + 10;
    }

    public int compareTo(Object obj) {
        SequenceDescriptor o = (SequenceDescriptor) obj;
        return name.compareTo(o.name);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SequenceDescriptor && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public boolean passes(Object obj) {
        return equals(obj);
    }
    // numbers
}
