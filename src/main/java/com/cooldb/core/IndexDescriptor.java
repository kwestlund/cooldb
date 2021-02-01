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

import com.cooldb.api.TypeException;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;

import java.nio.ByteBuffer;

public class IndexDescriptor implements DBObject {

    private final FilePage segmentId; // Segment identifier
    private final VarcharColumn keyMap; // User-assigned index key mappings
    private boolean isUnique; // Is the key-mapping enough to uniquely

    public IndexDescriptor() {
        this.segmentId = new FilePage();
        this.keyMap = new VarcharColumn();
    }

    public IndexDescriptor(IndexDescriptor indexDescriptor) {
        segmentId = new FilePage(indexDescriptor.segmentId);
        keyMap = new VarcharColumn(indexDescriptor.keyMap);
        isUnique = indexDescriptor.isUnique;
    }

    public void assign(DBObject o) {
        IndexDescriptor indexDescriptor = (IndexDescriptor) o;
        segmentId.assign(indexDescriptor.segmentId);
        keyMap.assign(indexDescriptor.keyMap);
        isUnique = indexDescriptor.isUnique;
    }

    public FilePage getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(FilePage segmentId) {
        this.segmentId.assign(segmentId);
    }

    public byte[] getKeyMap() {
        return keyMap.getBytes();
    }

    public void setKeyMap(byte[] keyMap) throws TypeException {
        this.keyMap.setBytes(keyMap);
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    // DBObject methods
    public DBObject copy() {
        return new IndexDescriptor(this);
    }

    public void writeTo(ByteBuffer bb) {
        segmentId.writeTo(bb);
        keyMap.writeTo(bb);
        bb.put(isUnique ? (byte) 1 : (byte) 0);
    }

    public void readFrom(ByteBuffer bb) {
        segmentId.readFrom(bb);
        keyMap.readFrom(bb);
        isUnique = bb.get() == (byte) 1;
    }

    public int storeSize() {
        return FilePage.sizeOf() + keyMap.storeSize() + 1;
    }

    public int compareTo(Object obj) {
        IndexDescriptor o = (IndexDescriptor) obj;
        int diff = segmentId.compareTo(o.segmentId);
        if (diff == 0)
            return keyMap.compareTo(o.keyMap);
        return diff;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof IndexDescriptor && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return segmentId.hashCode();
    }
    // identify each row?
}
