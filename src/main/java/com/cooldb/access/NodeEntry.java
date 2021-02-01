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

package com.cooldb.access;

import com.cooldb.buffer.DBObject;

import java.nio.ByteBuffer;

/**
 * <code>NodeEntry</code> implements the GiST.Entry methods.
 */

abstract class NodeEntry implements GiST.Entry {
    final GiST.Predicate predicate;
    DBObject pointer;

    NodeEntry(NodeEntry entry) {
        predicate = (GiST.Predicate) entry.predicate.copy();
        pointer = entry.pointer.copy();
    }

    NodeEntry(GiST.Predicate predicate) {
        this.predicate = predicate;
    }

    public GiST.Predicate getPredicate() {
        return predicate;
    }

    public void setPredicate(GiST.Predicate predicate) {
        this.predicate.assign(predicate);
    }

    public DBObject getPointer() {
        return pointer;
    }

    public void setPointer(DBObject pointer) {
        this.pointer.assign(pointer);
    }

    // Comparable method
    @SuppressWarnings("unchecked")
    public int compareTo(Object obj) {
        return predicate.compareTo(obj);
    }

    // Object overrides
    @Override
    public boolean equals(Object obj) {
        return compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return predicate.hashCode();
    }

    // DBObject implementation
    public void writeTo(ByteBuffer bb) {
        predicate.writeTo(bb);
        pointer.writeTo(bb);
    }

    public void readFrom(ByteBuffer bb) {
        predicate.readFrom(bb);
        pointer.readFrom(bb);
    }

    public int storeSize() {
        return predicate.storeSize() + pointer.storeSize();
    }

    public void assign(DBObject o) {
        NodeEntry e = (NodeEntry) o;
        predicate.assign(e.predicate);
        pointer.assign(e.pointer);
    }

    public String toString() {
        return "Key->Ptr:" + predicate + "->" + pointer;
    }
}
