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

package com.cooldb.segment;

import com.cooldb.buffer.FilePage;
import com.cooldb.buffer.DBObject;

import java.nio.ByteBuffer;

public abstract class SegmentDescriptor implements DBObject {

    private static final int OVERHEAD = FilePage.sizeOf() * 2;
    private final FilePage segmentId; // Segment identifier and first page with data
    private final FilePage freePage; // Start of freed pages list

    public SegmentDescriptor() {
        this.segmentId = new FilePage();
        this.freePage = new FilePage();
    }

    public SegmentDescriptor(FilePage segmentId) {
        this.segmentId = new FilePage(segmentId);
        this.freePage = new FilePage();
    }

    public SegmentDescriptor(SegmentDescriptor datasetDescriptor) {
        this.segmentId = new FilePage(datasetDescriptor.segmentId);
        this.freePage = new FilePage(datasetDescriptor.freePage);
    }

    public static int sizeOf() {
        return OVERHEAD;
    }

    public void assign(DBObject o) {
        SegmentDescriptor segmentDescriptor = (SegmentDescriptor) o;
        this.segmentId.assign(segmentDescriptor.segmentId);
        this.freePage.assign(segmentDescriptor.freePage);
    }

    public FilePage getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(FilePage segmentId) {
        this.segmentId.assign(segmentId);
    }

    public FilePage getFreePage() {
        return freePage;
    }

    public void setFreePage(FilePage freePage) {
        this.freePage.assign(freePage);
    }

    // Object overrides
    @Override
    public boolean equals(Object obj) {
        return obj instanceof SegmentDescriptor && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return segmentId.hashCode();
    }

    @Override
    public String toString() {
        return "SegmentId(" + segmentId + ")";
    }

    // Comparable method
    public int compareTo(Object o) {
        SegmentDescriptor other = (SegmentDescriptor) o;
        return segmentId.compareTo(other.segmentId);
    }

    // DBObject methods
    public void writeTo(ByteBuffer bb) {
        segmentId.writeTo(bb);
        freePage.writeTo(bb);
    }

    public void readFrom(ByteBuffer bb) {
        segmentId.readFrom(bb);
        freePage.readFrom(bb);
    }

    public int storeSize() {
        return OVERHEAD;
    }
}
