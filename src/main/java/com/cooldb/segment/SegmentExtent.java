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

/**
 * SegmentExtent is a type of Extent that has been allocated to a specific
 * Segment.
 */

public class SegmentExtent extends Extent {
    private final FilePage segmentId; // Segment using extent

    public SegmentExtent() {
        super();
        segmentId = new FilePage();
    }

    public SegmentExtent(SegmentExtent extent) {
        super(extent);
        segmentId = new FilePage(extent.segmentId);
    }

    public SegmentExtent(FilePage segmentId, Extent extent) {
        super(extent);
        this.segmentId = new FilePage(segmentId);
    }

    public static int sizeOf() {
        return Extent.sizeOf() + FilePage.sizeOf();
    }

    public void assign(SegmentExtent extent) {
        super.assign(extent);
        segmentId.assign(extent.segmentId);
    }

    @Override
    public DBObject copy() {
        return new SegmentExtent(this);
    }

    public FilePage getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(FilePage segmentId) {
        this.segmentId.assign(segmentId);
    }

    // Object overrides
    @Override
    public String toString() {
        return "SegmentExtent(" + super.toString() + ", Segment("
                + segmentId.toString() + "))";
    }

    // Comparable method
    @Override
    public int compareTo(Object o) {
        SegmentExtent other = (SegmentExtent) o;
        int r = segmentId.compareTo(other.segmentId);
        if (r == 0)
            return super.compareTo(other);
        return r;
    }

    // DBObject methods
    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        segmentId.writeTo(bb);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        segmentId.readFrom(bb);
    }

    @Override
    public int storeSize() {
        return Extent.sizeOf() + FilePage.sizeOf();
    }
}
