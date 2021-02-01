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

public class Segment implements DBObject {

    private static final int OVERHEAD = FilePage.sizeOf() + Extent.sizeOf() + 21;
    private FilePage segmentId; // Segment identifier
    private byte segmentType; // Derived method type
    private Extent newExtent; // Extent from which new pages are allocated
    private int nextPage; // Next page to be allocated within the newExtent
    private int initialSize; // Size of initial extent
    private int nextSize; // Size of next extent to be allocated
    private float growthRate; // Rate of increase of subsequent extents
    private int pageCount; // Number of pages allocated

    public Segment() {
        this.segmentId = new FilePage();
        this.segmentType = (byte) 0;
        this.newExtent = new Extent();
        this.nextPage = 0;
        this.initialSize = 1;
        this.nextSize = 4;
        this.growthRate = (float) 1.5;
        this.pageCount = 0;
    }

    public Segment(FilePage segmentId) {
        this.segmentId = new FilePage(segmentId);
        this.segmentType = (byte) 0;
        this.newExtent = new Extent();
        this.nextPage = 0;
        this.initialSize = 1;
        this.nextSize = 4;
        this.growthRate = (float) 1.5;
        this.pageCount = 0;
    }

    public Segment(Segment segment) {
        assign(segment);
    }

    public Segment(FilePage segmentId, int initialSize, int nextSize,
                   float growthRate) {
        this.segmentId = new FilePage(segmentId);
        this.segmentType = (byte) 0;
        this.newExtent = new Extent();
        this.nextPage = 0;
        this.initialSize = initialSize;
        this.nextSize = nextSize;
        this.growthRate = growthRate;
        this.pageCount = 0;
    }

    public static int sizeOf() {
        return OVERHEAD;
    }

    public void assign(DBObject o) {
        Segment segment = (Segment) o;
        this.segmentId = new FilePage(segment.segmentId);
        this.segmentType = segment.segmentType;
        this.newExtent = new Extent(segment.newExtent);
        this.nextPage = segment.nextPage;
        this.initialSize = segment.initialSize;
        this.nextSize = segment.nextSize;
        this.growthRate = segment.growthRate;
        this.pageCount = segment.pageCount;
    }

    public DBObject copy() {
        return new Segment(this);
    }

    public FilePage getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(FilePage segmentId) {
        this.segmentId.assign(segmentId);
    }

    public byte getSegmentType() {
        return segmentType;
    }

    public void setSegmentType(byte segmentType) {
        this.segmentType = segmentType;
    }

    public Extent getNewExtent() {
        return newExtent;
    }

    public void setNewExtent(Extent newExtent) {
        this.newExtent.assign(newExtent);
    }

    public int getNextPage() {
        return nextPage;
    }

    public void setNextPage(int nextPage) {
        this.nextPage = nextPage;
    }

    public int getInitialSize() {
        return initialSize;
    }

    public void setInitialSize(int initialSize) {
        this.initialSize = initialSize;
    }

    public int getNextSize() {
        return nextSize;
    }

    public void setNextSize(int nextSize) {
        this.nextSize = nextSize;
    }

    public float getGrowthRate() {
        return growthRate;
    }

    public void setGrowthRate(float growthRate) {
        this.growthRate = growthRate;
    }

    public int getPageCount() {
        return pageCount;
    }

    public void setPageCount(int pageCount) {
        this.pageCount = pageCount;
    }

    // Object overrides
    @Override
    public boolean equals(Object obj) {
        return obj instanceof Segment && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return segmentId.hashCode();
    }

    @Override
    public String toString() {
        return "Segment(Id:" + segmentId + " Type:" + segmentType
                + " NextExtent:" + newExtent + " NextPageId:" + nextPage
                + " Sizing(" + initialSize + "," + nextSize + "," + growthRate
                + ") PageCount:" + pageCount + ")";
    }

    // Comparable method
    public int compareTo(Object o) {
        Segment other = (Segment) o;
        return segmentId.compareTo(other.segmentId);
    }

    // DBObject methods
    public void writeTo(ByteBuffer bb) {
        segmentId.writeTo(bb);
        bb.put(segmentType);
        newExtent.writeTo(bb);
        bb.putInt(nextPage);
        bb.putInt(initialSize);
        bb.putInt(nextSize);
        bb.putFloat(growthRate);
        bb.putInt(pageCount);
    }

    public void readFrom(ByteBuffer bb) {
        segmentId.readFrom(bb);
        segmentType = bb.get();
        newExtent.readFrom(bb);
        nextPage = bb.getInt();
        initialSize = bb.getInt();
        nextSize = bb.getInt();
        growthRate = bb.getFloat();
        pageCount = bb.getInt();
    }

    public int storeSize() {
        return OVERHEAD;
    }
}
