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

package com.cooldb.storage;

import com.cooldb.buffer.FilePage;
import com.cooldb.buffer.DBObject;
import com.cooldb.segment.SegmentDescriptor;

import java.nio.ByteBuffer;

public class DatasetDescriptor extends SegmentDescriptor {

    public static final byte DEFAULT_LOAD_MIN = 40;
    public static final byte DEFAULT_LOAD_MAX = 80;
    private static final int OVERHEAD = FilePage.sizeOf() * 2 + 2;
    private final FilePage lastPage; // Last page with data
    private final FilePage surplusSegmentId; // Surplus segment identifier
    private byte loadMin; // Maintain row page load at at least loadMin
    private byte loadMax; // Maintain row page load at at most loadMax

    public DatasetDescriptor() {
        super();
        this.lastPage = new FilePage();
        this.surplusSegmentId = new FilePage();
        this.loadMin = DEFAULT_LOAD_MIN;
        this.loadMax = DEFAULT_LOAD_MAX;
    }

    public DatasetDescriptor(FilePage segmentId) {
        super(segmentId);
        this.lastPage = new FilePage();
        this.surplusSegmentId = new FilePage();
        this.loadMin = DEFAULT_LOAD_MIN;
        this.loadMax = DEFAULT_LOAD_MAX;
    }

    public DatasetDescriptor(DatasetDescriptor datasetDescriptor) {
        super(datasetDescriptor);
        this.lastPage = new FilePage(datasetDescriptor.lastPage);
        this.surplusSegmentId = new FilePage(datasetDescriptor.surplusSegmentId);
        this.loadMin = DEFAULT_LOAD_MIN;
        this.loadMax = DEFAULT_LOAD_MAX;
    }

    public static int sizeOf() {
        return SegmentDescriptor.sizeOf() + OVERHEAD;
    }

    @Override
    public void assign(DBObject o) {
        super.assign(o);
        DatasetDescriptor datasetDescriptor = (DatasetDescriptor) o;
        this.lastPage.assign(datasetDescriptor.lastPage);
        this.surplusSegmentId.assign(datasetDescriptor.surplusSegmentId);
        this.loadMin = datasetDescriptor.loadMin;
        this.loadMax = datasetDescriptor.loadMax;
    }

    public DBObject copy() {
        return new DatasetDescriptor(this);
    }

    public FilePage getLastPage() {
        return lastPage;
    }

    public void setLastPage(FilePage lastPage) {
        this.lastPage.assign(lastPage);
    }

    public FilePage getSurplusSegmentId() {
        return surplusSegmentId;
    }

    public void setSurplusSegmentId(FilePage surplusSegmentId) {
        this.surplusSegmentId.assign(surplusSegmentId);
    }

    public byte getLoadMin() {
        return loadMin;
    }

    public void setLoadMin(byte loadMin) {
        this.loadMin = loadMin;
    }

    public byte getLoadMax() {
        return loadMax;
    }

    public void setLoadMax(byte loadMax) {
        this.loadMax = loadMax;
    }

    // DBObject methods
    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        lastPage.writeTo(bb);
        surplusSegmentId.writeTo(bb);
        bb.put(loadMin);
        bb.put(loadMax);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        lastPage.readFrom(bb);
        surplusSegmentId.readFrom(bb);
        loadMin = bb.get();
        loadMax = bb.get();
    }

    @Override
    public int storeSize() {
        return super.storeSize() + OVERHEAD;
    }
}
