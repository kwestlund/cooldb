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
 * Extent represents a region of disk space, specified as a starting file page
 * and a number of pages contiguous with the start page.
 */

public class Extent extends FilePage {
    private int size; // Number of pages in extent

    public Extent() {
        super();
    }

    public Extent(Extent extent) {
        super(extent);
        this.size = extent.size;
    }

    public Extent(FilePage start, int size) {
        super(start);
        this.size = size;
    }

    public Extent(short fileId, int pageId, int size) {
        super(fileId, pageId);
        this.size = size;
    }

    public static int sizeOf() {
        return FilePage.sizeOf() + 4;
    }

    public void assign(Extent extent) {
        super.assign(extent);
        size = extent.size;
    }

    @Override
    public DBObject copy() {
        return new Extent(this);
    }

    public void setStart(FilePage page) {
        super.assign(page);
    }

    public int getSize() {
        return size;
    }

    public void setSize(int size) {
        this.size = size;
    }

    public void extend(int size) {
        this.size += size;
    }

    public int getEndPageId() {
        return getPageId() + size;
    }

    // Object overrides
    @Override
    public String toString() {
        return "Extent(" + super.toString() + ", Size(" + size + "))";
    }

    // DBObject methods
    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        bb.putInt(size);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        size = bb.getInt();
    }

    @Override
    public int storeSize() {
        return FilePage.sizeOf() + 4;
    }
}
