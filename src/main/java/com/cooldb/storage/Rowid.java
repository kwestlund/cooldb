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
import com.cooldb.api.RID;
import com.cooldb.buffer.DBObject;

import java.nio.ByteBuffer;

/**
 * Rowid identifies a row within a page.
 */

public class Rowid implements RID, DBObject {
    private final FilePage page; // Page pointer
    private short index; // Index of row in page

    public Rowid() {
        this.page = new FilePage();
    }

    public Rowid(Rowid rowid) {
        this.page = new FilePage(rowid.page);
        this.index = rowid.index;
    }

    public Rowid(FilePage start, short index) {
        this.page = new FilePage(start);
        this.index = index;
    }

    public Rowid(short fileId, int pageId, short index) {
        this.page = new FilePage(fileId, pageId);
        this.index = index;
    }

    public static int sizeOf() {
        return FilePage.sizeOf() + 2;
    }

    public void assign(DBObject o) {
        Rowid rowid = (Rowid) o;
        page.assign(rowid.page);
        index = rowid.index;
    }

    public DBObject copy() {
        return new Rowid(this);
    }

    public FilePage getPage() {
        return page;
    }

    public void setPage(FilePage page) {
        this.page.assign(page);
    }

    public short getIndex() {
        return index;
    }

    public void setIndex(short index) {
        this.index = index;
    }

    // Comparable method
    public int compareTo(Object o) {
        Rowid rowid = (Rowid) o;
        int diff = page.compareTo(rowid.page);
        if (diff == 0)
            return index - rowid.index;
        return diff;
    }

    // Object overrides
    @Override
    public boolean equals(Object obj) {
        return obj instanceof Rowid && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return page.hashCode() * index;
    }

    @Override
    public String toString() {
        return "Rowid(" + page.toString() + ", Index(" + index + "))";
    }

    // DBObject methods
    public void writeTo(ByteBuffer bb) {
        page.writeTo(bb);
        bb.putShort(index);
    }

    public void readFrom(ByteBuffer bb) {
        page.readFrom(bb);
        index = bb.getShort();
    }

    public int storeSize() {
        return FilePage.sizeOf() + 2;
    }
}
