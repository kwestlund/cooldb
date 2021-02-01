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

package com.cooldb.buffer;

import java.nio.ByteBuffer;

public class FilePage implements DBObject {

    public final static FilePage NULL = new FilePage();
    private short fileId;
    private int pageId;

    public FilePage() {
        reset();
    }

    public FilePage(short fileId, int pageId) {
        this.fileId = fileId;
        this.pageId = pageId;
    }

    // Copy constructor
    public FilePage(FilePage page) {
        this.fileId = page.fileId;
        this.pageId = page.pageId;
    }

    public static int sizeOf() {
        return 6;
    }

    public void reset() {
        fileId = -1;
        pageId = -1;
    }

    public void assign(DBObject o) {
        FilePage page = (FilePage) o;
        this.fileId = page.fileId;
        this.pageId = page.pageId;
    }

    // Object overrides
    @Override
    public boolean equals(Object obj) {
        return obj instanceof FilePage && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return pageId;
    }

    @Override
    public String toString() {
        return "Page(" + fileId + "," + pageId + ")";
    }

    // Comparable method
    @Override
    public int compareTo(Object o) {
        FilePage other = (FilePage) o;
        return fileId == other.fileId ? pageId - other.pageId : fileId
                - other.fileId;
    }

    // DBObject methods
    @Override
    public DBObject copy() {
        return new FilePage(this);
    }

    @Override
    public void writeTo(ByteBuffer bb) {
        bb.putShort(fileId);
        bb.putInt(pageId);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        fileId = bb.getShort();
        pageId = bb.getInt();
    }

    @Override
    public int storeSize() {
        return 6;
    }

    public short getFileId() {
        return fileId;
    }

    // FilePage methods
    public void setFileId(short fileId) {
        this.fileId = fileId;
    }

    public int getPageId() {
        return pageId;
    }

    public void setPageId(int pageId) {
        this.pageId = pageId;
    }

    public boolean isNull() {
        return fileId == -1;
    }

    public void setNull() {
        fileId = -1;
    }
}
