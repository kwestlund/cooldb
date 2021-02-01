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

package com.cooldb.log;

import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;

import java.nio.ByteBuffer;

/**
 * UndoPointer uniquely identifies a single undo log record.
 * <p>
 * It contains two pieces of information necessary to support extendable undo
 * space and fast random access to historical records, which is required to
 * support efficient mvcc, rollback, and recovery.
 * <p>
 * The first is the traditional sequential component necessary to determine each
 * record's chronological relationship with all other updates, and the second is
 * a physical location component necessary to efficiently store and retrieve the
 * record.
 * <p>
 * The sequential property aids in log space garbage collection while the
 * physical location property permits dynamic growth in the size of available
 * log record storage space.
 */

public class UndoPointer implements DBObject {
    private final FilePage page;
    private short offset;
    private long lsn;

    public UndoPointer() {
        this.page = new FilePage();
    }

    public UndoPointer(long lsn) {
        this.page = new FilePage();
        this.lsn = lsn;
    }

    public UndoPointer(FilePage page, short offset) {
        this.page = new FilePage(page);
        this.offset = offset;
    }

    public UndoPointer(FilePage page, short offset, long lsn) {
        this.page = new FilePage(page);
        this.offset = offset;
        this.lsn = lsn;
    }

    // Copy constructor
    public UndoPointer(UndoPointer undoPointer) {
        this.page = new FilePage(undoPointer.page);
        this.offset = undoPointer.offset;
        this.lsn = undoPointer.lsn;
    }

    public static int sizeOf() {
        return FilePage.sizeOf() + 10;
    }

    public void reset() {
        page.reset();
        offset = 0;
        lsn = 0;
    }

    public void assign(DBObject o) {
        UndoPointer undoPointer = (UndoPointer) o;
        this.page.assign(undoPointer.page);
        this.offset = undoPointer.offset;
        this.lsn = undoPointer.lsn;
    }

    // Object overrides
    @Override
    public boolean equals(Object obj) {
        return obj instanceof UndoPointer && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return (int) lsn;
    }

    @Override
    public String toString() {
        return page.toString() + "/Offset(" + offset + ")" + "/LSN(" + lsn
                + ")";
    }

    // Comparable method
    public DBObject copy() {
        return new UndoPointer(this);
    }

    public int compareTo(Object o) {
        UndoPointer other = (UndoPointer) o;
        return (int) (lsn - other.lsn);
    }

    // DBObject methods
    public void writeTo(ByteBuffer bb) {
        page.writeTo(bb);
        bb.putShort(offset);
        bb.putLong(lsn);
    }

    public void readFrom(ByteBuffer bb) {
        page.readFrom(bb);
        offset = bb.getShort();
        lsn = bb.getLong();
    }

    public int storeSize() {
        return page.storeSize() + 10;
    }

    // UndoPointer methods
    public boolean isNull() {
        return page.isNull();
    }

    public FilePage getPage() {
        return page;
    }

    public void setPage(FilePage page) {
        this.page.assign(page);
    }

    public short getOffset() {
        return offset;
    }

    public void setOffset(short offset) {
        this.offset = offset;
    }

    public long getLSN() {
        return lsn;
    }

    public void setLSN(long lsn) {
        this.lsn = lsn;
    }
}
