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
import com.cooldb.log.UndoPointer;
import com.cooldb.storage.RowHeader;
import com.cooldb.storage.Rowid;

import java.nio.ByteBuffer;

/**
 * <code>LeafEntry</code> extends NodeEntry with attributes and methods
 * specific to leaf nodes.
 */

public class LeafEntry extends NodeEntry {
    private UndoPointer undo;
    private byte rowHeaderFlags;
    private long lockHolder;

    LeafEntry(GiST.Predicate predicate) {
        super(predicate);
        pointer = new Rowid();
        undo = new UndoPointer();
    }

    LeafEntry(LeafEntry entry) {
        super(entry);
        undo = (UndoPointer) entry.undo.copy();
        rowHeaderFlags = entry.rowHeaderFlags;
        lockHolder = entry.lockHolder;
    }

    UndoPointer getUndo() {
        return undo;
    }

    void setUndo(UndoPointer undo) {
        this.undo = undo;
    }

    byte getRowHeaderFlags() {
        return rowHeaderFlags;
    }

    void setRowHeaderFlags(byte rowHeaderFlags) {
        this.rowHeaderFlags = rowHeaderFlags;
    }

    long getLockHolder() {
        return lockHolder;
    }

    void setLockHolder(long lockHolder) {
        this.lockHolder = lockHolder;
    }

    @Override
    public void writeTo(ByteBuffer bb) {
        bb.put(rowHeaderFlags);
        bb.putLong(lockHolder);
        super.writeTo(bb);
        undo.writeTo(bb);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        rowHeaderFlags = bb.get();
        lockHolder = bb.getLong();
        super.readFrom(bb);
        undo.readFrom(bb);
    }

    @Override
    public int storeSize() {
        return 9 + super.storeSize() + undo.storeSize();
    }

    @Override
    public void assign(DBObject o) {
        LeafEntry entry = (LeafEntry) o;
        predicate.assign(entry.getPredicate());
        pointer.assign(entry.getPointer());
        undo.assign(entry.undo);
        rowHeaderFlags = entry.rowHeaderFlags;
        lockHolder = entry.lockHolder;
    }

    public DBObject copy() {
        return new LeafEntry(this);
    }

    public String toString() {
        return "Flags:" + RowHeader.describeFlags(rowHeaderFlags) + " Lock:"
                + lockHolder + " " + super.toString() + " Undo:" + undo;
    }
}
