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

package com.cooldb.transaction;

import com.cooldb.buffer.DBObject;
import com.cooldb.log.UndoPointer;

import java.nio.ByteBuffer;

public class TransactionState implements DBObject {

    private static final int OVERHEAD = UndoPointer.sizeOf() * 2 + 17;
    // DBObject attributes
    private long transId;
    private final UndoPointer firstLSN = new UndoPointer();
    private final UndoPointer undoNxtLSN = new UndoPointer();
    private long rollbackCost;
    private boolean isCommitted;

    public TransactionState() {
        transId = -1;
    }

    public TransactionState(TransactionState state) {
        assign(state);
    }

    public void assign(DBObject o) {
        TransactionState state = (TransactionState) o;
        this.transId = state.transId;
        this.firstLSN.assign(state.firstLSN);
        this.undoNxtLSN.assign(state.undoNxtLSN);
        this.rollbackCost = state.rollbackCost;
        this.isCommitted = state.isCommitted;
    }

    // Unique transaction identifier
    public long getTransId() {
        return transId;
    }

    public void setTransId(long transId) {
        this.transId = transId;
    }

    // First undo log record written by this transaction
    public UndoPointer getFirstLSN() {
        return firstLSN;
    }

    public void setFirstLSN(UndoPointer firstLSN) {
        this.firstLSN.assign(firstLSN);
    }

    // Next log record to be processed during rollback
    public UndoPointer getUndoNxtLSN() {
        return undoNxtLSN;
    }

    public void setUndoNxtLSN(UndoPointer undoNxtLSN) {
        this.undoNxtLSN.assign(undoNxtLSN);
    }

    // Commit state
    public boolean isCommitted() {
        return isCommitted || transId == -1;
    }

    public void setCommitted(boolean isCommitted) {
        this.isCommitted = isCommitted;
    }

    // Cost of rollback
    public long getRollbackCost() {
        return rollbackCost;
    }

    public void addRollbackCost(int cost) {
        rollbackCost += cost;
    }

    // Object overrides
    @Override
    public boolean equals(Object obj) {
        return obj instanceof TransactionState && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return (int) transId;
    }

    // Comparable method
    public int compareTo(Object o) {
        TransactionState other = (TransactionState) o;
        return (int) (transId - other.transId);
    }

    // DBObject
    public DBObject copy() {
        return new TransactionState(this);
    }

    public void writeTo(ByteBuffer bb) {
        bb.putLong(transId);
        firstLSN.writeTo(bb);
        undoNxtLSN.writeTo(bb);
        bb.putLong(rollbackCost);
        bb.put(isCommitted ? (byte) 1 : (byte) 0);
    }

    public void readFrom(ByteBuffer bb) {
        transId = bb.getLong();
        firstLSN.readFrom(bb);
        undoNxtLSN.readFrom(bb);
        rollbackCost = bb.getLong();
        isCommitted = bb.get() == 1;
    }

    public int storeSize() {
        return OVERHEAD;
    }
}
