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

import com.cooldb.log.UndoPointer;

import java.util.BitSet;

/**
 * A transaction is known to be committed (with respect to this list) if it is
 * prior to any in the list or if it is in the list and its position is set on.
 * <p>
 * CommitLists are basically immutable. Only the MasterCommitList should be
 * changed.
 */

class CommitList {

    BitSet bits;
    int count;
    long baseTransId;

    // Protected members
    long commitTransId;
    final UndoPointer commitLSN;

    CommitList(long baseTransId, UndoPointer commitLSN, long commitTransId) {
        this.baseTransId = baseTransId;
        this.commitLSN = new UndoPointer(commitLSN);
        this.commitTransId = commitTransId;
        bits = new BitSet(TransactionPool.ALLOC_TRANSACTIONS);
    }

    // The commit-LSN must be an address such that all updates
    // prior to that address have been committed.

    CommitList(CommitList clist) {
        baseTransId = clist.baseTransId;
        commitLSN = new UndoPointer(clist.commitLSN);
        commitTransId = clist.commitTransId;
        count = clist.count;

        bits = (BitSet) clist.bits.clone();
    }

    // The commit-transId must be a transaction identifier such that all
    // transactions prior to it had committed prior to the start
    // of all transactions active at the time this one began.

    /**
     * Return true if the specified transaction was committed with respect to
     * this list and all transactions active in the list.
     */
    boolean isUniversallyCommitted(long transId) {
        return transId < commitTransId;
    }

    /**
     * Return true of the specified transaction is committed with respect to
     * this list.
     */
    boolean isCommitted(long transId) {
        if (transId < baseTransId)
            return true;

        int t = calcOffset(transId);
        if (t < count)
            return bits.get(t);

        return false;
    }

    long getBaseTransId() {
        return baseTransId;
    }

    UndoPointer getCommitLSN() {
        return commitLSN;
    }

    void setCommitLSN(UndoPointer commitLSN) {
        this.commitLSN.assign(commitLSN);
    }

    long getCommitTransId() {
        return commitTransId;
    }

    void setCommitTransId(long commitTransId) {
        this.commitTransId = commitTransId;
    }

    int getCount() {
        return count;
    }

    int calcOffset(long transId) {
        return (int) (transId - baseTransId);
    }

    long calcTransId(int offset) {
        return baseTransId + offset;
    }
}
