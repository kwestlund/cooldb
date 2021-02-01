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

/**
 * This CommitList is the master commit list; all other CommitLists are created
 * as copies (snapshots in time) of this one.
 */

class MasterCommitList extends CommitList {

    MasterCommitList(long baseTransId, UndoPointer commitLSN) {
        super(baseTransId, commitLSN, baseTransId);
    }

    // Remove leading committed transactions from the list.
    void truncate() {
        // count number of consecutive committed transactions at the beginning
        // of the list
        int c = bits.nextClearBit(0);
        bits = bits.get(c, count);

        // update state
        count -= c;
        baseTransId += c;
    }

    CommitList copy() {
        return new CommitList(this);
    }

    // Note that the transaction has started.
    void enlist(long transId) {
        int t = calcOffset(transId);
        bits.clear(t);
        ++count;
    }

    // This is the point before which all updates are known to be committed
    @Override
    void setCommitLSN(UndoPointer commitLSN) {
        this.commitLSN.assign(commitLSN);
    }

    @Override
    void setCommitTransId(long commitTransId) {
        // This is the transaction prior to which all transactions are known to
        // be committed
        // with respect to all currently active transactions
        this.commitTransId = commitTransId;
    }

    // Note that the transaction has committed.
    void commit(long transId) {
        int t = calcOffset(transId);
        bits.set(t);
        truncate();
    }
}
