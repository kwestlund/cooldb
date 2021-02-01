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

import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.LogManager;
import com.cooldb.log.LogPage;
import com.cooldb.log.UndoLog;
import com.cooldb.log.UndoPointer;

import java.util.HashMap;

public class MVCC {
    private final LogManager logManager;
    private final RollbackDelegate delegate;

    public MVCC(LogManager logManager, RollbackDelegate delegate) {
        this.logManager = logManager;
        this.delegate = delegate;
    }

    /**
     * Determine whether the page needs to be rolled back to a previous version
     * in order to be consistent with the start of the specified transaction.
     */
    public boolean needsRollback(Transaction trans, PageBuffer pb) {
        // If the current version is older than the oldest update by the
        // oldest active transaction in the commit list, then no
        // rollback is needed.
        UndoPointer pageUndoNxtLSN = new UndoPointer();
        LogPage.getPageUndoNxtLSN(pb, pageUndoNxtLSN);
        return pageUndoNxtLSN.getLSN() >= trans.getCommitLSN().getLSN();
    }

    /**
     * Undo all updates applied to the given page by all transactions other than
     * the given transaction back to the start of the given transaction,
     * excluding any updates made by the given transaction prior to or equal to
     * the specified cursor-stability-point.
     *
     * @param cusp cursor-stability-point
     */
    public PageBuffer rollback(Transaction trans, PageBuffer pb, long cusp)
            throws RollbackException {
        try {
            // Start of transaction
            long start = trans.getCommitLSN().getLSN();

            // If the current version is older than the oldest update by the
            // oldest active transaction in the commit list, then no rollback is
            // needed.
            UndoPointer pageUndoNxtLSN = new UndoPointer();
            LogPage.getPageUndoNxtLSN(pb, pageUndoNxtLSN);
            if (pageUndoNxtLSN.getLSN() < start)
                return pb;

            // Reconstruct an old version of the page in the buffer copy
            HashMap<TransactionState, TransactionState> transTab = new HashMap<>();
            UndoLog undoLog = new UndoLog();
            TransactionState tte;
            TransactionState key = new TransactionState();
            UndoPointer next = new UndoPointer(pageUndoNxtLSN);

            // Scan the log from the last one on the page backwards until there
            // are
            // no updates left on the page that were made after the commitLSN
            // point
            // for the transaction, which defines the point prior to which all
            // updates are guaranteed to be committed with respect to the
            // transaction.

            while (next.getLSN() >= start) {

                undoLog.setAddress(next);
                logManager.readUndo(undoLog);

                // Since updates made after the commitLSN for the transaction
                // may
                // actually be committed with respect to this transaction, check
                // the commit list to be sure.

                if (!trans.isCommitted(undoLog.getTransID())) {

                    // Keep track of the undoNxtLSN for each transaction
                    // being undone in this reconstruction of the page so that
                    // if the next LSN to be undone represents the log record
                    // of a transaction with an undoNxtLSN that points before
                    // the next LSN addresss, then skip the log record and
                    // continue with the next pageUndoNxtLSN

                    key.setTransId(undoLog.getTransID());
                    tte = transTab.get(key);
                    if (tte == null) {
                        tte = new TransactionState();
                        tte.setTransId(undoLog.getTransID());
                        tte.setUndoNxtLSN(next);
                        transTab.put(tte, tte);
                    }
                    if (tte.getUndoNxtLSN().getLSN() >= next.getLSN()) {
                        // If the log was written by a transaction other than
                        // the
                        // current one then we must undo it, unless it is a clr
                        // log record indicating the update is already undone in
                        // which case we must avoid undoing the update again by
                        // following the clr undoNxtLSN pointer.
                        // If the update was written by the current transaction
                        // after
                        // the cursor stability point it must also be undone
                        if (undoLog.getTransID() != trans.getTransId()
                                || next.getLSN() > cusp) {
                            if (undoLog.isCLR())
                                tte.setUndoNxtLSN(undoLog.getUndoNxtLSN());
                            else
                                delegate.undo(undoLog, pb);
                        }
                        // Note that updates made by the current transaction
                        // prior
                        // to the 'cusp' point are not undone
                    }
                }
                next = new UndoPointer(undoLog.getPageUndoNxtLSN());
            }

            // Set the pageUndoNxtLSN to ensure we do not repeat the rollback
            pageUndoNxtLSN.setLSN(start - 1);
            LogPage.setPageUndoNxtLSN(pb, pageUndoNxtLSN);

            return pb;
        } catch (Exception e) {
            throw new RollbackException("Failed to create snapshot of page.", e);
        }
    }
}
