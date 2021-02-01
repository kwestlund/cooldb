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

import com.cooldb.buffer.FilePage;
import com.cooldb.buffer.BufferNotFound;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.*;
import com.cooldb.log.*;

/**
 * TransactionManager encompasses the complete set of transaction services. It
 * delegates to underlying TransactionPool, TransactionLogger, DeadlockDetector,
 * and BufferPool objects for support.
 * <p>
 * TransactionManager ties PageBuffer unpin methods with TransactionLogger
 * logging methods in order to better enforce the Write-Ahead-Logging protocol
 * and to preserve the integrity of transactions in the face of asynchronous
 * cancellation and rollback.
 */

public class TransactionManager {

    private final TransactionPool transactionPool;
    private final TransactionLogger transactionLogger;
    private final BufferPool bufferPool;
    private final DeadlockDetector deadlockDetector;
    private MVCC mvcc;

    public TransactionManager(TransactionPool transactionPool,
                              TransactionLogger transactionLogger, BufferPool bufferPool) {
        this.transactionPool = transactionPool;
        this.transactionLogger = transactionLogger;
        this.bufferPool = bufferPool;
        this.deadlockDetector = new DeadlockDetector();
    }

    public void setVersionController(MVCC mvcc) {
        this.mvcc = mvcc;
    }

    /**
     * Enables or disables the creation of new transactions.
     *
     * @param quiesce If true, causes {@link #beginTransaction} to fail.
     */
    public void setQuiesce(boolean quiesce) {
        transactionPool.setQuiesce(quiesce);
    }

    /**
     * Disables creation of new transactions and waits until all currently
     * active transactions are committed or until transaction creation is
     * re-enabled (see {@link #setQuiesce}) or until the timeout is reached.
     *
     * @param timeout the maximum time to wait in milliseconds
     * @return true if all transactions are committed
     */
    public boolean quiesce(long timeout) throws InterruptedException {
        return transactionPool.quiesce(timeout);
    }

    /**
     * Returns the transaction DeadlockDetector.
     */
    public DeadlockDetector getDeadlockDetector() {
        return deadlockDetector;
    }

    /**
     * Return the current log-sequence-number such that any modification to the
     * database with a sequence number greater than or equal to the returned
     * number will have been made after this point in time.
     */
    public long getStabilityPoint() {
        return transactionLogger.getStabilityPoint();
    }

    /**
     * Retrieve the specified undo log.
     */
    public void readUndo(UndoLog log) throws LogNotFoundException,
            BufferNotFound, InterruptedException {
        transactionLogger.readUndo(log);
    }

    /**
     * Creates a new transaction.
     *
     * @throws TransactionCancelledException if transaction creation is disabled (see {@link #setQuiesce})
     */
    public Transaction beginTransaction() throws TransactionCancelledException {
        return transactionPool.beginTransaction();
    }

    public void commitTransaction(Transaction trans)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        if (!trans.isCommitted()) {
            transactionLogger.writeCommitLog(trans);
            trans.releaseLocks();
            deadlockDetector.didCommit(trans);
            transactionPool.endTransaction(trans);
        }
    }

    /**
     * Is the specified transaction committed?
     */
    public boolean isCommitted(long transId) {
        return transactionPool.isCommitted(transId);
    }

    /**
     * Is the specified transaction committed?
     */
    public boolean isUniversallyCommitted(long transId) {
        return transactionPool.isUniversallyCommitted(transId);
    }

    /**
     * Note that one transaction is waiting for another, check for deadlock, and
     * wait for the holder to commit.
     */
    public void waitFor(Transaction trans, long transId)
            throws TransactionCancelledException {
        Transaction holder = transactionPool.getTransaction(transId);
        if (holder != null) {
            waitFor(trans, holder);
        }
    }

    /**
     * Note that one transaction is waiting for another, check for deadlock, and
     * wait for the holder to commit.
     */
    public void waitFor(Transaction trans, Transaction holder)
            throws TransactionCancelledException {
        try {
            trans.setSuspended(true);
            deadlockDetector.waitFor(trans, holder);
        } finally {
            trans.setSuspended(false);
        }
    }

    public PageBuffer pin(FilePage page, BufferPool.Mode mode)
            throws BufferNotFound, InterruptedException {
        return bufferPool.pin(page, mode);
    }

    public PageBuffer tryPin(FilePage page, BufferPool.Mode mode)
            throws BufferNotFound {
        return bufferPool.tryPin(page, mode);
    }

    public PageBuffer pinVersion(Transaction trans, FilePage page, long version)
            throws BufferNotFound, InterruptedException, RollbackException {

        // First pin the current version of the page
        PageBuffer pageBuffer = bufferPool.pin(page, BufferPool.Mode.SHARED);
        if (mvcc.needsRollback(trans, pageBuffer)) {
            bufferPool.unPin(pageBuffer, BufferPool.Affinity.LIKED);
            pageBuffer = bufferPool.pinVersion(page, trans.getTransId(),
                                               version);
            try {
                mvcc.rollback(trans, pageBuffer, version);
            } catch (RollbackException re) {
                bufferPool.unPin(pageBuffer, BufferPool.Affinity.HATED);
                throw re;
            }
        }
        return pageBuffer;
    }

    public PageBuffer pinNew(FilePage page) throws BufferNotFound,
            InterruptedException {
        PageBuffer pageBuffer = bufferPool.pinNew(page);

        // Set the pageLSN of the buffer to the end-of-log (necessary to
        // maintain recovery redo protocol)
        LogPage.setPageLSN(pageBuffer, transactionLogger.getEndOfLog());

        // Set the pageFirstLSN of the buffer to the end-of-undo-log (for
        // multi-version performance purposes)
        LogPage.setPageFirstLSN(pageBuffer, transactionLogger
                .getStabilityPoint());

        return pageBuffer;
    }

    public PageBuffer pinTemp(FilePage page, Transaction trans)
            throws BufferNotFound, InterruptedException {
        return bufferPool.pinTemp(page, trans.getTransId());
    }

    public void unPin(PageBuffer page, BufferPool.Affinity affinity) {
        bufferPool.unPin(page, affinity);
    }

    public void unPinRedo(PageBuffer page, BufferPool.Affinity affinity,
                          long redoAddress) {
        // Set the pageLSN to the address of the redoLog applied
        LogPage.setPageLSN(page, redoAddress);

        bufferPool.unPinDirty(page, affinity, redoAddress);
    }

    public void unPinDirty(PageBuffer page, BufferPool.Affinity affinity,
                           long redoAddress) {
        bufferPool.unPinDirty(page, affinity, redoAddress);
    }

    public void unPin(Transaction trans, PageBuffer page,
                      BufferPool.Affinity affinity, UndoLog undo, RedoLog redo)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            // Write to the log first (WAL)
            transactionLogger.writeUndoRedo(trans, page, undo, redo);
        } finally {
            // Now unpin the page, supplying the buffer manager with the end log
            // point
            bufferPool.unPinDirty(page, affinity, redo.getAddress());
        }
    }

    public void unPin(Transaction trans, PageBuffer page,
                      BufferPool.Affinity affinity, RedoLog redo)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            // Write to the log first (WAL)
            transactionLogger.writeRedo(trans, page, redo);
        } finally {
            // Now unpin the page, supplying the buffer manager with the end log
            // point
            bufferPool.unPinDirty(page, affinity, redo.getAddress());
        }
    }

    public void unPin(Transaction trans, PageBuffer page,
                      BufferPool.Affinity affinity, CLR clr)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            // Write to the log first (WAL)
            transactionLogger.writeCLR(trans, page, clr);
        } finally {
            // Now unpin the page, supplying the buffer manager with the end log
            // point
            bufferPool.unPinDirty(page, affinity, clr.getAddress());
        }
    }

    public void writeUndoRedo(Transaction trans, PageBuffer page, UndoLog undo,
                              RedoLog redo) throws LogExhaustedException, BufferNotFound,
            InterruptedException {
        transactionLogger.writeUndoRedo(trans, page, undo, redo);
    }

    public void writeRedo(Transaction trans, PageBuffer page, RedoLog redo)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        transactionLogger.writeRedo(trans, page, redo);
    }

    public void writeCLR(Transaction trans, PageBuffer page, CLR clr)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        transactionLogger.writeCLR(trans, page, clr);
    }

    /**
     * Begin a nested transaction.
     */
    public NestedTopAction beginNestedTopAction(Transaction trans) {
        return new NestedTopAction((UndoPointer) trans.getUndoNxtLSN().copy(),
                                   trans.getUndoNxtLock());
    }

    /**
     * Commit the nested transaction.
     */
    public void commitNestedTopAction(Transaction trans,
                                      NestedTopAction savePoint) throws LogExhaustedException,
            BufferNotFound, InterruptedException {
        transactionLogger.commitNestedTopAction(trans, savePoint
                .getUndoNxtLSN());
        trans.releaseLocksTo(savePoint.getUndoNxtLock());
        deadlockDetector.didCommit(trans);
    }

    /**
     * Rollback the innermost nested transaction using the provided delegate.
     */
    public void rollbackNestedTopAction(Transaction trans,
                                        NestedTopAction savePoint, RollbackDelegate delegate)
            throws RollbackException {
        transactionLogger.rollbackNestedTopAction(trans, savePoint
                .getUndoNxtLSN(), delegate);
        trans.releaseLocksTo(savePoint.getUndoNxtLock());
        deadlockDetector.didCommit(trans);
    }

    /**
     * Rollback a transaction using the provided delegate.
     */
    public void rollback(Transaction trans, RollbackDelegate delegate)
            throws RollbackException {
        transactionLogger.rollback(trans, null, delegate);
        trans.releaseLocks();
    }
}
