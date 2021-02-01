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

import com.cooldb.buffer.BufferNotFound;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.*;
import com.cooldb.log.*;
import com.cooldb.recovery.CheckpointWriter;

/**
 * A TransactionLogger handles the writing of undo and redo log records on
 * behalf of transactions and supports atomicity and recovery semantics.
 */

public class TransactionLogger {

    private final LogManager logManager;
    private final CheckpointWriter checkpointWriter;

    public TransactionLogger(LogManager logManager,
                             CheckpointWriter checkpointWriter) {
        this.logManager = logManager;
        this.checkpointWriter = checkpointWriter;
    }

    /**
     * Returns the current end-of-log.
     */
    public long getEndOfLog() {
        return logManager.getEndOfLog();
    }

    /**
     * Returns the current log-sequence-number such that any modification to the
     * database with a sequence number greater than or equal to the returned
     * number will have been made after this point in time, or conversely, any
     * modification to the database with a sequence number less than the
     * returned number was made prior to this point in time.
     */
    public long getStabilityPoint() {
        return logManager.getEndOfUndoLog().getLSN();
    }

    /**
     * Writes an update to a specific page described by both undo and redo log
     * records, to the log file(s) as in
     * <code>writeUndoRedo(TransactionState trans, UndoLog undoLog, RedoLog redoLog)</code>.
     * In addition, the following modifications are made to the specific page
     * buffer.
     * <p>
     * Set the pageLSN of the buffer to the address of the redo log record.
     * <p>
     * Set the pageUndoNxtLSN of the undo log record to that of the buffer, and
     * <p>
     * Set the pageUndoNxtLSN of the buffer to the address of the undo log
     * record.
     * <p>
     * The pageBuffer must be pinned in mode EXCLUSIVE.
     */
    public void writeUndoRedo(TransactionState trans, PageBuffer pageBuffer,
                              UndoLog undoLog, RedoLog redoLog) throws LogExhaustedException,
            BufferNotFound, InterruptedException {

        // Set the pageUndoNxtLSN of the undo log record to that of the buffer
        // (before writing the undoLog, of course)
        LogPage.getPageUndoNxtLSN(pageBuffer, undoLog.getPageUndoNxtLSN());

        writeUndoRedo(trans, undoLog, redoLog);

        // Set the pageLSN of the buffer to the address of the redo log record.
        // (after writing the logs and obtaining their addresses)
        LogPage.setPageLSN(pageBuffer, redoLog.getAddress());

        // Set the pageUndoNxtLSN of the buffer to the address of the undo log
        // record.
        LogPage.setPageUndoNxtLSN(pageBuffer, undoLog.getAddress());
    }

    /**
     * Writes an update to a specific page described only by a redo log record,
     * to the redo log file as in
     * <code>writeRedo(TransactionState trans, RedoLog redoLog)</code>. In
     * addition, the following modifications are made to the specific page
     * buffer.
     * <p>
     * Set the pageLSN of the buffer to the address of the redo log record.
     * <p>
     * The pageBuffer must be pinned in mode EXCLUSIVE.
     */
    public void writeRedo(TransactionState trans, PageBuffer pageBuffer,
                          RedoLog redoLog) throws LogExhaustedException, BufferNotFound,
            InterruptedException {

        writeRedo(trans, redoLog);

        // Set the pageLSN of the buffer to the address of the redo log record.
        // (after writing the logs and obtaining their addresses)
        LogPage.setPageLSN(pageBuffer, redoLog.getAddress());
    }

    /**
     * Write a pair of undo and redo log records to the log file(s).
     * <p>
     * Set the identifier of the specified transaction in each log record.
     * <p>
     * If the undoLog is not a CLR, then set the undoNxtLSN of the undoLog to
     * that of the transaction. Otherwise, if the undoLog is a CLR, then keep
     * the undoNxtLSN set in the CLR already.
     * <p>
     * Set the undoNxtLSN of the redoLog and of the transaction to the address
     * of the undoLog obtained after writing it to the log file.
     * <p>
     * If this update is the first of the transaction, make note of it in the
     * transaction for Multi-Version-Concurrency-Control (MVCC) and log space
     * management purposes.
     */
    public void writeUndoRedo(TransactionState trans, UndoLog undoLog,
                              RedoLog redoLog) throws LogExhaustedException, BufferNotFound,
            InterruptedException {

        checkpoint(redoLog);

        // Prepare log records
        redoLog.setTransID(trans.getTransId());
        undoLog.setTransID(trans.getTransId());

        if (!undoLog.isCLR())
            undoLog.setUndoNxtLSN(trans.getUndoNxtLSN());

        /*
         * Note: this is synchronized on the transaction so that the
         * checkpointing and begin-transaction processes can be guaranteed to
         * view a transaction state that is consistent with what has been
         * written to the log
         */
        synchronized (trans) {
            // Write log records
            logManager.writeUndoRedo(undoLog, redoLog);

            // Note log record address assigned during the write
            if (trans.getFirstLSN().isNull())
                trans.setFirstLSN(undoLog.getAddress());

            // Set the undoNxtLSN of the transaction to the address
            // of the undoLog obtained after writing it to the log file.
            trans.setUndoNxtLSN(undoLog.getAddress());

            if (undoLog.isCLR()) {
                // Subtract from the cost of rolling back this transaction
                trans.addRollbackCost(-undoLog.storeSize());
            } else {
                // Add to the cost of rolling back this transaction
                trans.addRollbackCost(undoLog.storeSize());
            }
        }
    }

    /**
     * Write a redoLog record to the log file.
     * <p>
     * Set the identifier of the specified transaction in the log record.
     * <p>
     * Set the undoNxtLSN of the log record to that of the specified
     * transaction. This ensures that the latest redo log always points to the
     * next undo log to be undone.
     */
    public void writeRedo(TransactionState trans, RedoLog redoLog)
            throws LogExhaustedException, BufferNotFound, InterruptedException {

        checkpoint(redoLog);

        redoLog.setTransID(trans.getTransId());
        redoLog.setUndoNxtLSN(trans.getUndoNxtLSN());

        logManager.writeRedo(redoLog);
    }

    /**
     * Write a Compensation-Log-Record (CLR) to the log that compensates for the
     * given UndoLog.
     * <p>
     * Two log records are written on behalf of this CLR, one to the redo log
     * and the other to the undo log, and the behavior of these log writes is as
     * described in writeUndoRedo.
     */
    public void writeCLR(TransactionState trans, PageBuffer pageBuffer, CLR clr)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        writeUndoRedo(trans, pageBuffer, new UndoCLR(clr), clr);
    }

    /**
     * Write transaction commit log record. Completion of this method guarantees
     * that the transaction has committed successfully.
     */
    public void writeCommitLog(TransactionState trans)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        CommitLog log = new CommitLog(trans.getTransId());

        checkpoint(log);

        // Note: this is synchronized on the transaction so that the
        // checkpointing process
        // can be guaranteed to view a transaction state that is consitent with
        // what has
        // been written to the log

        synchronized (trans) {
            // Write log
            writeRedo(trans, log);

            // Set transaction state to committed
            trans.setCommitted(true);
        }

        // Flush log to commit point
        logManager.flushTo(log.getAddress());
    }

    /**
     * Write a CLR to skip nested action's updates during rollback.
     */
    public void commitNestedTopAction(Transaction trans, UndoPointer savePoint)
            throws LogExhaustedException, BufferNotFound, InterruptedException {

        CLR clr = new CLR(savePoint);

        writeUndoRedo(trans, new UndoCLR(clr), clr);

        // Flush log to commit.
        logManager.flushTo(clr.getAddress());
    }

    /**
     * Rollback a transaction to the start of the last nested-top-action using
     * the provided delegate.
     */
    public void rollbackNestedTopAction(Transaction trans,
                                        UndoPointer savePoint, RollbackDelegate delegate)
            throws RollbackException {
        rollback(trans, savePoint, delegate);
    }

    /**
     * Undo all updates performed by the given transaction back to but not
     * including the update logged at address 'to' using the given delegate.
     */
    public void rollback(Transaction trans, UndoPointer savePoint,
                         RollbackDelegate delegate) throws RollbackException {
        try {
            if (trans.isCommitted())
                return;

            UndoLog undoLog = new UndoLog();
            long to = savePoint == null ? 0 : savePoint.getLSN();

            while (to < trans.getUndoNxtLSN().getLSN()) {
                // Get the log record
                undoLog.setAddress(trans.getUndoNxtLSN());
                logManager.readUndo(undoLog);

                // Undo the update if it is not itself a CLR,
                // and write a CLR to note the update was undone
                if (!undoLog.isCLR())
                    delegate.undo(undoLog, trans);

                trans.setUndoNxtLSN(undoLog.getUndoNxtLSN());
            }
        } catch (Exception e) {
            throw new RollbackException(e);
        }
    }

    /**
     * Retrieve the specified undo log.
     */
    public void readUndo(UndoLog log) throws LogNotFoundException,
            BufferNotFound, InterruptedException {
        logManager.readUndo(log);
    }

    /**
     * Make sure there is enough redo log space for the write.
     */
    private void checkpoint(RedoLog redoLog) throws LogExhaustedException,
            BufferNotFound, InterruptedException {
        // make sure we leave a MINIMUM_AVAILABLE space in the redo log for
        // checkpointing
        if (logManager.getRemaining() - redoLog.storeSize() < CheckpointWriter.REDO_MINIMUM)
            checkpointWriter.syncCheckPoint();

        // Periodically request an asynchronous checkpoint in order to reclaim
        // log space
        checkpointWriter.checkPoint();
    }
}
