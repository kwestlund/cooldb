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

package com.cooldb.recovery;

import com.cooldb.buffer.*;
import com.cooldb.log.*;
import com.cooldb.transaction.TransactionLogger;
import com.cooldb.transaction.TransactionPool;
import com.cooldb.transaction.TransactionState;
import com.cooldb.buffer.*;
import com.cooldb.log.*;
import com.cooldb.transaction.Transaction;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public class RecoveryManager implements WAL {

    final TreeMap<FilePage, DPEntry> dirtyPages = new TreeMap<>();
    TreeMap<TransactionState, TransactionState> transTab = new TreeMap<>();
    private final SystemKey systemKey;
    private final LogManager logManager;
    private final TransactionPool transactionPool;
    private final TransactionLogger transactionLogger;
    private final BufferPool bufferPool;
    private final CheckpointWriter checkpointWriter;
    private final RecoveryContext context;
    private RedoLog redoLog;
    private long nextTransId;

    public RecoveryManager(SystemKey systemKey, LogManager logManager,
                           TransactionPool transactionPool,
                           TransactionLogger transactionLogger, BufferPool bufferPool,
                           CheckpointWriter checkpointWriter, RecoveryContext context) {
        this.systemKey = systemKey;
        this.logManager = logManager;
        this.transactionPool = transactionPool;
        this.transactionLogger = transactionLogger;
        this.bufferPool = bufferPool;
        this.checkpointWriter = checkpointWriter;
        this.context = context;
    }

    // RecoveryManager
    public void recover() throws RecoveryException {
        try {
            analyze();

            if (dirtyPages.size() > 0)
                redo();

            context.didRedoPass();

            if (transTab.size() > 0)
                undo();

            restart();

            checkpointWriter.syncCheckPoint();
        } catch (Exception e) {
            throw new RecoveryException(e);
        }
    }

    // WriteAheadLogging implementation used during redo
    public long flushTo(long lsn) {
        return lsn;
    }

    /**
     * Returns the address of the next redo record to be applied during the redo
     * pass. The BufferPool will ask for this address when the page to which the
     * redo record applies is pinned in EXCLUSIVE mode. The BufferManager uses
     * this address as the recLSN for that page's buffer.
     */
    public long getEndOfLog() {
        return redoLog.getAddress();
    }

    public UndoPointer getEndOfUndoLog() {
        return redoLog.getUndoNxtLSN();
    }

    public int getRemaining(long recLSN) {
        return logManager.getRemaining(recLSN);
    }

    /**
     * Analyze the redo log to determine: 1) the set of dirty pages with updates
     * that need to be redone 2) the set of loser transactions with updates that
     * need to be undone 3) the next transaction identifier that is greater than
     * all previous 4) the redo and undo log boundaries
     */
    void analyze() {

        // Scan redo log file, starting with master record, until the endOfFile
        RedoLog redoLog = new RedoLog();
        redoLog.setAddress(systemKey.getMaster());

        // If the address is zero, the database was never successfully
        // created
        if (redoLog.getAddress() == 0)
            throw new RuntimeException("Database does not exist.");

        // Make sure we have matching versions so we don't corrupt a database
        if (systemKey.getVersion() != SystemKey.VERSION)
            throw new RuntimeException(
                    "Database instance version does not match database version.");

        TransactionState tte = null;
        /*
         * Identify the next transaction id to be allocated when we restart the
         * transaction manager. Never reuse a transaction id that had some
         * effect on the database.
         */
        long lastTransId = 0;
        Iterator<RedoLog> iter = logManager.redoIterator(redoLog);
        while (iter.hasNext()) {
            iter.next();

            if (redoLog.getTransID() > 0) {
                lastTransId = Math.max(lastTransId, redoLog.getTransID());

                // Keep track of the firstLSN and undoNxtLSN of each transaction
                tte = new TransactionState();
                tte.setTransId(redoLog.getTransID());
                tte.setFirstLSN(redoLog.getUndoNxtLSN());
                tte.setUndoNxtLSN(redoLog.getUndoNxtLSN());
            }

            // Process record based on type
            switch (redoLog.getType()) {
                case Log.CLR:
                case Log.UPDATE:
                    // Capture the firstLSN and the undoNxtLSN of the transaction
                    if (tte != null) {
                        addTransaction(tte);
                    }

                    // Capture the recLSN of the page, if there is one
                    if (!redoLog.getPage().isNull())
                        addDirtyPage(new DPEntry(redoLog.getPage(), redoLog
                                .getAddress()));
                    break;

                case Log.COMMIT:
                    // Set the transaction state to committed
                    if (tte != null) {
                        tte.setCommitted(true);
                        addTransaction(tte);
                    }
                    break;

                case Log.BEGIN_CHECKPOINT:
                    // Nothing to do here
                    break;

                case Log.END_CHECKPOINT:
                    readCheckPoint(redoLog);
                    break;

                default:
                    break;
            }
        }

        // Remove committed transactions
        removeCommitted();

        // Determine the next transaction identifier
        nextTransId = Math.max(lastTransId + 1, systemKey.getNextTransId());

        // Prepare the redo log for writing
        logManager.setEndOfLog(redoLog.getAddress());
        logManager.setStartOfLog(Math.min(calcMinRecLSN(), systemKey
                .getMaster()));
    }

    /**
     * Redo updates on a physical page-by-page basis for those pages that need
     * it.
     */
    void redo() throws RedoException, BufferNotFound, InterruptedException {
        /*
         * Tell the BufferPool to use this RecoveryManager as its
         * WriteAheadLogging delegate for this redo operation.
         */
        bufferPool.setWriteAheadLogging(this);

        // Scan log file, starting with minimum recovery lsn
        redoLog = new RedoLog();
        redoLog.setAddress(calcMinRecLSN());

        Iterator<RedoLog> iter = logManager.redoIterator(redoLog);
        while (iter.hasNext()) {
            iter.next();

            DPEntry dpe = dirtyPages.get(redoLog.getPage());
            if (dpe != null && redoLog.getAddress() >= dpe.getRecLSN()) {
                /*
                 * We have a redoable page update. The updated page may not have
                 * made it to stable storage before the system failure, so we
                 * need to access the page and check its LSN.
                 *
                 * If the update is not already on the page, redo it. The
                 * BufferPool is informed that the end of the log is the current
                 * redo log record's address for purposes of ensuring that the
                 * updates redone here are eventually written to disk, either
                 * during replacement or during the next checkpoint, as usual.
                 * (see getEndOfLog above for more details on how this works).
                 *
                 * Furthermore, since the redo records are already stored in the
                 * the log, any attempt by the BufferManager to enforce the WAL
                 * protocol by flushing the log is intercepted and ignored. (see
                 * flushTo above)
                 */
                PageBuffer pageBuffer = bufferPool.pin(dpe,
                                                       BufferPool.Mode.SHARED);
                long pageLSN = LogPage.getPageLSN(pageBuffer);
                bufferPool.unPin(pageBuffer, BufferPool.Affinity.LIKED);

                if (pageLSN < redoLog.getAddress()) {
                    context.redo(redoLog);
                } else {
                    /*
                     * Advance the recLSN to equal the pageLSN + 1 now that we
                     * know what the pageLSN actually is. This allows us to skip
                     * any further unnecessary checking. The next record we find
                     * for this page will definitely need to be redone.
                     */
                    dpe.setRecLSN(pageLSN + 1);
                }
            }
        }

        /*
         * Now tell the BufferPool to use the LogManager as its
         * WriteAheadLogging delegate from now on.
         */
        bufferPool.setWriteAheadLogging(logManager);
    }

    /**
     * Undo updates on a logical transaction-by-transaction basis. Scan undoLog
     * file, starting with maxUndoLSN, undoing updates for all uncommitted
     * transactions in the reverse order in which they occurred.
     */
    void undo() throws RecoveryException {
        try {
            TransactionState tte;
            UndoLog undoLog = new UndoLog();

            // prepare transaction manager
            transactionPool.reset(calcFirstTransId(), calcCommitLSN());
            createTransactions();

            /*
             * Determine start point as maximum undoNxtLSN of all uncommitted
             * trans. Return if there is nothing to be undone
             */
            while ((tte = getMaxUndoNxtLSN()) != null) {

                // Get the log record
                undoLog.setAddress(tte.getUndoNxtLSN());
                logManager.readUndo(undoLog);

                // Undo it if it is not a CLR
                if (!undoLog.isCLR())
                    context.undo(undoLog, (Transaction) tte);

                // Set the next record to be undone for this transaction
                tte.setUndoNxtLSN(undoLog.getUndoNxtLSN());
            }

            commitTransactions();
        } catch (Exception e) {
            throw new RecoveryException(e);
        }
    }

    private void createTransactions() {
        // Create a managed transaction for each loser transaction, in transId
        // order
        TreeMap<TransactionState, TransactionState> newTransTab = new TreeMap<>();
        for (TransactionState tte : transTab.values()) {
            Transaction trans = transactionPool.beginTransaction(tte);
            newTransTab.put(trans, trans);
        }
        transTab = newTransTab;
    }

    void restart() {
        transactionPool.reset(nextTransId);
    }

    /**
     * Identify transactions that have no work left to be undone and make sure
     * they are committed.
     */
    private void commitTransactions() throws LogExhaustedException,
            BufferNotFound, InterruptedException {
        for (TransactionState tte : transTab.values()) {
            if (tte.getUndoNxtLSN().isNull() && !tte.isCommitted()) {
                transactionLogger.writeCommitLog(tte);
            }
        }
    }

    /**
     * Read from the End Checkpoint record the contents of the transaction
     * table, dirty pages table, and (if we support distributed processing) the
     * locks held by transactions in the 'in-doubt' state. Restore these to
     * their appropriate places.
     */
    void readCheckPoint(Log log) {
        LogData logData = log.getData();
        ByteBuffer bb;

        // Read recovery dirty pages, replacing existing entries with new ones
        bb = logData.getData();
        if (bb != null)
            readDirtyPages(bb);

        // Read transactions, without replacing existing entries
        logData = logData.next();
        bb = logData.getData();
        if (bb != null)
            readTransactions(bb);
    }

    private void readDirtyPages(ByteBuffer bb) {
        /*
         * Read recovery dirty pages, replacing the recLSN of existing entries
         * only if the recLSN of the checkpointed page is less than the existing
         * one
         */
        while (bb.hasRemaining()) {
            DPEntry dpe = new DPEntry();
            dpe.readFrom(bb);
            addDirtyPage(dpe);
        }
    }

    private void readTransactions(ByteBuffer bb) {
        /*
         * Read transactions from a checkpoint. Merge the transaction states in
         * the checkpoint record with those already collected such that the
         * resulting state contains the minimum firstLSN and maximum undoNxtLSN.
         */
        while (bb.hasRemaining()) {
            TransactionState trans = new TransactionState();
            trans.readFrom(bb);
            addTransaction(trans);
        }
    }

    private long calcMinRecLSN() {
        long minRecLSN = Long.MAX_VALUE;
        for (DPEntry dpe : dirtyPages.values()) {
            if (dpe.getRecLSN() < minRecLSN)
                minRecLSN = dpe.getRecLSN();
        }
        return minRecLSN;
    }

    /**
     * Determine the transaction with the next valid (>0) maximum 'undoLSN' of
     * all loser (uncommitted) transactions during the 'undo' pass.
     */
    private TransactionState getMaxUndoNxtLSN() {
        TransactionState max = null;
        for (TransactionState tte : transTab.values()) {
            // choose the loser transaction with the most recent update to be
            // undone
            if (max != null) {
                if (tte.getUndoNxtLSN().getLSN() > max.getUndoNxtLSN().getLSN())
                    max = tte;
            } else if (tte.getUndoNxtLSN().getLSN() > 0)
                max = tte;
        }
        return max;
    }

    /**
     * Determine the first loser transaction id needing recovery.
     */
    private long calcFirstTransId() {
        TransactionState tte = transTab.firstKey();
        if (tte != null)
            return tte.getTransId();
        return 0;
    }

    /**
     * Determine the oldest undo log record written by loser transactions
     */
    private UndoPointer calcCommitLSN() {
        TransactionState oldest = getOldestFirstLSN(transTab);
        if (oldest == null)
            return logManager.getEndOfUndoLog();
        return oldest.getFirstLSN();
    }

    /**
     * Find the transaction with the oldest non-null firstLSN.
     */
    private TransactionState getOldestFirstLSN(
            Map<TransactionState, TransactionState> pool) {
        TransactionState oldest = null;
        for (TransactionState tte : pool.values()) {
            if (!tte.getFirstLSN().isNull()) {
                if (oldest == null)
                    oldest = tte;
                else if (tte.getFirstLSN().getLSN() < oldest.getFirstLSN()
                        .getLSN())
                    oldest = tte;
            }
        }
        return oldest;
    }

    private void addDirtyPage(DPEntry dp) {
        DPEntry dpe = dirtyPages.get(dp);
        if (dpe == null)
            dirtyPages.put(dp, dp);
        else
            dpe.setRecLSN(Math.min(dpe.getRecLSN(), dp.getRecLSN()));
    }

    private void addTransaction(TransactionState trans) {
        TransactionState tte = transTab.get(trans);
        if (tte == null)
            transTab.put(trans, trans);
        else {
            if (!trans.getFirstLSN().isNull()) {
                if (tte.getFirstLSN().isNull()
                        || trans.getFirstLSN().compareTo(tte.getFirstLSN()) < 0)
                    tte.setFirstLSN(trans.getFirstLSN());
            }
            if (!trans.getUndoNxtLSN().isNull()) {
                if (tte.getUndoNxtLSN().isNull()
                        || trans.getUndoNxtLSN().compareTo(tte.getUndoNxtLSN()) > 0)
                    tte.setUndoNxtLSN(trans.getUndoNxtLSN());
            }
            tte.setCommitted(tte.isCommitted() || trans.isCommitted());
        }
    }

    private void removeCommitted() {
        transTab.values().removeIf(TransactionState::isCommitted);
    }
}
