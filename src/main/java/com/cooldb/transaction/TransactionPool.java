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

import com.cooldb.buffer.Transactions;
import com.cooldb.log.UndoPointer;
import com.cooldb.log.WAL;

/**
 * TransactionPool is a factory for transactions. It provides methods for
 * creating, committing, and cancelling transactions, and maintains the set of
 * active transactions.
 */
public class TransactionPool implements Transactions {

    // This is the initial number of concurrent transactions allowed,
    // and also the increment number if the system needs more
    final static int ALLOC_TRANSACTIONS = 8;
    // Transaction pool
    private Transaction[] pool = new Transaction[0];
    // Authoritative list of all committed transactions to date.
    private MasterCommitList masterCommitList;
    // Next transaction identifier
    private long nextTransId;
    // Log manager
    private final WAL wal;
    // Transaction creation flag
    private boolean quiesce;

    public TransactionPool(WAL wal, long nextTransId) {
        this.wal = wal;
        reset(nextTransId, wal.getEndOfUndoLog());
    }

    /**
     * Enables or disables the creation of new transactions.
     *
     * @param quiesce If true, causes {@link #beginTransaction} to fail.
     */
    public synchronized void setQuiesce(boolean quiesce) {
        this.quiesce = quiesce;
    }

    /**
     * Disables creation of new transactions and waits until all currently
     * active transactions are committed or until transaction creation is
     * re-enabled (see {@link #setQuiesce}) or until the timeout is reached.
     *
     * @param timeout the maximum time to wait in milliseconds
     * @return true if all transactions are committed
     */
    public synchronized boolean quiesce(long timeout)
            throws InterruptedException {
        quiesce = true;

        // wait for all transactions to commit or for the quiesce state to
        // change
        // or until the timeout time has elapsed
        while (quiesce && masterCommitList.getCount() > 0
                && timeout > 0) {
            wait(1000);
            timeout -= 1000;
        }

        return masterCommitList.getCount() == 0;
    }

    /**
     * Reset the TransactionPool, setting the next transaction identifier to the
     * one provided and assuming all prior transactions are committed. Also set
     * the masterCommitList commitLSN to the end-of-log.
     */
    public synchronized void reset(long nextTransId) {
        reset(nextTransId, wal.getEndOfUndoLog());
    }

    /**
     * Reset the TransactionPool, setting the next transaction identifier to the
     * one provided and assuming all prior transactions are committed. Also set
     * the masterCommitList commitLSN to the one provided.
     */
    public synchronized void reset(long nextTransId, UndoPointer commitLSN) {
        if (nextTransId < 1)
            throw new RuntimeException(
                    "Transaction identifier must be positive. Zero is reserved.");
        this.nextTransId = nextTransId;
        pool = new Transaction[1];
        masterCommitList = new MasterCommitList(nextTransId, commitLSN);
    }

    /**
     * The next transaction identifier that will be assigned to the next
     * transaction created by the next call of beginTransaction.
     */
    public synchronized long getNextTransId() {
        return nextTransId;
    }

    /**
     * Creates a new transaction.
     *
     * @throws TransactionCancelledException if transaction creation is disabled (see {@link #setQuiesce})
     */
    public synchronized Transaction beginTransaction()
            throws TransactionCancelledException {
        if (quiesce)
            throw new TransactionCancelledException(
                    "Database shutdown in progress.");

        Transaction trans = allocTransaction();
        trans.setTransId(nextTransId++);
        trans.setCommitList(masterCommitList.copy());
        masterCommitList.enlist(trans.getTransId());
        return trans;
    }

    /**
     * Resume a prior transaction during startup recovery.
     */
    public synchronized Transaction beginTransaction(TransactionState state) {
        Transaction trans = allocTransaction();
        trans.assign(state);
        trans.setCommitList(masterCommitList.copy());
        masterCommitList.enlist(trans.getTransId());
        return trans;
    }

    /**
     * End the transaction created by the method beginTransaction.
     */
    public synchronized void endTransaction(Transaction trans) {
        // remove from pool
        for (int i = 0; i < pool.length; i++) {
            if (pool[i] == trans) {
                pool[i] = null;
                break;
            }
        }
        masterCommitList.commit(trans.getTransId());
        masterCommitList.setCommitLSN(calcCommitLSN());
        masterCommitList.setCommitTransId(calcCommitTransId());
    }

    /**
     * Get the transaction identified by transId.
     */
    public synchronized Transaction getTransaction(long transId) {
        for (Transaction transaction : pool) {
            if (transaction != null && transaction.getTransId() == transId)
                return transaction;
        }
        return null;
    }

    /**
     * Is the specified transaction committed?
     */
    public synchronized boolean isCommitted(long transId) {
        return masterCommitList.isCommitted(transId);
    }

    /**
     * Is the specified transaction committed with respect to all active
     * transactions?
     */
    public synchronized boolean isUniversallyCommitted(long transId) {
        return masterCommitList.isUniversallyCommitted(transId);
    }

    /**
     * Return a copy of the states of currently active transactions.
     */
    public synchronized TransactionState[] getActiveTransactions() {
        // Return a copy of the state of active transactions
        int active = 0;
        for (Transaction transaction : pool) {
            if (transaction != null && transaction.getTransId() != -1)
                ++active;
        }
        TransactionState[] t = new TransactionState[active];
        active = 0;
        for (Transaction trans : pool) {
            if (trans != null && trans.getTransId() != -1) {

                // Synchronize on each transaction to guarantee that
                // the state returned is consistent with what has
                // been written to the log.
                synchronized (trans) {
                    t[active++] = new TransactionState(trans);
                }
            }
        }
        return t;
    }

    /**
     * Determine the oldest undo log record that could possibly be wanted by all
     * active transactions for reconstructing old versions of pages. This is the
     * oldest commitLSN among the active transactions.
     */
    public synchronized UndoPointer calcMinCommitLSN() {
        Transaction oldest = getOldestCommitLSN();
        if (oldest == null)
            return wal.getEndOfUndoLog();
        return oldest.getCommitLSN();
    }

    /**
     * Determine the oldest undo log record written by all active transactions
     */
    public synchronized UndoPointer calcCommitLSN() {
        UndoPointer commitLSN = null;
        for (Transaction tte : pool) {
            if (tte != null && tte.getTransId() != -1) {
                synchronized (tte) {
                    UndoPointer firstLSN = tte.getFirstLSN().isNull() ? wal
                            .getEndOfUndoLog() : tte.getFirstLSN();
                    if (commitLSN == null)
                        commitLSN = firstLSN;
                    else if (firstLSN.getLSN() < commitLSN.getLSN())
                        commitLSN = firstLSN;
                }
            }
        }
        return commitLSN == null ? wal.getEndOfUndoLog() : commitLSN;
    }

    /**
     * Determine the oldest baseTransId with respect to all active transactions
     */
    public synchronized long calcCommitTransId() {
        Transaction oldest = getOldestBaseTransId();
        if (oldest == null)
            return masterCommitList.getBaseTransId();
        return oldest.getBaseTransId();
    }

    /**
     * Find the transaction with the oldest non-null commitLSN.
     */
    public synchronized Transaction getOldestCommitLSN() {
        Transaction oldest = null;
        for (Transaction tte : pool) {
            if (tte != null && tte.getTransId() != -1) {
                if (!tte.getCommitLSN().isNull()) {
                    if (oldest == null)
                        oldest = tte;
                    else if (tte.getCommitLSN().getLSN() < oldest.getCommitLSN().getLSN())
                        oldest = tte;
                }
            }
        }
        return oldest;
    }

    /**
     * Find the transaction with the oldest baseTransId.
     */
    public synchronized Transaction getOldestBaseTransId() {
        Transaction oldest = null;
        for (Transaction tte : pool) {
            if (tte != null && tte.getTransId() != -1) {
                if (oldest == null)
                    oldest = tte;
                else if (tte.getBaseTransId() < oldest.getBaseTransId())
                    oldest = tte;
            }
        }
        return oldest;
    }

    private Transaction allocTransaction() {
        for (int i = 0; i < pool.length; i++) {
            if (pool[i] == null) {
                pool[i] = new Transaction();
                return pool[i];
            }
        }
        // need room for more transactions
        Transaction[] newPool = new Transaction[pool.length
                + ALLOC_TRANSACTIONS];
        System.arraycopy(pool, 0, newPool, 0, pool.length);
        pool = newPool;
        return allocTransaction();
    }
}
