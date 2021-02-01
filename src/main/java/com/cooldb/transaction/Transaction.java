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

import com.cooldb.lock.Lock;
import com.cooldb.log.UndoPointer;

public class Transaction extends TransactionState {

    // This is the initial number of locks allowed,
    // and also the increment number if the transaction needs more
    final static int ALLOC_LOCKS = 5;
    private Lock[] locks = new Lock[ALLOC_LOCKS];
    private int nlocks;
    private CommitList commitList;
    private boolean isCancelled;
    private boolean isSuspended;
    private boolean isSerializable;
    private boolean hasWaiters;

    // package-private constructor
    Transaction() {
        super();
    }

    public void setCommitList(CommitList commitList) {
        this.commitList = commitList;
    }

    /**
     * Return true of the specified transaction was committed with respect to
     * this list and all transactions active in the list.
     */
    public boolean isUniversallyCommitted(long transId) {
        return commitList.isUniversallyCommitted(transId);
    }

    /**
     * Was the specified transaction committed when this one began?
     */
    public boolean isCommitted(long transId) {
        return commitList.isCommitted(transId);
    }

    /**
     * Returns the address prior to which updates are guaranteed to be committed
     * with respect to the start of this transaction, used in multi-version
     * concurrency control. Note that the converse is not also true: updates
     * made after this point may in fact be committed with respect to this
     * transaction.
     */
    public UndoPointer getCommitLSN() {
        return commitList.getCommitLSN();
    }

    /**
     * The base transId is the oldest transaction such that all transactions
     * prior to it had committed prior to the start of this transaction.
     */
    public long getBaseTransId() {
        return commitList.getBaseTransId();
    }

    /**
     * The commit transId is the oldest transaction such that all transactions
     * prior to it had committed prior to the start of all transactions active
     * at the time this one began.
     */
    public long getCommitTransId() {
        return commitList.getCommitTransId();
    }

    // Cancelled state
    public boolean isCancelled() {
        return isCancelled;
    }

    public void setCancelled(boolean isCancelled) {
        this.isCancelled = isCancelled;
    }

    // Suspended state
    public boolean isSuspended() {
        return isSuspended;
    }

    public void setSuspended(boolean isSuspended) {
        this.isSuspended = isSuspended;
    }

    // Isolation level
    public boolean isSerializable() {
        return isSerializable;
    }

    public void setSerializable(boolean isSerializable) {
        this.isSerializable = isSerializable;
    }

    // Deadlock detector state
    public boolean hasWaiters() {
        return hasWaiters;
    }

    public void setHasWaiters(boolean hasWaiters) {
        this.hasWaiters = hasWaiters;
    }

    public void validate() throws InvalidTransactionException {
        if (isCancelled || isCommitted())
            throw new InvalidTransactionException(
                    "Transaction Already Committed.");
    }

    /**
     * Pushes the lock onto this transaction lock stack.
     *
     * @param lock the lock to be added to the stack
     */
    public void addLock(Lock lock) {
        if (nlocks == locks.length) {
            // need room for more locks
            Lock[] newArray = new Lock[locks.length + ALLOC_LOCKS];
            System.arraycopy(locks, 0, newArray, 0, locks.length);
            locks = newArray;
        }
        locks[nlocks++] = lock;
    }

    /**
     * Returns the lock on the top of the lock stack without popping it off, if
     * there is one, or null if the stack is empty.
     */
    public Lock getUndoNxtLock() {
        if (nlocks == 0)
            return null;
        return locks[nlocks - 1];
    }

    /**
     * Pops and unlocks all locks held by this transaction.
     */
    public void releaseLocks() {
        releaseLocksTo(null);
    }

    /**
     * Pops and unlocks locks from this transaction lock stack. If 'to' is
     * specified, then locks are popped from the stack only until the one
     * specified is encountered, in which case all remaining locks, including
     * the one specified, are left on the stack.
     *
     * @param to The lock at which point the releasing of locks is aborted.
     */
    public void releaseLocksTo(Lock to) {
        while (nlocks > 0) {
            if (locks[nlocks - 1] == to)
                return;
            locks[--nlocks].unlock();
        }
    }
}
