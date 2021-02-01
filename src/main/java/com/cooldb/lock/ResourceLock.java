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

package com.cooldb.lock;

import com.cooldb.transaction.DeadlockDetector;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionCancelledException;
import com.cooldb.transaction.TransactionInterruptedException;

/**
 * ResourceLock brokers transaction access to a resource.
 */
public class ResourceLock implements Resource {
    private static final int ALLOC_LOCKS = 5;
    private final DeadlockDetector dd;
    private TransactionLock[] locks = new TransactionLock[ALLOC_LOCKS];
    private int nlocks;
    private TransactionLock lastWriter;
    private TransactionLock convertLock;

    public ResourceLock(DeadlockDetector dd) {
        this.dd = dd;
    }

    /**
     * Returns a share lock associated with this resource.
     */
    public Lock readLock(Transaction trans) {
        return new TransactionLock(trans, false);
    }

    /**
     * Returns an exclusive lock associated with this resource.
     */
    public Lock writeLock(Transaction trans) {
        return new TransactionLock(trans, true);
    }

    /**
     * Returns the lock held by the transaction, if there is one, null
     * otherwise.
     */
    public synchronized Lock getLock(Transaction trans) {
        for (int i = 0; i < nlocks; i++) {
            if (locks[i].trans == trans)
                return locks[i];
        }
        return null;
    }

    private boolean grab(TransactionLock lock, boolean trylock)
            throws TransactionCancelledException, LockAlreadyHeld {
        while (!Thread.currentThread().isInterrupted()) {
            Transaction holder = acquire(lock);

            // If no other transaction blocks this one, return
            if (holder == null)
                return true;

            // If only testing the lock, return false without waiting
            // and without acquiring the lock
            if (trylock)
                return false;

            // Otherwise, wait then try again
            dd.waitFor(lock.trans, holder);
        }
        throw new TransactionInterruptedException("lock failed");
    }

    /**
     * Acquire the lock if possible.
     *
     * @param lock the lock to be acquired
     * @return the blocking transaction if the lock could not be acquired, null
     * otherwise
     * @throws LockAlreadyHeld when the lock is already held by the same transaction
     */
    private synchronized Transaction acquire(TransactionLock lock)
            throws LockAlreadyHeld {
        if (nlocks > 0) {
            // scan the queue, perform maintenance, and gather some info
            if (alreadyExists(lock)) {
                throw new LockAlreadyHeld();
            } else {
                Transaction holder = checkConflict(lock);
                if (holder != null) {
                    // return the transaction holding the conflicting lock
                    return holder;
                }
            }
        }
        // add the lock to the resource queue
        push(lock);
        return null;
    }

    private Transaction checkConflict(TransactionLock lock) {
        // no locks, no conflict
        if (nlocks == 0)
            return null;
        /*
         * if the request is exclusive, wait for the last lock, unless the
         * transaction is converting a previously held read lock to a write lock
         * in which case make sure there are no other share locks granted and
         * replace the previous lock with the new one, or wait for the other
         * share locks to be released.
         */
        if (lock.isExclusive) {
            if (convertLock != null) {
                if (nlocks == 1) {
                    locks[0] = lock;
                    return null;
                } else if (locks[0] != convertLock) {
                    return locks[0].trans;
                } else if (locks[1].isExclusive) {
                    locks[0] = lock;
                    return null;
                } else {
                    return locks[1].trans;
                }
            }
            // Wait for the last lock
            return locks[nlocks - 1].trans;
        }
        // if there are writers, wait on the last one
        if (lastWriter != null)
            return lastWriter.trans;

        // otherwise no conflict exists
        return null;
    }

    private synchronized void release(TransactionLock lock) {
        lock.trans = null;
    }

    private void push(TransactionLock lock) {
        if (nlocks == locks.length) {
            // need room for more locks
            TransactionLock[] newArray = new TransactionLock[locks.length
                    + ALLOC_LOCKS];
            System.arraycopy(locks, 0, newArray, 0, locks.length);
            locks = newArray;
        }
        locks[nlocks++] = lock;
    }

    private boolean alreadyExists(TransactionLock request) {
        boolean alreadyExists = false;
        int removed = 0;
        lastWriter = null;
        convertLock = null;

        for (int i = 0; i < nlocks; i++) {
            // Remove locks held by committed transactions
            if (locks[i].trans == null || locks[i].trans.isCommitted()) {
                locks[i] = null;
                ++removed;
            } else {
                // Shift over removed locks
                locks[i - removed] = locks[i];

                // Identify the last writer
                if (locks[i].isExclusive) {
                    lastWriter = locks[i];
                }
                // Note whether the requesting transaction already has a lock on
                // this resource
                if (locks[i].trans == request.trans) {
                    alreadyExists = !request.isExclusive
                            || locks[i].isExclusive;
                    if (!alreadyExists)
                        convertLock = locks[i];
                }
            }
        }
        // Adjust count down by the number of removed locks
        nlocks -= removed;

        return alreadyExists;
    }

    private class TransactionLock implements Lock {
        final boolean isExclusive;
        Transaction trans;

        TransactionLock(Transaction trans, boolean isExclusive) {
            this.trans = trans;
            this.isExclusive = isExclusive;
        }

        @Override
        public void lock() throws TransactionCancelledException {
            try {
                grab(this, false);
                trans.addLock(this);
            } catch (LockAlreadyHeld e) {
                // skip adding to list of locks held
            }
        }

        @Override
        public boolean trylock() throws TransactionCancelledException {
            try {
                if (grab(this, true)) {
                    trans.addLock(this);
                    return true;
                }
            } catch (LockAlreadyHeld e) {
                return true;
            }
            return false;
        }

        @Override
        public void unlock() {
            release(this);
        }

        @Override
        public boolean isExclusive() {
            return this.isExclusive;
        }
    }
}
