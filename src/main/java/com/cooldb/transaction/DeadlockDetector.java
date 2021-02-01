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

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * DeadlockDetector looks for waits-for cycles in groups of transactions that
 * are blocking in mutual deadlock.
 * <p>
 * The DeadlockDetector makes the following assumptions:
 * <ul>
 * <li>No transaction can wait for more than one resource simultaneously.
 * <li>A resource can have at most one blocking transaction (other transactions
 * may share the resource but only one can be considered the "blocking"
 * transaction).
 * </ul>
 * <p>
 * Given the above assumptions, the deadlock DeadlockDetector need only keep
 * track of which transaction is waiting for which transaction without regard
 * for the specific resource involved.
 * <p>
 * The DeadlockDetector must be informed by some external lock scheduler of each
 * of the following types of events:
 * <ul>
 * <li>Transaction A waits for Transaction B
 * <li>Transaction B commits
 * </ul>
 * <p>
 * The DeadlockDetector will automatically check for deadlock whenever an event
 * of the type "Transaction A waits for Transaction B" occurs by checking for
 * the creation of a cycle such that Transaction A ends up waiting for
 * Transaction A. For example, in the simplest case, an entry "Transaction B
 * waits for Transaction A" may already exist in the DeadlockDetector. If a
 * cycle is created, the DeadlockDetector will choose one transaction to cancel
 * from among the participating transactions. The DeadlockDetector will choose
 * the transaction having done the least amount of work on the database and
 * therefore the cheapest to rollback.
 * <p>
 * Upon receiving an event of type "Transaction B commits", the DeadlockDetector
 * will remove all previously registered waitsFor dependencies involving the
 * committed transaction and signal all waiting transactions to proceed.
 */

public class DeadlockDetector {
    private final Map<Transaction, Transaction> waitsForTab = Collections
            .synchronizedMap(new HashMap<>());

    /**
     * Note that one transaction is waiting for another, check for deadlock, and
     * wait for the holder to commit. If this results in a deadlock AND this
     * waiter is chosen for rollback in order to break the deadlock, then the
     * waiter transaction will be set cancelled and the DeadlockException will
     * be thrown.
     */
    public void waitFor(Transaction waiter, Transaction holder)
            throws DeadlockException, TransactionInterruptedException {
        waitsForTab.put(waiter, holder);

        detectDeadlock(waiter);

        synchronized (holder) {
            if (!waiter.isCancelled() && !holder.isCommitted()) {
                holder.setHasWaiters(true);
                try {
                    holder.wait();
                } catch (InterruptedException ie) {
                    throw new TransactionInterruptedException(ie);
                }
            }
        }
        if (waiter.isCancelled()) {
            throw new DeadlockException(
                    "Deadlock detected: transaction cancelled.");
        }
    }

    /**
     * Inform the DeadlockDetector that the transaction has committed so that
     * the DeadlockDetector can remove any previously registered waitsFor
     * dependencies involving the committed transaction and signal all dependent
     * waiting transactions to retry resource acquisition.
     */
    public void didCommit(Transaction trans) {
        if (!trans.hasWaiters())
            return;

        synchronized (waitsForTab) {
            waitsForTab.values().removeIf(holder -> trans == holder);
        }
        synchronized (trans) {
            trans.setHasWaiters(false);
            trans.notifyAll();
        }
    }

    private synchronized void detectDeadlock(Transaction waiter) {
        Transaction holder = waiter;
        do {
            holder = waitsForTab.get(holder);
        } while (holder != null && holder != waiter);

        if (holder == waiter)
            breakDeadlock(waiter);
    }

    private void breakDeadlock(Transaction waiter) {
        Transaction loser = waiter;
        Transaction holder = waiter;
        do {
            if (loser.getRollbackCost() > holder.getRollbackCost())
                loser = holder;
            holder = waitsForTab.get(holder);
        } while (holder != waiter);

        holder = waitsForTab.get(loser);

        synchronized (holder) {
            loser.setCancelled(true);
            holder.notifyAll();
        }
    }
}
