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

import com.cooldb.transaction.Transaction;

/**
 * Resource is a type of read-write lock that allows a resource to control
 * access to it by concurrent transactions.
 */
public interface Resource {
    /**
     * Returns a Lock associated with the given transaction. The returned Lock
     * can be locked to grant the transaction shared access to this Resource.
     *
     * @param trans The transaction requesting access to this resource
     * @return The share Lock associated with this resource and the requesting
     * transaction
     */
    Lock readLock(Transaction trans);

    /**
     * Returns a Lock associated with the given transaction. The returned Lock
     * can be locked to grant the transaction exclusive access to this Resource.
     *
     * @param trans The transaction requesting access to this resource
     * @return The exclusive Lock associated with this resource and the
     * requesting transaction
     */
    Lock writeLock(Transaction trans);

    /**
     * Returns the lock held by the given transaction, if there is one, null
     * otherwise.  Note: returns only granted locks.
     *
     * @param trans The transaction holding the lock on this resource
     * @return The lock held by the transaction if there is one, null otherwise
     */
    Lock getLock(Transaction trans);
}
