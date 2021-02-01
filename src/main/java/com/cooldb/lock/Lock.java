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

import com.cooldb.transaction.TransactionCancelledException;

/**
 * Lock synchronizes transaction access to some resource.
 */
public interface Lock {
    /**
     * Grabs the lock, waiting if necessary for conflicting locks to be
     * released.
     *
     * @throws TransactionCancelledException if the transaction is cancelled
     */
    void lock() throws TransactionCancelledException;

    /**
     * Grabs the lock immediately and returns true if no conflict exists,
     * otherwise returns false without acquiring the lock and without waiting.
     *
     * @throws TransactionCancelledException if the transaction is cancelled
     */
    boolean trylock() throws TransactionCancelledException;

    /**
     * Releases the lock.
     */
    void unlock();

    /**
     * Returns true if this is an exclusive lock.
     */
    boolean isExclusive();
}
