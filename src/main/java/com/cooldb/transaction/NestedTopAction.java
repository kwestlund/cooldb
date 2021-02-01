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

/**
 * NestedTopAction records the information necessary for a transaction to commit
 * or rollback to the point at which this NestedTopAction was created.
 */
public class NestedTopAction {
    private final UndoPointer undoNxtLSN;
    private final Lock undoNxtLock;

    public NestedTopAction(UndoPointer undoNxtLSN, Lock undoNxtLock) {
        this.undoNxtLSN = undoNxtLSN;
        this.undoNxtLock = undoNxtLock;
    }

    public UndoPointer getUndoNxtLSN() {
        return undoNxtLSN;
    }

    public Lock getUndoNxtLock() {
        return undoNxtLock;
    }
}
