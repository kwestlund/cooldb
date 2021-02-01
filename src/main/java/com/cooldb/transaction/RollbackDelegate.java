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

import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.UndoLog;

public interface RollbackDelegate {
    /**
     * Rollback the update specified by the given log record associated with the
     * given transaction.
     * <p>
     * Write a redo CLR record describing how to redo the undo, and an undo CLR
     * record with undoNxtLSN set to skip over this log record (by pointing to
     * this log record's undoNxtLSN). <br>
     * Set the pageUndoNxtLSN in the buffer to that of the log record.
     */
    void undo(UndoLog log, Transaction trans) throws RollbackException;

    /**
     * Rollback the update specified by 'log' in the given page buffer.
     * <p>
     * Set the pageUndoNxtLSN in the buffer to that of the log record. <br>
     * Do not write a CLR log record for this rollback, as it is meant solely
     * for use in reconstructing older versions of pages.
     * <p>
     * Note: the supplied PageBuffer must be pinned with the relevant page in
     * mode EXCLUSIVE.
     */
    void undo(UndoLog log, PageBuffer pb) throws RollbackException;
}
