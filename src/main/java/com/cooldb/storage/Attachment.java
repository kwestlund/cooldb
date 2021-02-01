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

package com.cooldb.storage;

import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.DBObject;
import com.cooldb.transaction.Transaction;

/**
 * Attachment defines a protocol that allows objects attached to a Dataset to
 * receive alerts after insert, remove, and update operations are performed on
 * the dataset.
 */

public interface Attachment {
    void didInsert(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException;

    void didRemove(Transaction trans, Rowid rowid, DBObject obj)
            throws DatabaseException;

    void didUpdate(Transaction trans, DBObject obj, Rowid rowid, DBObject old)
            throws DatabaseException;
}
