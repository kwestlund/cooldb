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
import com.cooldb.api.Filter;
import com.cooldb.api.Row;
import com.cooldb.api.RowStream;
import com.cooldb.api.impl.RowImpl;
import com.cooldb.transaction.Transaction;

/**
 * Cursor for storage methods.
 */

public class StorageMethodIterator implements RowStream {
    protected final StorageMethod sm;
    protected final Transaction trans;
    protected DatasetCursor cursor;
    protected final Row proto;
    protected boolean isOpen;

    public StorageMethodIterator(StorageMethod sm, Transaction trans, Row row) {
        this.sm = sm;
        this.trans = trans;
        this.proto = row;
    }

    @Override
    public Row allocateRow() {
        return (RowImpl) proto.copy();
    }

    @Override
    public void open() throws DatabaseException {
        if (cursor == null)
            cursor = new DatasetCursor();
        try {
            sm.openCursor(trans, cursor);
        } catch (Exception e) {
            throw new DatabaseException("open failed", e);
        }
        isOpen = true;
    }

    @Override
    public void close() throws DatabaseException {
        isOpen = false;
        try {
            sm.closeCursor(cursor);
        } catch (Exception e) {
            throw new DatabaseException("open failed", e);
        }
    }

    @Override
    public boolean fetchNext(Row row) throws DatabaseException {
        if (!isOpen)
            open();

        try {
            return sm.fetchNext(trans, cursor, row, null);
        } catch (Exception e) {
            throw new DatabaseException("Fetch Next failed", e);
        }
    }

    @Override
    public boolean fetchNext(Row row, Filter filter) throws DatabaseException {
        if (!isOpen)
            open();

        try {
            return sm.fetchNext(trans, cursor, row, filter);
        } catch (Exception e) {
            throw new DatabaseException("Fetch Next failed", e);
        }
    }

    @Override
    public void rewind() throws DatabaseException {
        if (!isOpen)
            open();

        try {
            sm.rewind(cursor);
        } catch (Exception e) {
            throw new DatabaseException("Fetch Next failed", e);
        }
    }
}
