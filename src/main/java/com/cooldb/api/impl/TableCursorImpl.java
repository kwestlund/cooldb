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

package com.cooldb.api.impl;

import com.cooldb.api.*;
import com.cooldb.storage.DatasetCursor;

public class TableCursorImpl implements TableCursor {

    private final TableImpl table;
    private final DatasetCursor cursor;
    private boolean isOpen;
    private boolean isCurrent;
    private boolean isForUpdate;

    TableCursorImpl(TableImpl table) {
        this.table = table;
        this.cursor = new DatasetCursor();
    }

    @Override
    public Row allocateRow() {
        return table.allocateRow();
    }

    @Override
    public void open() throws DatabaseException {
        table.getDataset().openCursor(table.session.getTransaction(), cursor);
        isOpen = true;
        isCurrent = false;
    }

    @Override
    public void close() throws DatabaseException {
        isCurrent = false;
        isOpen = false;
        table.getDataset().closeCursor(cursor);
    }

    @Override
    public boolean isForUpdate() {
        return isForUpdate;
    }

    @Override
    public void setForUpdate(boolean isForUpdate) {
        this.isForUpdate = isForUpdate;
    }

    @Override
    public boolean fetchNext(Row row) throws DatabaseException {
        if (((RowImpl) row).table != table)
            throw new DatabaseException("Row does not belong to this table.");

        if (!isOpen)
            open();

        try {
            if (isForUpdate)
                return (isCurrent = table.getDataset().fetchNextForUpdate(
                        table.session.getTransaction(), cursor,
                        row, null));
            else
                return (isCurrent = table.getDataset().fetchNext(
                        table.session.getTransaction(), cursor,
                        row, null));
        } catch (Exception e) {
            throw new DatabaseException("Fetch Next failed", e);
        }
    }

    @Override
    public boolean fetchNext(Row row, Filter filter) throws DatabaseException {
        if (((RowImpl) row).table != table)
            throw new DatabaseException("Row does not belong to this table.");

        if (!isOpen)
            open();

        try {
            if (isForUpdate)
                return (isCurrent = table.getDataset().fetchNextForUpdate(
                        table.session.getTransaction(), cursor,
                        row, filter));
            else
                return (isCurrent = table.getDataset().fetchNext(
                        table.session.getTransaction(), cursor,
                        row, filter));
        } catch (Exception e) {
            throw new DatabaseException("Fetch Next failed", e);
        }
    }

    @Override
    public boolean fetchPrev(Row row, Filter filter) throws DatabaseException {
        if (((RowImpl) row).table != table)
            throw new DatabaseException("Row does not belong to this table.");

        if (!isOpen)
            open();

        try {
            if (isForUpdate)
                return (isCurrent = table.getDataset().fetchPrevForUpdate(
                        table.session.getTransaction(), cursor,
                        row, filter));
            else
                return (isCurrent = table.getDataset().fetchPrev(
                        table.session.getTransaction(), cursor,
                        row, filter));
        } catch (Exception e) {
            throw new DatabaseException("Fetch Previous failed", e);
        }
    }

    @Override
    public boolean fetchPrev(Row row) throws DatabaseException {
        if (((RowImpl) row).table != table)
            throw new DatabaseException("Row does not belong to this table.");

        if (!isOpen)
            open();

        try {
            if (isForUpdate)
                return (isCurrent = table.getDataset().fetchPrevForUpdate(
                        table.session.getTransaction(), cursor,
                        row, null));
            else
                return (isCurrent = table.getDataset().fetchPrev(
                        table.session.getTransaction(), cursor,
                        row, null));
        } catch (Exception e) {
            throw new DatabaseException("Fetch Previous failed", e);
        }
    }

    @Override
    public void rewind() throws DatabaseException {
        if (!isOpen)
            open();

        table.getDataset().rewind(cursor);

        isCurrent = false;
    }

    @Override
    public void forward() throws DatabaseException {
        if (!isOpen)
            open();

        table.getDataset().forward(cursor);

        isCurrent = false;
    }

    @Override
    public void removeCurrent() throws DatabaseException {
        if (!isCurrent)
            throw new DatabaseException(
                    "The cursor does not currently point to a row.  Use one of the fetch methods first.");

        table.remove(cursor.getRowid());
    }

    @Override
    public void updateCurrent(Row row) throws DatabaseException {
        if (!isCurrent)
            throw new DatabaseException(
                    "The cursor does not currently point to a row.  Use one of the fetch methods first.");

        table.update(cursor.getRowid(), row);
    }

    @Override
    public RID getCurrentRID() throws DatabaseException {
        if (!isCurrent)
            throw new DatabaseException(
                    "The cursor does not currently point to a row.  Use one of the fetch methods first.");

        return (RID) cursor.getRowid().copy();
    }
}
