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

import com.cooldb.access.BTreePredicate;
import com.cooldb.access.TreeCursor;
import com.cooldb.api.*;
import com.cooldb.buffer.FilePage;
import com.cooldb.storage.Rowid;
import com.cooldb.transaction.Transaction;

public class IndexCursorImpl implements IndexCursor {

    private final IndexImpl index;
    private final Key lowerKey;
    private final Key upperKey;
    private final BTreePredicate pred;
    private final TreeCursor cursor;
    private Rowid rowid;
    private int lowerOp;
    private int upperOp;
    private boolean isOpen;
    private boolean isForUpdate;

    IndexCursorImpl(IndexImpl index) throws DatabaseException {
        this.index = index;
        lowerKey = index.allocateKey();
        upperKey = index.allocateKey();
        pred = new BTreePredicate(lowerKey, upperKey);
        try {
            cursor = (TreeCursor) index.getTree().allocateCursor();
        } catch (Exception e) {
            throw new DatabaseException("Allocate index cursor failed", e);
        }
        lowerKey.setMinValue();
        upperKey.setMaxValue();
        lowerOp = BTreePredicate.GTE;
        upperOp = BTreePredicate.LTE;
    }

    @Override
    public Row allocateRow() {
        return index.table.allocateRow();
    }

    @Override
    public void open() throws DatabaseException {
        index.getTree().openCursor(index.table.session.getTransaction(), cursor);
        isOpen = true;
        rowid = null;
    }

    @Override
    public void close() {
        rowid = null;
        isOpen = false;
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
    public Key lowerKey() {
        return lowerKey;
    }

    @Override
    public Key upperKey() {
        return upperKey;
    }

    @Override
    public void setInclusive(boolean lower, boolean upper) {
        this.lowerOp = lower ? BTreePredicate.GTE : BTreePredicate.GT;
        this.upperOp = upper ? BTreePredicate.LTE : BTreePredicate.LT;
    }

    @Override
    public boolean fetchNext(Row row) throws DatabaseException {
        return _fetchNext(row, null, false);
    }

    @Override
    public boolean fetchNext(Row row, Filter filter) throws DatabaseException {
        return _fetchNext(row, filter, false);
    }

    @Override
    public boolean fetchPrev(Row row) throws DatabaseException {
        return _fetchNext(row, null, true);
    }

    @Override
    public boolean fetchPrev(Row row, Filter filter) throws DatabaseException {
        return _fetchNext(row, filter, true);
    }

    @Override
    public void rewind() {
        cursor.setLeaf(FilePage.NULL);
    }

    @Override
    public void forward() {
        cursor.setLeaf(FilePage.NULL);
    }

    @Override
    public void removeCurrent() throws DatabaseException {
        if (rowid == null)
            throw new DatabaseException(
                    "The cursor does not currently point to a row.  Use one of the fetch methods first.");

        index.table.remove(rowid);
    }

    @Override
    public void updateCurrent(Row row) throws DatabaseException {
        if (rowid == null)
            throw new DatabaseException(
                    "The cursor does not currently point to a row.  Use one of the fetch methods first.");

        index.table.update(rowid, row);
    }

    @Override
    public RID getCurrentRID() throws DatabaseException {
        if (rowid == null)
            throw new DatabaseException(
                    "The cursor does not currently point to a row.  Use one of the fetch methods first.");

        return (RID) rowid.copy();
    }

    private boolean _fetchNext(Row row, Filter filter, boolean reverse)
            throws DatabaseException {
        if (((RowImpl) row).table != index.table)
            throw new DatabaseException(
                    "Row does not belong to the indexed table.");

        if (!isOpen)
            open();

        try {
            Transaction trans = index.table.session.getTransaction();
            do {
                if (cursor.getLeaf().isNull()) {
                    // Establish bounds
                    if (reverse)
                        pred.setRange(upperKey, upperOp, lowerKey, lowerOp);
                    else
                        pred.setRange(lowerKey, lowerOp, upperKey, upperOp);

                    // Start the scan
                    rowid = (Rowid) index.getTree().findFirst(trans, pred,
                                                              cursor, reverse);
                } else
                    rowid = (Rowid) index.getTree().findNext(trans, pred,
                                                             cursor, reverse);

                if (rowid == null)
                    return false;
            } while (!index.table.getDataset().fetch(trans, row, rowid,
                                                     cursor.getCusp(), isForUpdate, filter));

            return true;
        } catch (Exception e) {
            throw new DatabaseException("Fetch Next failed", e);
        }
    }
}
