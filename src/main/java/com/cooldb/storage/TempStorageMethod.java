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

import com.cooldb.buffer.FilePage;
import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Core;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.recovery.RedoException;
import com.cooldb.segment.AbstractSegmentMethod;
import com.cooldb.segment.CatalogMethod;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentDescriptor;
import com.cooldb.transaction.RollbackException;
import com.cooldb.transaction.Transaction;

/**
 * TempStorageMethod is a limited implementation of StorageMethod that stores
 * rows in the database without logging the inserts. TempStorageMethod does not
 * permit deletes or updates and exists exclusively for the single transaction
 * that creates it.
 */

public class TempStorageMethod extends AbstractSegmentMethod implements
        StorageMethod {
    final DatasetDescriptor descriptor;
    TempPage tempPage;

    public TempStorageMethod(Segment segment, Core core) throws DatabaseException {
        super(segment, core);
        descriptor = new DatasetDescriptor(segment.getSegmentId());
        core.getDatasetManager().select(descriptor);
    }

    // SegmentMethod implementation
    public CatalogMethod getCatalogMethod() {
        return core.getDatasetManager();
    }

    public SegmentDescriptor getDescriptor() {
        return descriptor;
    }

    // Allocate page from the extent without logging the allocation
    @Override
    public FilePage allocateNextPage(Transaction trans) throws DatabaseException {
        if (segment.getNewExtent().isNull()
                || segment.getNextPage() == segment.getNewExtent()
                .getEndPageId())
            core.getSpaceManager().allocateNextExtent(trans, segment);
        FilePage newPage = new FilePage(segment.getNewExtent());
        newPage.setPageId(segment.getNextPage());

        // remove page from segment by incrementing page pointer
        segment.setNextPage(segment.getNextPage() + 1);

        didAllocatePage(trans, newPage);

        return newPage;
    }

    // StorageMethod implementation
    public Rowid insert(Transaction trans, DBObject obj) throws DatabaseException {
        if (tempPage == null || !tempPage.canHold(obj.storeSize())) {
            allocateNextPage(trans);
        }

        // Insert into currently active page
        tempPage.insert(obj);

        return null;
    }

    public boolean remove(Transaction trans, Rowid rowid, DBObject obj)
            throws DatabaseException, InterruptedException {
        throw new DatabaseException("remove method not supported");
    }

    public void update(Transaction trans, DBObject obj, Rowid rowid,
                       DBObject old) throws DatabaseException, InterruptedException {
        throw new DatabaseException("update method not supported");
    }

    public boolean fetch(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException {
        throw new DatabaseException("fetch method not supported");
    }

    public boolean fetchForUpdate(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException {
        throw new DatabaseException("fetchForUpdate method not supported");
    }

    public void openCursor(Transaction trans, DatasetCursor cursor)
            throws DatabaseException {
        rewind(cursor);
    }

    public void closeCursor(DatasetCursor cursor) throws DatabaseException {
        tempPage.unPin(BufferPool.Affinity.HATED);
    }

    public boolean fetchNext(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter) throws DatabaseException {
        while (true) {
            if (tempPage.fetchNext(obj, filter))
                return true;

            // Continue search on the next page
            FilePage next = cursor.getRowid().getPage();
            tempPage.getNextPage(next);
            tempPage.unPin(BufferPool.Affinity.HATED);
            if (next.isNull())
                return false;
            else
                tempPage.readPin(next);
        }
    }

    public boolean fetchNextForUpdate(Transaction trans, DatasetCursor cursor,
                                      DBObject obj, Filter filter) throws DatabaseException {
        throw new DatabaseException("fetchNextForUpdate method not supported");
    }

    public boolean fetchPrev(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter) throws DatabaseException {
        throw new DatabaseException("fetchPrev method not supported");
    }

    public boolean fetchPrevForUpdate(Transaction trans, DatasetCursor cursor,
                                      DBObject obj, Filter filter) throws DatabaseException {
        throw new DatabaseException("fetchPrevForUpdate method not supported");
    }

    public void forward(DatasetCursor cursor) throws DatabaseException {
        throw new DatabaseException("forward method not supported");
    }

    public void rewind(DatasetCursor cursor) throws DatabaseException {
        tempPage.unPin(BufferPool.Affinity.HATED);
        tempPage.readPin(descriptor.getSegmentId());
    }

    // SpaceDelegate implementation
    public void didAllocatePage(Transaction trans, FilePage page)
            throws DatabaseException {
        // Set the previous page's nextPage pointer to the new page and unPin
        // the previous page
        if (tempPage == null) {
            tempPage = new TempPage(allocPageBroker());
        } else {
            tempPage.setNextPage(page);
            tempPage.unPin(BufferPool.Affinity.HATED);
        }

        // initialize the new page pinned for temporary writing without logging
        tempPage.tempPin(page, trans);
        tempPage.create(FilePage.NULL, descriptor.getLastPage());

        // update the dataset descriptor in-memory only
        descriptor.setLastPage(page);
    }

    // RecoveryDelegate implementation
    public void redo(RedoLog log) throws RedoException {
    }

    public void undo(UndoLog log, Transaction trans) throws RollbackException {
    }

    public void undo(UndoLog log, PageBuffer pageBuffer)
            throws RollbackException {
    }
}
