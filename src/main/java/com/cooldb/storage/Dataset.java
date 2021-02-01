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
import com.cooldb.api.UniqueConstraintException;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Core;
import com.cooldb.lock.Lock;
import com.cooldb.lock.Resource;
import com.cooldb.lock.ResourceLock;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.recovery.RedoException;
import com.cooldb.segment.AbstractSegmentMethod;
import com.cooldb.segment.CatalogMethod;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentDescriptor;
import com.cooldb.transaction.*;
import com.cooldb.transaction.*;

/**
 * Dataset is an implementation of StorageMethod that stores rows in the
 * database as an unordered set.
 * <p>
 * Dataset automatically grows in size as required by inserts.
 * <p>
 * Dataset supports two levels of transaction isolation, Read Committed and
 * Serializable, and combines versioning with an optimistic locking strategy to
 * permit a high level of concurrency while enforcing the transaction isolation
 * levels.
 * <p>
 * The fetch methods never acquire locks, but instead may access older versions
 * of rows consistent with the starting point of the transaction. These methods
 * never lead to a conflict.
 * <p>
 * Inserts acquire row-level locks on each inserted row and may block subsequent
 * updates to the same rows but the inserts themselves never have to wait. Only
 * updates and deletes can directly cause a lock conflict. A lock conflict
 * occurs when updates or deletes encounter a row that is currently locked by
 * another transaction.
 * <p>
 * A lock conflict can also occur when updates or deletes encounter a row that
 * is not currently locked, but which was modified by a transaction that
 * committed sometime <i>after</i> the requesting transaction began, if the
 * requesting transaction's isolation level is Serializable.
 * <p>
 * Lock conflicts are handled by either blocking the requesting transaction
 * until the current lock holder releases the lock or by raising an exception.
 * Which action is taken depends on the isolation level of the requesting
 * transaction.
 * <p>
 * If the isolation level is not Serializable (i.e., Read-Committed), then the
 * transaction will wait, otherwise an an exception is thrown indicating that
 * the transaction cannot be properly serialized. This exception is the cost of
 * the multi-version, optimistic locking concurrency control.
 * <p>
 * Since the locks are stored in the rows themselves, no limit to the number of
 * locks exists. When a transaction commits or rolls back, all acquired locks
 * are implicitly released.
 */

public class Dataset extends AbstractSegmentMethod implements StorageMethod,
        Resource {
    final DatasetDescriptor descriptor;
    DatasetPage datasetPage;
    Attachment[] attachments;
    private final ResourceLock resourceLock;
    private final FilePage tmpPage = new FilePage();

    public Dataset(Segment segment, Core core) throws DatabaseException {
        super(segment, core);
        descriptor = new DatasetDescriptor(segment.getSegmentId());
        core.getDatasetManager().select(descriptor);
        attachments = new Attachment[0];
        resourceLock = new ResourceLock(core.getDeadlockDetector());
    }

    // Resource implementation
    public Lock readLock(Transaction trans) {
        return resourceLock.readLock(trans);
    }

    public Lock writeLock(Transaction trans) {
        return resourceLock.writeLock(trans);
    }

    public Lock getLock(Transaction trans) {
        return resourceLock.getLock(trans);
    }

    // SegmentMethod implementation
    public CatalogMethod getCatalogMethod() {
        return core.getDatasetManager();
    }

    public synchronized SegmentDescriptor getDescriptor() {
        return descriptor;
    }

    public synchronized void attach(Attachment attachment) {
        boolean found = false;
        for (Attachment value : attachments) {
            if (attachment == value) {
                found = true;
                break;
            }
        }
        if (!found) {
            Attachment[] newAttachments = new Attachment[attachments.length + 1];
            System.arraycopy(attachments, 0, newAttachments, 0,
                             attachments.length);
            newAttachments[attachments.length] = attachment;
            attachments = newAttachments;
        }
    }

    public synchronized void detach(Attachment attachment) {
        boolean found = false;
        for (Attachment value : attachments) {
            if (attachment == value) {
                found = true;
                break;
            }
        }
        if (found) {
            Attachment[] newAttachments = new Attachment[attachments.length - 1];
            int j = 0;
            for (Attachment value : attachments) {
                if (attachment != value)
                    newAttachments[j++] = value;
            }
            attachments = newAttachments;
        }
    }

    public synchronized int getAttachmentCount() {
        return attachments.length;
    }

    // StorageMethod implementation
    public Rowid insert(Transaction trans, DBObject obj)
            throws DatabaseException {
        NestedTopAction savePoint = core.getTransactionManager()
                .beginNestedTopAction(trans);
        try {
            grabLock(trans);
            return _insert(trans, obj);
        } catch (UniqueConstraintException ucv) {
            core.getTransactionManager().rollbackNestedTopAction(trans,
                                                                 savePoint, core.getSegmentFactory());
            throw ucv;
        }
    }

    public boolean remove(Transaction trans, Rowid rowid, DBObject obj)
            throws DatabaseException {
        grabLock(trans);
        while (true) {
            try {
                return _remove(trans, rowid, obj);
            } catch (LockConflict lc) {
                handleLockConflict(trans, lc);
            }
        }
    }

    public void update(Transaction trans, DBObject obj, Rowid rowid,
                       DBObject old) throws DatabaseException {
        NestedTopAction savePoint = core.getTransactionManager()
                .beginNestedTopAction(trans);
        try {
            grabLock(trans);
            while (true) {
                try {
                    _update(trans, obj, rowid, old);
                    return;
                } catch (LockConflict lc) {
                    handleLockConflict(trans, lc);
                }
            }
        } catch (UniqueConstraintException ucv) {
            core.getTransactionManager().rollbackNestedTopAction(trans,
                                                                 savePoint, core.getSegmentFactory());
            throw ucv;
        }
    }

    public boolean fetch(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException {
        return fetch(trans, obj, rowid, trans.getUndoNxtLSN().getLSN(), false,
                     null);
    }

    public boolean fetchForUpdate(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException {
        return fetch(trans, obj, rowid, 0, true, null);
    }

    public boolean fetch(Transaction trans, DBObject obj, Rowid rowid,
                         long cusp, boolean forUpdate, Filter filter)
            throws DatabaseException {
        grabLock(trans);
        while (true) {
            try {
                return _fetch(trans, obj, rowid, cusp, forUpdate, filter);
            } catch (LockConflict lc) {
                handleLockConflict(trans, lc);
            }
        }
    }

    public void openCursor(Transaction trans, DatasetCursor cursor)
            throws TransactionCancelledException {
        grabLock(trans);
        /*
         * Although trans is not used here, it is required that there be an
         * active transaction context prior to opening the cursor in order for
         * the cursor to be valid
         */
        synchronized (this) {
            Rowid rowid = cursor.getRowid();
            rowid.setPage(descriptor.getSegmentId());
            rowid.setIndex((short) -1);
            cursor.setLastPage(descriptor.getLastPage());
            cursor.setCusp(trans.getUndoNxtLSN().getLSN());
        }
    }

    public void closeCursor(DatasetCursor cursor) {
    }

    public boolean fetchNext(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter) throws DatabaseException {
        return fetchNext(trans, cursor, obj, filter, false);
    }

    public boolean fetchNextForUpdate(Transaction trans, DatasetCursor cursor,
                                      DBObject obj, Filter filter) throws DatabaseException {
        return fetchNext(trans, cursor, obj, filter, true);
    }

    public boolean fetchNext(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter, boolean forUpdate)
            throws DatabaseException {
        while (true) {
            try {
                return _fetchNext(trans, cursor, obj, filter, forUpdate);
            } catch (LockConflict lc) {
                handleLockConflict(trans, lc);
            }
        }
    }

    public boolean fetchPrev(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter) throws DatabaseException {
        return fetchPrev(trans, cursor, obj, filter, false);
    }

    public boolean fetchPrevForUpdate(Transaction trans, DatasetCursor cursor,
                                      DBObject obj, Filter filter) throws DatabaseException {
        return fetchPrev(trans, cursor, obj, filter, true);
    }

    public boolean fetchPrev(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter, boolean forUpdate)
            throws DatabaseException {
        while (true) {
            try {
                return _fetchPrev(trans, cursor, obj, filter, forUpdate);
            } catch (LockConflict lc) {
                handleLockConflict(trans, lc);
            }
        }
    }

    private void grabLock(Transaction trans)
            throws TransactionCancelledException {
        // ensure the transaction has been granted access to this resource,
        // waiting if necessary
        if (getLock(trans) == null) {
            readLock(trans).lock();
        }
    }

    private synchronized boolean _fetchNext(Transaction trans,
                                            DatasetCursor cursor, DBObject obj, Filter filter, boolean forUpdate)
            throws DatabaseException, LockConflict {
        Rowid rowid = cursor.getRowid();
        DatasetPage dsp = getDatasetPage();

        while (rowid.getIndex() < Short.MAX_VALUE) {
            try {
                if (forUpdate)
                    dsp.logPin(rowid.getPage());
                else
                    dsp.versionPin(trans, rowid.getPage(), cursor.getCusp());

                if (dsp.fetchNext(trans, cursor, obj, filter, forUpdate))
                    return true;

                // Continue search on the next page
                dsp.getNextPage(rowid.getPage());
                if (rowid.getPage().isNull())
                    forward(cursor);
                else
                    rowid.setIndex((short) -1);
            } finally {
                BufferPool.Affinity affinity = rowid.getIndex() == -1 ? BufferPool.Affinity.HATED
                        : BufferPool.Affinity.LIKED;
                if (forUpdate)
                    dsp.unPin(trans, affinity);
                else
                    dsp.unPin(affinity);
            }
        }
        return false;
    }

    private synchronized boolean _fetchPrev(Transaction trans,
                                            DatasetCursor cursor, DBObject obj, Filter filter, boolean forUpdate)
            throws DatabaseException, LockConflict {
        Rowid rowid = cursor.getRowid();
        DatasetPage dsp = getDatasetPage();

        while (rowid.getIndex() > -1) {
            try {
                dsp.versionPin(trans, rowid.getPage(), cursor.getCusp());

                if (dsp.fetchPrev(trans, cursor, obj, filter, forUpdate))
                    return true;

                // Continue search on the next page
                dsp.getPrevPage(rowid.getPage());
                if (rowid.getPage().isNull())
                    rewind(cursor);
                else
                    rowid.setIndex(Short.MAX_VALUE);
            } finally {
                if (rowid.getIndex() == Short.MAX_VALUE)
                    dsp.unPin(BufferPool.Affinity.HATED);
                else
                    dsp.unPin(BufferPool.Affinity.LIKED);
            }
        }
        return false;
    }

    public void forward(DatasetCursor cursor) {
        Rowid rowid = cursor.getRowid();
        rowid.setPage(cursor.getLastPage());
        rowid.setIndex(Short.MAX_VALUE);
    }

    public void rewind(DatasetCursor cursor) {
        Rowid rowid = cursor.getRowid();
        rowid.setPage(descriptor.getSegmentId());
        rowid.setIndex((short) -1);
    }

    private synchronized boolean _fetch(Transaction trans, DBObject obj,
                                        Rowid rowid, long cusp, boolean forUpdate, Filter filter)
            throws DatabaseException, LockConflict {
        DatasetPage dsp = getDatasetPage();
        try {
            if (forUpdate)
                dsp.logPin(rowid.getPage());
            else
                dsp.versionPin(trans, rowid.getPage(), cusp);

            return dsp.select(trans, obj, rowid, forUpdate, filter);
        } finally {
            if (forUpdate)
                dsp.unPin(trans, BufferPool.Affinity.LIKED);
            else
                dsp.unPin(BufferPool.Affinity.LIKED);
        }
    }

    private synchronized Rowid _insert(Transaction trans, DBObject obj)
            throws DatabaseException {
        // Calculate tuple size
        short tsize = (short) (obj.storeSize() + RowHeader.getOverhead());

        // Find and pin a page with room to insert this tuple
        DatasetPage dsp = findRoomyPage(trans, tsize);
        try {
            // Assign the to-be-inserted object a row identifier
            Rowid rowid = new Rowid(dsp.getPage(), dsp
                    .prepareSlot(trans, tsize));

            // Write the object into the prepared slot
            dsp.insert(trans, obj, rowid, tsize, true);

            // Alert attachments
            for (Attachment attachment : attachments) attachment.didInsert(trans, obj, rowid);

            return rowid;
        } finally {
            dsp.unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    /**
     * Find a page with space enough to hold a tuple of tsize. The page is
     * returned log-pinned.
     */
    private DatasetPage findRoomyPage(Transaction trans, int tsize)
            throws DatabaseException {
        DatasetPage dsp = getDatasetPage();
        try {
            FilePage freePage;

            while (true) {
                freePage = descriptor.getFreePage();

                if (freePage.isNull()) {
                    freePage = allocateNextPage(trans);
                    dsp.logPin(freePage);

                    // TODO: implement row chaining
                    if (tsize > dsp.getMaxCapacity())
                        throw new StorageException("Tuple too large");

                    return dsp;
                } else {
                    dsp.logPin(freePage);

                    // If page cannot hold tuple, then remove page from free
                    // list.
                    // This presumes that there is little variance in the size
                    // of
                    // each data record, so that if one does not fit, then most
                    // others will also not fit.

                    if (dsp.canHold(tsize, descriptor.getLoadMax())) {
                        return dsp;
                    } else {
                        // Remove page from available pages list
                        makeUnAvailable(trans, dsp);

                        dsp.unPin(trans, BufferPool.Affinity.LIKED);
                    }
                }
            }
        } catch (DatabaseException e) {
            if (dsp != null)
                dsp.unPin(trans, BufferPool.Affinity.LIKED);
            throw e;
        }
    }

    private boolean isInFreeList(DirPage dp) {
        // A page is known to be in the free list if it either has its
        // 'nextFreePage' pointer set or is itself pointed to by the
        // descriptor 'freePage'.

        dp.getNextFreePage(tmpPage);
        return !tmpPage.isNull()
                || getDescriptor().getFreePage().equals(dp.getPage());
    }

    private void insertIntoFreeList(Transaction trans, DirPage dp)
            throws DatabaseException {
        // Push page onto the free list

        dp.updateNextFreePage(trans, getDescriptor().getFreePage());
        getDescriptor().setFreePage(dp.getPage());

        getCatalogMethod().update(trans, getDescriptor());
    }

    private void removeFromFreeList(Transaction trans, DirPage dp)
            throws DatabaseException {
        // Pop page from the free list

        dp.getNextFreePage(tmpPage);
        getDescriptor().setFreePage(tmpPage);
        dp.updateNextFreePage(trans, FilePage.NULL);

        getCatalogMethod().update(trans, getDescriptor());
    }

    /**
     * Handle a lock conflict by either waiting for the current lock holder to
     * release its lock or by raising an exception depending on the isolation
     * level of the requesting transaction.
     * <p>
     * If the isolation level is READ_COMMITTED, then wait, otherwise in the
     * case of isolation level SERIALIZABLE, throw an exception indicating that
     * the transaction cannot be properly isolated. This exception is the cost
     * of optimistic locking.
     */
    private void handleLockConflict(Transaction trans, LockConflict lc)
            throws DatabaseException {
        core.getTransactionManager().waitFor(trans, lc.getHolder());
    }

    private synchronized boolean _remove(Transaction trans, Rowid rowid,
                                         DBObject obj) throws DatabaseException, LockConflict {
        DatasetPage dsp = getDatasetPage();
        try {
            dsp.logPin(rowid.getPage());

            if (!dsp.remove(trans, rowid, obj))
                return false;

            maintainAvailability(trans, dsp);
        } finally {
            dsp.unPin(trans, BufferPool.Affinity.LIKED);
        }

        for (Attachment attachment : attachments) attachment.didRemove(trans, rowid, obj);

        return true;
    }

    private synchronized void _update(Transaction trans, DBObject obj,
                                      Rowid rowid, DBObject old) throws DatabaseException, LockConflict {
        DatasetPage dsp = getDatasetPage();
        try {
            dsp.logPin(rowid.getPage());

            dsp.update(trans, obj, rowid, old);
        } finally {
            dsp.unPin(trans, BufferPool.Affinity.LIKED);
        }

        for (Attachment attachment : attachments) attachment.didUpdate(trans, obj, rowid, old);
    }

    // SpaceDelegate implementation
    public synchronized void didAllocatePage(Transaction trans, FilePage page)
            throws DatabaseException {
        FilePage lastPage = descriptor.getLastPage();

        // initialize the new page, flush immediately
        DatasetPage dsp = getDatasetPage();
        try {
            dsp.writePin(page, true);
            dsp.create(FilePage.NULL, lastPage, descriptor.getFreePage());
        } finally {
            dsp.unPin(BufferPool.Affinity.LIKED);
        }

        // add the new page to the dataset by setting the last page's nextPage
        // pointer
        // to the new page
        if (!lastPage.isNull()) {
            try {
                dsp.logPin(lastPage);
                dsp.updateNextPage(trans, page);
            } finally {
                dsp.unPin(trans, BufferPool.Affinity.HATED);
            }
        }

        // update the dataset descriptor
        descriptor.setFreePage(page);
        descriptor.setLastPage(page);
        getCatalogMethod().update(trans, descriptor);
    }

    // RecoveryDelegate implementation
    public synchronized void redo(RedoLog log) throws RedoException {
        getDatasetPage().redo(log);
    }

    public synchronized void undo(UndoLog log, Transaction trans)
            throws RollbackException {
        DatasetPage dsp = getDatasetPage();
        try {
            try {
                dsp.undoPin(log);
                dsp.undo(log, trans);
                maintainAvailability(trans, dsp);
            } finally {
                dsp.unPin(trans, BufferPool.Affinity.LIKED);
            }
        } catch (Exception e) {
            throw new RollbackException("Undo failed", e);
        }
    }

    public synchronized void undo(UndoLog log, PageBuffer pageBuffer)
            throws RollbackException {
        getDatasetPage().undo(log, pageBuffer);
    }

    private DatasetPage getDatasetPage() {
        if (datasetPage == null)
            datasetPage = new DatasetPage(core.getTransactionManager(),
                                          allocPageBroker());
        return datasetPage;
    }

    private void maintainAvailability(Transaction trans, DatasetPage dsp)
            throws DatabaseException {
        byte load = dsp.loadFactor();
        if (load <= descriptor.getLoadMin() && !isInFreeList(dsp)) {
            // If the page's loadFactor drops below loadMin, add it
            // to the freeList of pages so that it becomes available
            // for new inserts (unless it is already in the list).
            makeAvailable(trans, dsp);
        }
    }

    /**
     * Add page to free list to make it available for more inserts.
     */
    private void makeAvailable(Transaction trans, DirPage dp)
            throws DatabaseException {
        // make sure this action is atomic
        TransactionManager tm = core.getTransactionManager();
        NestedTopAction savePoint = tm.beginNestedTopAction(trans);
        try {
            insertIntoFreeList(trans, dp);
            tm.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            tm.rollbackNestedTopAction(trans, savePoint, core
                    .getSegmentFactory());
            throw new DatabaseException(
                    "Failed to remove page from the free list.", e);
        }
    }

    /**
     * Remove page from free list to prevent more inserts.
     */
    private void makeUnAvailable(Transaction trans, DirPage dp)
            throws DatabaseException {
        // make sure this action is atomic
        TransactionManager tm = core.getTransactionManager();
        NestedTopAction savePoint = tm.beginNestedTopAction(trans);
        try {
            removeFromFreeList(trans, dp);
            tm.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            tm.rollbackNestedTopAction(trans, savePoint, core
                    .getSegmentFactory());
            throw new DatabaseException(
                    "Failed to remove page from the free list.", e);
        }
    }
}
