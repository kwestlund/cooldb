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

import com.cooldb.transaction.TransactionManager;
import com.cooldb.api.Filter;
import com.cooldb.buffer.DBObject;
import com.cooldb.log.LogData;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.segment.PageBroker;
import com.cooldb.transaction.Transaction;

import java.nio.ByteBuffer;

public class DatasetPage extends DirPage {

    // log data types
    private static final byte REDO_INSERT = NEXT_LOGDATA_TYPE;
    private static final byte REDO_REMOVE = NEXT_LOGDATA_TYPE + 1;
    private static final byte REDO_UPDATE = NEXT_LOGDATA_TYPE + 2;
    private static final byte UNDO_INSERT = NEXT_LOGDATA_TYPE + 3;
    private static final byte UNDO_REMOVE = NEXT_LOGDATA_TYPE + 4;
    private static final byte UNDO_UPDATE = NEXT_LOGDATA_TYPE + 5;
    private static final byte UNDO_LOCK = NEXT_LOGDATA_TYPE + 6;
    private static final byte REDO_LOCK = NEXT_LOGDATA_TYPE + 7;

    public DatasetPage(TransactionManager transactionManager,
                       PageBroker pageBroker) {
        super(transactionManager, pageBroker);
    }

    /**
     * Insert the tuple into the Dataset and assign the tuple a unique
     * identifier.
     */
    public void insert(Transaction trans, DBObject obj, Rowid rowid, int tsize,
                       boolean logging) {
        short tid = rowid.getIndex();

        // Write UNDO info, which is just the tid, needed to 'remove' the tuple
        if (logging) {
            attachUndo(UNDO_INSERT, 2).getData().putShort(tid);
        }

        // write the object into the slot
        short loc = dir.loc(tid);
        pageBuffer.put(loc + RowHeader.getOverhead(), obj);

        // set deleted false (NOTE: this must happen BEFORE the REDO log below)
        setDeleted(tid, false);

        // Write REDO info, which is the inserted row and its new index id
        if (logging) {
            ByteBuffer bb = attachRedo(REDO_INSERT, tsize + 2).getData();
            bb.putShort(tid);
            pageBuffer.get(loc, bb.array(), 2, tsize);
        }
    }

    /**
     * Delete the row identified by the specified index from the Dataset. Return
     * true if successful, false if there is no such row.
     */
    public boolean remove(Transaction trans, Rowid rowid, DBObject obj)
            throws StorageException, LockConflict {
        // TODO: remove all associated overflow links as well
        short index = rowid.getIndex();

        if (index < 0 || index >= dir.getCount())
            return false;

        // Acquire X-lock on to-be-removed tuple.
        long previousHolder = lock(trans, index);

        // Get the location of the row on this page
        int loc = dir.loc(index);

        if (RowHeader.isDeleted(pageBuffer, loc)) {
            // Replace the lock before aborting
            if (previousHolder == 0)
                RowHeader.setLocked(pageBuffer, loc, false);
            else
                RowHeader.setLockHolder(pageBuffer, loc, previousHolder);

            return false;
        }

        // Write UNDO info, which is the index needed to toggle the action plus
        // the previous lock holder
        attachUndo(UNDO_REMOVE, 10).getData().putShort(index).putLong(
                previousHolder);

        // Write REDO info, which is the index needed to toggle the action
        attachRedo(REDO_REMOVE, 2).getData().putShort(index);

        // Set the row status to DELETED
        setDeleted(index, true);

        // Return the deleted object
        pageBuffer.get(loc + RowHeader.getOverhead(), obj);

        return true;
    }

    /**
     * Replace the value of the tuple stored in the Dataset. The given tuple
     * must contain the Rowid of the row to be replaced.
     */
    public void update(Transaction trans, DBObject obj, Rowid rowid,
                       DBObject old) throws StorageException, LockConflict {
        // TODO: update any associated overflow links as well
        short index = rowid.getIndex();

        if (index < 0 || index >= dir.getCount())
            throw new StorageException("Update failed: row not found");

        // Acquire X-lock on to-be-updated tuple.
        long previousHolder = lock(trans, index);

        // Get the location of the row on this page
        int loc = dir.loc(index);

        if (RowHeader.isDeleted(pageBuffer, loc)) {
            // Replace the lock before aborting
            if (previousHolder == 0)
                RowHeader.setLocked(pageBuffer, loc, false);
            else
                RowHeader.setLockHolder(pageBuffer, loc, previousHolder);

            throw new StorageException("Update failed: row is deleted");
        }

        // Return the old object
        pageBuffer.get(loc + RowHeader.getOverhead(), old);

        // Calculate new tuple size
        int tsize = obj.storeSize() + RowHeader.getOverhead();

        // Get existing tuple size
        int rsize = dir.sizeAt(index);

        // Write UNDO info, which is the existing row, its index identifier, and
        // the previous lock holder
        ByteBuffer bb = attachUndo(UNDO_UPDATE, rsize + 10).getData();
        bb.putShort(index).putLong(previousHolder);
        pageBuffer.get(loc, bb.array(), 10, rsize);

        loc -= prepareUpdate(index, loc, tsize, rsize);

        // Write the new tuple
        pageBuffer.put(loc + RowHeader.getOverhead(), obj);

        // Write REDO info, which is the new row and its index identifier
        bb = attachRedo(REDO_UPDATE, tsize + 2).getData();
        bb.putShort(index);
        pageBuffer.get(loc, bb.array(), 2, tsize);
    }

    /**
     * Read the stored value of the tuple from this page. The given tuple must
     * contain the Rowid of the stored object. Return true if found, false
     * otherwise.
     */
    public boolean select(Transaction trans, DBObject obj, Rowid rowid,
                          boolean forUpdate, Filter filter) throws StorageException,
            LockConflict {
        int index = rowid.getIndex();

        if (index < 0 || index >= dir.getCount())
            return false;

        return _read(trans, obj, index, forUpdate, filter);
    }

    /**
     * Read the next value of the tuple from this page that passes the filter.
     */
    public boolean fetchNext(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter, boolean forUpdate)
            throws StorageException, LockConflict {
        Rowid rowid = cursor.getRowid();
        int index = rowid.getIndex();

        while (true) {
            ++index;

            if (index < 0 || index >= dir.getCount())
                return false;

            rowid.setIndex((short) index);

            if (_read(trans, obj, index, forUpdate, filter))
                return true;
        }
    }

    /**
     * Read the next value of the tuple from this page that passes the filter in
     * a backward scan.
     */
    public boolean fetchPrev(Transaction trans, DatasetCursor cursor,
                             DBObject obj, Filter filter, boolean forUpdate)
            throws StorageException, LockConflict {
        Rowid rowid = cursor.getRowid();
        int index = rowid.getIndex();

        if (index == Short.MAX_VALUE)
            index = dir.getCount();

        while (true) {
            --index;

            if (index < 0 || index >= dir.getCount())
                return false;

            rowid.setIndex((short) index);

            if (_read(trans, obj, index, forUpdate, filter))
                return true;
        }
    }

    private boolean _read(Transaction trans, DBObject obj, int index,
                          boolean forUpdate, Filter filter) throws StorageException,
            LockConflict {
        // Acquire X-lock if For Update
        long previousHolder = 0;
        if (forUpdate)
            previousHolder = lock(trans, index);

        // Get the location of the row on this page
        int loc = dir.loc(index);

        if (!RowHeader.isDeleted(pageBuffer, loc)) {
            // Read the tuple
            pageBuffer.get(loc + RowHeader.getOverhead(), obj);

            if (filter == null || filter.passes(obj)) {
                if (forUpdate) {
                    // log the lock (needed for multi-versioning undo purposes)
                    attachUndo(UNDO_LOCK, 10).getData().putShort((short) index)
                            .putLong(previousHolder);
                    attachRedo(REDO_LOCK, 10).getData().putShort((short) index)
                            .putLong(trans.getTransId());
                }
                return true;
            }
        }

        if (forUpdate) {
            // Replace the lock before returning false
            if (previousHolder == 0)
                RowHeader.setLocked(pageBuffer, loc, false);
            else
                RowHeader.setLockHolder(pageBuffer, loc, previousHolder);
        }

        return false;
    }

    // DirPage implementation
    @Override
    protected void redo(RedoLog log, LogData entry) throws StorageException {
        switch (entry.getType()) {
            case REDO_INSERT:
                redoInsert(entry.getData());
                break;

            case REDO_REMOVE:
                redoRemove(entry.getData());
                break;

            case REDO_UPDATE:
                redoUpdate(entry.getData());
                break;

            case REDO_LOCK:
                redoLock(entry.getData());
                break;

            default:
                break;
        }
    }

    @Override
    protected void undo(UndoLog log, LogData entry, boolean writeCLR)
            throws StorageException {
        switch (entry.getType()) {
            case UNDO_INSERT:
                if (writeCLR)
                    attachCLR(entry);
                else if (!checkLock(log))
                    return;
                undoInsert(entry.getData());
                break;

            case UNDO_REMOVE:
                if (writeCLR)
                    attachCLR(entry);
                undoRemove(entry.getData());
                break;

            case UNDO_UPDATE:
                if (writeCLR)
                    attachCLR(entry);
                else if (!checkLock(log))
                    return;
                undoUpdate(entry.getData());
                break;

            case UNDO_LOCK:
                if (writeCLR)
                    attachCLR(entry);
                else if (!checkLock(log))
                    return;
                redoLock(entry.getData());
                break;

            default:
                break;
        }
    }

    @Override
    protected void redoCLR(RedoLog clr, LogData entry) throws StorageException {
        switch (entry.getType()) {
            case UNDO_INSERT:
                undoInsert(entry.getData());
                break;

            case UNDO_REMOVE:
                undoRemove(entry.getData());
                break;

            case UNDO_UPDATE:
                undoUpdate(entry.getData());
                break;

            case UNDO_LOCK:
                redoLock(entry.getData());
                break;

            default:
                break;
        }
    }

    /**
     * Find an existing slot to reuse or create a new slot, then prepare, lock,
     * and return the slot with exactly the requested amount of space, initially
     * marked as deleted.
     */
    public short prepareSlot(Transaction trans, int tsize)
            throws StorageException {
        int slotId = -1;

        // look for a slot to reuse
        int deleteCount = getDeleteCount();
        if (deleteCount > 0) {
            int count = dir.getCount();
            for (int i = 0; i < count; i++) {
                int loc = dir.loc(i);
                if (RowHeader.isDeleted(pageBuffer, loc)
                        && !RowHeader.isLocked(pageBuffer, loc)) {
                    int rsize = dir.sizeAt(i) - RowHeader.getOverhead();
                    if (rsize > 0)
                        setDeleteCount((short) (getDeleteCount() - 1));
                    dir.replaceAt(i, tsize);
                    slotId = i;
                    break;
                }
            }
        }

        // or add a slot
        if (slotId == -1)
            slotId = dir.push(tsize);

        // initialize the slot
        int loc = dir.loc(slotId);
        RowHeader.create(pageBuffer, loc);

        // mark it as deleted until the actual insert takes place (if it takes
        // place)
        setDeleted((short) slotId, true);

        // lock the slot
        try {
            lock(trans, slotId);
        } catch (LockConflict lc) {
            // should not be possible
            throw new RuntimeException(
                    "Internal error: unexpected exception in attempt to acquire lock");
        }

        return (short) slotId;
    }

    private void redoInsert(ByteBuffer bb) {
        short index = bb.getShort();
        int tsize = bb.remaining();
        int count = dir.getCount();

        if (index < count)
            dir.replaceAt(index, tsize);
        else
            dir.push(tsize);

        short loc = dir.loc(index);
        pageBuffer.put(loc, bb);
    }

    private void undoInsert(ByteBuffer bb) {
        redoRemove(bb);
    }

    private void redoRemove(ByteBuffer bb) {
        short index = bb.getShort();
        int loc = setDeleted(index, true);

        // make sure the row is not locked after the redo
        RowHeader.setLocked(pageBuffer, loc, false);
    }

    private void undoRemove(ByteBuffer bb) {
        short index = bb.getShort();
        long previousHolder = bb.getLong();
        int loc = setDeleted(index, false);

        // make sure the row is locked by the previous holder after undo
        if (previousHolder > 0
                && !transactionManager.isUniversallyCommitted(previousHolder)) {
            RowHeader.setLockHolder(pageBuffer, loc, previousHolder);
            RowHeader.setLocked(pageBuffer, loc, true);
        } else
            RowHeader.setLocked(pageBuffer, loc, false);
    }

    private void redoUpdate(ByteBuffer bb) {
        int index = bb.getShort();
        int tsize = bb.remaining();
        int rsize = dir.sizeAt(index);

        // Overwrite the existing row, making room for it if necessary
        if (tsize - rsize > 0)
            dir.replaceAt(index, tsize);

        int loc = dir.loc(index);

        // Write the new tuple
        pageBuffer.put(loc, bb);

        // make sure the row is not locked after the redo
        RowHeader.setLocked(pageBuffer, loc, false);
    }

    private void undoUpdate(ByteBuffer bb) throws StorageException {
        short index = bb.getShort();
        long previousHolder = bb.getLong();
        int loc = dir.loc(index);
        int rsize = dir.sizeAt(index);
        int tsize = bb.remaining();

        loc -= prepareUpdate(index, loc, tsize, rsize);

        // Write the old tuple
        pageBuffer.put(loc, bb);

        // make sure the row is locked by the previous holder after undo,
        // or no lock if the previous holder is universally committed
        if (previousHolder > 0
                && !transactionManager.isUniversallyCommitted(previousHolder)) {
            RowHeader.setLockHolder(pageBuffer, loc, previousHolder);
            RowHeader.setLocked(pageBuffer, loc, true);
        } else
            RowHeader.setLocked(pageBuffer, loc, false);
    }

    private void redoLock(ByteBuffer bb) {
        int index = bb.getShort();
        long previousHolder = bb.getLong();

        int loc = dir.loc(index);

        // make sure the row is locked by the previous holder after undo,
        // or no lock if the previous holder is universally committed
        if (previousHolder > 0
                && !transactionManager.isUniversallyCommitted(previousHolder)) {
            RowHeader.setLockHolder(pageBuffer, loc, previousHolder);
            RowHeader.setLocked(pageBuffer, loc, true);
        } else
            RowHeader.setLocked(pageBuffer, loc, false);
    }

    private boolean checkLock(UndoLog log) {
        // verify that the transaction that wrote the log is the one holding the
        // row lock
        LogData entry = log.getData();
        ByteBuffer bb = entry.getData();
        bb.mark();
        short rowid = bb.getShort();
        bb.reset();
        int loc = dir.loc(rowid);
        long holder = RowHeader.getLockHolder(pageBuffer, loc);
        return holder == log.getTransID();
    }

    private int prepareUpdate(int index, int loc, int tsize, int rsize)
            throws StorageException {
        int diff = tsize - rsize;

        // Overwrite the existing row, making room for it if necessary
        if (diff > 0) {
            // Make room for the new tuple
            int unused = dir.getUnusedSpace();
            if (unused < diff) {
                if (getDeleteCount() > 0)
                    unused += compact();

                if (unused < diff)
                    // TODO: chain the updated row to overflow area
                    throw new StorageException(
                            "No room left on page for update.");
            }
            dir.replaceAt(index, tsize);
            return diff;
        }

        return 0;
    }
}
