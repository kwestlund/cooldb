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
import com.cooldb.transaction.RollbackException;
import com.cooldb.transaction.TransactionManager;
import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.LogData;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.log.UndoPointer;
import com.cooldb.recovery.RecoveryDelegate;
import com.cooldb.recovery.RedoException;
import com.cooldb.segment.PageBroker;
import com.cooldb.transaction.Transaction;

import java.io.PrintStream;
import java.nio.ByteBuffer;

public abstract class DirPage extends RowPage implements RecoveryDelegate {

    // for subclasses
    protected static final byte NEXT_LOGDATA_TYPE = 9;
    // log data types
    private static final byte REDO_NEXTPAGE = 1;
    private static final byte UNDO_NEXTPAGE = 2;
    private static final byte REDO_PREVPAGE = 3;
    private static final byte UNDO_PREVPAGE = 4;
    private static final byte REDO_NEXTFREEPAGE = 5;
    private static final byte UNDO_NEXTFREEPAGE = 6;
    private static final byte REDO_COMPACT = 7;
    private static final byte REDO_PURGE = 8;
    private static final byte DIRPAGE = 1;
    protected final TransactionManager transactionManager;
    protected final DirectoryArea dir;
    private final PageBroker pageBroker;
    private final FilePage tmpPage = new FilePage();

    public DirPage(TransactionManager transactionManager, PageBroker pageBroker) {
        super();
        this.transactionManager = transactionManager;
        this.pageBroker = pageBroker;
        this.dir = new DirectoryArea();
    }

    public FilePage getPage() {
        return pageBroker.getPage();
    }

    public void create(FilePage nextPage, FilePage prevPage,
                       FilePage nextFreePage) {
        setNextPage(nextPage);
        setPrevPage(prevPage);
        setNextFreePage(nextFreePage);
        setDeleteCount((short) 0);
    }

    public void writePin(FilePage page, boolean isNew) throws StorageException {
        try {
            init(pageBroker.writePin(page, isNew));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void tempPin(FilePage page, Transaction trans)
            throws StorageException {
        try {
            init(pageBroker.tempPin(page, trans));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void readPin(FilePage page) throws StorageException {
        try {
            init(pageBroker.readPin(page));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void versionPin(Transaction trans, FilePage page, long version)
            throws StorageException {
        try {
            init(pageBroker.versionPin(trans, page, version));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void logPin(FilePage page) throws StorageException {
        try {
            init(pageBroker.logPin(page, DIRPAGE));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void undoPin(UndoLog log) throws StorageException {
        try {
            init(pageBroker.undoPin(log));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void unPin(Transaction trans, BufferPool.Affinity affinity)
            throws StorageException {
        try {
            pageBroker.unPin(trans, affinity);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void unPin(BufferPool.Affinity affinity) throws StorageException {
        try {
            pageBroker.unPin(affinity);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public LogData attachUndo(byte type, int size) {
        return pageBroker.attachUndo(type, size);
    }

    public LogData attachLogicalUndo(byte type, int size) {
        return pageBroker.attachLogicalUndo(type, size);
    }

    public LogData attachRedo(byte type, int size) {
        return pageBroker.attachRedo(type, size);
    }

    public LogData attachCLR(byte type, int size) {
        return pageBroker.attachCLR(type, size);
    }

    public void attachUndo(byte type, DBObject obj) {
        pageBroker.attachUndo(type, obj);
    }

    public void attachRedo(byte type, DBObject obj) {
        pageBroker.attachRedo(type, obj);
    }

    public void attachCLR(byte type, DBObject obj) {
        pageBroker.attachCLR(type, obj);
    }

    public void attachCLR(LogData rld) {
        pageBroker.attachCLR(rld);
    }

    public UndoPointer writeUndoRedo(Transaction trans) throws StorageException {
        try {
            return pageBroker.writeUndoRedo(trans);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    /**
     * Update the next-page pointer and log the change.
     */
    public void updateNextPage(Transaction trans, FilePage nextPage) {
        getNextPage(tmpPage);

        attachUndo(UNDO_NEXTPAGE, tmpPage);

        setNextPage(nextPage);

        attachRedo(REDO_NEXTPAGE, nextPage);
    }

    /**
     * Update the prev-page pointer and log the change.
     */
    public void updatePrevPage(Transaction trans, FilePage prevPage) {
        getPrevPage(tmpPage);

        attachUndo(UNDO_PREVPAGE, tmpPage);

        setPrevPage(prevPage);

        attachRedo(REDO_PREVPAGE, prevPage);
    }

    /**
     * Update the next-free-page pointer and log the change.
     */
    public void updateNextFreePage(Transaction trans, FilePage nextFreePage) {
        getNextFreePage(tmpPage);

        attachUndo(UNDO_NEXTFREEPAGE, tmpPage);

        setNextFreePage(nextFreePage);

        attachRedo(REDO_NEXTFREEPAGE, nextFreePage);
    }

    // RecoveryDelegate implementation
    public void redo(RedoLog log) throws RedoException {
        try {
            try {
                init(pageBroker.redoPin(log));

                if (log.isCLR()) {
                    redoCLR(log);
                    return;
                }

                LogData entry = log.getData();
                while (entry != null) {
                    switch (entry.getType()) {
                        case REDO_NEXTPAGE:
                            setNextPage(entry.getData());
                            break;

                        case REDO_PREVPAGE:
                            setPrevPage(entry.getData());
                            break;

                        case REDO_NEXTFREEPAGE:
                            setNextFreePage(entry.getData());
                            break;

                        case REDO_COMPACT:
                            redoCompact(entry.getData());
                            break;

                        case REDO_PURGE:
                            redoPurge(entry.getData());
                            break;

                        default:
                            redo(log, entry);
                    }
                    entry = entry.next();
                }
            } finally {
                unPin(BufferPool.Affinity.LIKED);
            }
        } catch (Exception e) {
            throw new RedoException(e);
        }
    }

    public void undo(UndoLog log, Transaction trans) throws RollbackException {
        try {
            undo(log, true);
        } catch (Exception e) {
            throw new RollbackException(e);
        }
    }

    public void undo(UndoLog log, PageBuffer pageBuffer)
            throws RollbackException {
        try {
            init(pageBuffer);
            undo(log, false);
        } catch (Exception e) {
            throw new RollbackException(e);
        }
    }

    // Space handling
    public boolean canHold(int tupleSize, byte loadMax) {
        int size = RowHeader.getOverhead() + tupleSize;
        int fs = insertSpace(loadMax);

        // If there is enough contiguous free space, return true.
        if (size <= fs)
            return true;

        // If there is enough combined free and tenuous space, then compact
        // the page's data space -- move tenuous space to the free space region

        if (getDeleteCount() > 0) {

            fs += compact();

            return size <= fs;
        }

        return false;
    }

    public byte loadFactor() {
        // return an estimate of load
        int count = dir.getCount();
        if (count <= 0)
            return 0;

        int usedSpace = dir.getUsedSpace();
        double avgSize = usedSpace * 1.0 / count;
        int freeSpace = (int) (avgSize * getDeleteCount());

        return (byte) ((usedSpace - freeSpace) * 100.0 / dir.getCapacity());
    }

    public int getMaxCapacity() {
        return pageBuffer.capacity() - BASE - 2;
    }

    /**
     * The space available for inserts is the total available space minus
     * reserved space.
     */
    public int insertSpace(byte loadMax) {
        return dir.getUnusedSpace() - 2
                - (int) (dir.getCapacity() * (1.0 - (loadMax / 100.0)));
    }

    protected int setDeleted(short index, boolean isDeleted) {
        int loc = dir.loc(index);
        if (isDeleted) {
            if (!RowHeader.isDeleted(pageBuffer, loc)) {
                RowHeader.setDeleted(pageBuffer, loc, true);
                setDeleteCount((short) (getDeleteCount() + 1));
            }
        } else if (RowHeader.isDeleted(pageBuffer, loc)) {
            RowHeader.setDeleted(pageBuffer, loc, false);
            setDeleteCount((short) (getDeleteCount() - 1));
        }
        return loc;
    }

    /**
     * Schedule the lock request. If the lock would conflict with a lock
     * currently held on the row, then this throws a LockConflict exception
     * identifying the transaction currently holding the lock.
     */
    protected long lock(Transaction trans, int rowid) throws LockConflict,
            StorageException {
        // Raise a lock conflict exception if there is a conflict with this
        // request
        long holder = 0;
        int loc = dir.loc(rowid);
        if (RowHeader.isLocked(pageBuffer, loc)) {
            holder = RowHeader.getLockHolder(pageBuffer, loc);

            // If this transaction already holds the lock, then return
            // immediately
            if (holder == trans.getTransId())
                return holder;

            // Otherwise, if the holder is not committed with respect to this
            // transaction then raise an exception
            if (!trans.isCommitted(holder)) {
                if (trans.isSerializable())
                    throw new SerializationConflict(
                            "Cannot serialize access for this transaction.");
                else if (!transactionManager.isCommitted(holder))
                    throw new LockConflict(holder);
            }
        } else
            RowHeader.setLocked(pageBuffer, loc, true);

        // Grant the lock on this record to the requesting transaction
        RowHeader.setLockHolder(pageBuffer, loc, trans.getTransId());

        return holder;
    }

    protected abstract void redo(RedoLog log, LogData entry)
            throws DatabaseException;

    protected abstract void undo(UndoLog log, LogData entry, boolean writeCLR)
            throws DatabaseException;

    protected abstract void redoCLR(RedoLog clr, LogData entry)
            throws DatabaseException;

    protected int compact() {
        return compact(true);
    }

    /**
     * Move free space within the data space to the free space region by
     * compacting the data tuples and then reducing the load, and finally
     * updating the directory index values to reflect new tuple locations. Use
     * the universal commit point to avoid stomping on updates that might yet be
     * needed by transactions for rollback or version control. If preserve is
     * true, then leave a placeholder in the directory for each compacted entry
     * to preserve the index location of each remaining entry, since the Rowid
     * depends on this index. Otherwise, if preserve is false, entirely remove
     * compacted entries from the directory as well as from the directory index
     * itself.
     */
    protected int compact(boolean preserve) {
        // Reclaim space of deleted rows
        int compactTo = 0; // 1 after the last to be compacted
        int shiftSize = 0; // number of rows to be compacted
        int reclaimed = 0; // number of bytes compacted
        int index = dir.getCount();
        // Iterate backwards so we can delete as we go
        while (index-- > 0) {
            int loc = dir.loc(index);

            // Reclaim space of deleted tuples that are no longer needed
            boolean reclaim = false;
            if (RowHeader.isDeleted(pageBuffer, loc)) {
                reclaim = true;
                if (RowHeader.isLocked(pageBuffer, loc)) {
                    if (!transactionManager.isUniversallyCommitted(RowHeader
                                                                           .getLockHolder(pageBuffer, loc)))
                        reclaim = false;
                }
            }

            if (reclaim) {
                if (shiftSize == 0)
                    compactTo = index + 1;
                ++shiftSize;
            } else if (shiftSize > 0) {
                reclaimed += compact(preserve, compactTo - shiftSize, compactTo);
                shiftSize = 0;
            }
        }
        if (shiftSize > 0) {
            reclaimed += compact(preserve, compactTo - shiftSize, compactTo);
        }

        return reclaimed;
    }

    private int compact(boolean preserve, int fromI, int toI) {
        attachRedo(preserve ? REDO_COMPACT : REDO_PURGE, 4).getData().putShort(
                (short) fromI).putShort((short) toI);
        return preserve ? compact(fromI, toI) : purge(fromI, toI);
    }

    /**
     * Remove all data associated with the entries >= fromI and < toI, but keep
     * the directory entries themselves unless the toI == count; Return the
     * number of bytes compacted.
     */
    private int compact(int fromI, int toI) {
        if (fromI < toI) {
            if (toI == dir.getCount()) {
                setDeleteCount((short) (getDeleteCount() - (toI - fromI)));
            }
            return dir.compact(fromI, toI);
        }
        return 0;
    }

    /**
     * Remove all data associated with the entries >= fromI and < toI. Return
     * the number of bytes purged.
     */
    private int purge(int fromI, int toI) {
        if (fromI < toI) {
            setDeleteCount((short) (getDeleteCount() - (toI - fromI)));
            return dir.purge(fromI, toI);
        }
        return 0;
    }

    private void undo(UndoLog log, boolean writeCLR) throws DatabaseException {
        LogData entry = log.getData();
        while (entry != null) {
            switch (entry.getType()) {
                case UNDO_NEXTPAGE:
                    // Do not undo these attributes during reconstruction of a page
                    // for multi-version concurrency control, since they are likely
                    // to have been part of a nested-top-action for space-allocation
                    // purposes that must not be undone.
                    if (writeCLR) {
                        attachCLR(entry);
                        setNextPage(entry.getData());
                    }
                    break;

                case UNDO_PREVPAGE:
                    if (writeCLR) {
                        attachCLR(entry);
                        setPrevPage(entry.getData());
                    }
                    break;

                case UNDO_NEXTFREEPAGE:
                    if (writeCLR) {
                        attachCLR(entry);
                        setNextFreePage(entry.getData());
                    }
                    break;

                default:
                    undo(log, entry, writeCLR);
            }
            entry = entry.next();
        }
    }

    private void redoCLR(RedoLog clr) throws DatabaseException {
        LogData entry = clr.getData();
        while (entry != null) {
            switch (entry.getType()) {
                case UNDO_NEXTPAGE:
                    setNextPage(entry.getData());
                    break;

                case UNDO_PREVPAGE:
                    setPrevPage(entry.getData());
                    break;

                case UNDO_NEXTFREEPAGE:
                    setNextFreePage(entry.getData());
                    break;

                default:
                    redoCLR(clr, entry);
            }
            entry = entry.next();
        }
    }

    private void redoCompact(ByteBuffer bb) {
        short fromI = bb.getShort();
        short toI = bb.getShort();

        compact(fromI, toI);
    }

    private void redoPurge(ByteBuffer bb) {
        short fromI = bb.getShort();
        short toI = bb.getShort();

        purge(fromI, toI);
    }

    private void setNextPage(ByteBuffer bb) {
        tmpPage.readFrom(bb);
        setNextPage(tmpPage);
    }

    private void setPrevPage(ByteBuffer bb) {
        tmpPage.readFrom(bb);
        setPrevPage(tmpPage);
    }

    private void setNextFreePage(ByteBuffer bb) {
        tmpPage.readFrom(bb);
        setNextFreePage(tmpPage);
    }

    private void init(PageBuffer pageBuffer) {
        setPageBuffer(pageBuffer);
        dir.setPageBuffer(pageBuffer);

        dir.setBounds(BASE, pageBuffer.capacity() - BASE);
    }

    public void print(PrintStream out, String linePrefix) {
        super.print(out, linePrefix);
        dir.print(out, linePrefix);
    }
}
