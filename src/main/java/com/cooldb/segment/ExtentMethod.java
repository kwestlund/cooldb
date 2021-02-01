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

package com.cooldb.segment;

import com.cooldb.buffer.FilePage;
import com.cooldb.recovery.RedoException;
import com.cooldb.transaction.RollbackException;
import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.BufferNotFound;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Core;
import com.cooldb.log.LogData;
import com.cooldb.log.LogExhaustedException;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.transaction.Transaction;

import java.util.Comparator;

/**
 * ExtentMethod manages extent allocation within the database.
 * <p>
 * Its operations are thread-safe, atomic and durable.
 */

public abstract class ExtentMethod extends AbstractSegmentMethod implements
        SegmentMethod {

    // log data types
    private static final byte EM_REDO_INSERT = 1;
    private static final byte EM_REDO_REMOVE = 2;
    private static final byte EM_UNDO_INSERT = 3;
    private static final byte EM_UNDO_REMOVE = 4;
    private final Extent prototype;
    // Pre-allocated Extent objects
    private final Extent redoExtentTmp;
    private final Extent undoExtentTmp;
    private final Extent findExtentTmp;
    ExtentPage extentPage;

    public ExtentMethod(Segment segment, Core core, Extent prototype) {
        super(segment, core);
        this.prototype = (Extent) prototype.copy();

        // Pre-allocated temporary objects
        redoExtentTmp = (Extent) prototype.copy();
        undoExtentTmp = (Extent) prototype.copy();
        findExtentTmp = (Extent) prototype.copy();
    }

    // SegmentMethod implementation
    public CatalogMethod getCatalogMethod() {
        return null;
    }

    public SegmentDescriptor getDescriptor() {
        return null;
    }

    @Override
    public synchronized void createSegmentMethod(Transaction trans)
            throws DatabaseException {
        try {
            try {
                init(writePin(true));
            } finally {
                unPin(BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new DatabaseException("Failed to create ExtentMethod.");
        }
    }

    @Override
    public synchronized void dropSegmentMethod(Transaction trans) {
    }

    /**
     * Return true if the extent exists, false otherwise.
     */
    public synchronized boolean exists(Extent extent)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            init(readPin());

            return extentPage.exists(extent);
        } finally {
            unPin(BufferPool.Affinity.LOVED);
        }
    }

    /**
     * Find and return an extent as large as that specified in 'minSize'. Return
     * true if found, false otherwise.
     */
    public synchronized boolean findExtent(int minSize, Extent extent)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            init(readPin());

            return extentPage.findExtent(minSize, extent);
        } finally {
            unPin(BufferPool.Affinity.LOVED);
        }
    }

    /**
     * Find the first extent that passes the filter.
     */
    public synchronized boolean findExtent(Comparator<?> filter)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            init(readPin());

            findExtentTmp.setNull();

            return extentPage.findExtent(findExtentTmp, filter);
        } finally {
            unPin(BufferPool.Affinity.LOVED);
        }
    }

    /**
     * Find the first extent that passes the filter, starting a scan from the
     * location of the extent argument.
     */
    public synchronized boolean findExtent(Extent extent, Comparator<?> filter)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            init(readPin());

            return extentPage.findExtent(extent, filter);
        } finally {
            unPin(BufferPool.Affinity.LOVED);
        }
    }

    /**
     * Insert the extent.
     */
    public synchronized void insertExtent(Transaction trans, Extent extent)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            init(logPin());

            attachUndo(EM_UNDO_INSERT, extent);

            extentPage.insertExtent(extent);

            attachRedo(EM_REDO_INSERT, extent);
        } finally {
            unPin(trans, BufferPool.Affinity.LOVED);
        }
    }

    /**
     * Remove the extent.
     */
    public synchronized void removeExtent(Transaction trans, Extent extent)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        try {
            init(logPin());

            attachUndo(EM_UNDO_REMOVE, extent);

            extentPage.removeExtent(extent);

            attachRedo(EM_REDO_REMOVE, extent);
        } finally {
            unPin(trans, BufferPool.Affinity.LOVED);
        }
    }

    // SpaceDelegate implementation
    public void didAllocatePage(Transaction trans, FilePage page) {
    }

    // RecoveryDelegate implementation
    public synchronized void redo(RedoLog log) throws RedoException {
        try {
            try {
                init(redoPin(log));

                if (log.isCLR()) {
                    redoCLR(log);
                    return;
                }

                LogData entry = log.getData();
                while (entry != null) {
                    switch (entry.getType()) {

                        case EM_REDO_INSERT:
                            redoExtentTmp.readFrom(entry.getData());
                            extentPage.insertExtent(redoExtentTmp);
                            break;

                        case EM_REDO_REMOVE:
                            redoExtentTmp.readFrom(entry.getData());
                            extentPage.removeExtent(redoExtentTmp);
                            break;

                        default:
                            break;
                    }

                    entry = entry.next();
                }
            } finally {
                unPin(BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new RedoException(e);
        }
    }

    public synchronized void undo(UndoLog log, Transaction trans)
            throws RollbackException {
        try {
            try {
                init(undoPin(log));
                undo(log, true);
            } finally {
                unPin(trans, BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new RollbackException(e);
        }
    }

    public synchronized void undo(UndoLog log, PageBuffer pageBuffer)
            throws RollbackException {
        try {
            init(bufferPin(pageBuffer));
            undo(log, false);
        } catch (Exception e) {
            throw new RollbackException(e);
        }
    }

    public synchronized void undo(UndoLog log, boolean writeCLR)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        LogData entry = log.getData().last();
        while (entry != null) {
            switch (entry.getType()) {
                case EM_UNDO_INSERT:
                    if (writeCLR)
                        attachCLR(entry);

                    // removeExtent as undo action of 'insertExtent'
                    undoExtentTmp.readFrom(entry.getData());
                    extentPage.removeExtent(undoExtentTmp);
                    break;

                case EM_UNDO_REMOVE:
                    if (writeCLR)
                        attachCLR(entry);

                    // insertExtent as undo action of 'removeExtent'
                    undoExtentTmp.readFrom(entry.getData());
                    extentPage.insertExtent(undoExtentTmp);
                    break;

                default:
                    break;
            }

            entry = entry.prev();
        }
    }

    /**
     * Redo CLR undo updates to this page as represented in log.
     */
    private void redoCLR(RedoLog log) {
        LogData entry = log.getData();
        while (entry != null) {
            switch (entry.getType()) {

                case EM_UNDO_REMOVE:
                    redoExtentTmp.readFrom(entry.getData());
                    extentPage.insertExtent(redoExtentTmp);
                    break;

                case EM_UNDO_INSERT:
                    redoExtentTmp.readFrom(entry.getData());
                    extentPage.removeExtent(redoExtentTmp);
                    break;

                default:
                    break;
            }

            entry = entry.next();
        }
    }

    void init(PageBuffer pb) {
        if (extentPage == null)
            extentPage = new ExtentPage(pb, prototype);
        else
            extentPage.setPageBuffer(pb);
    }
}
