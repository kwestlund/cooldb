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

import com.cooldb.buffer.*;
import com.cooldb.recovery.RedoException;
import com.cooldb.transaction.RollbackException;
import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.core.Core;
import com.cooldb.log.LogData;
import com.cooldb.log.LogExhaustedException;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.transaction.Transaction;
import com.cooldb.buffer.*;

// TODO: reimplement so that the segment can grow beyond a single page

public class CatalogMethod extends AbstractSegmentMethod {

    // log data types
    private static final byte REDO_INSERT = 1;
    private static final byte REDO_REMOVE = 2;
    private static final byte UNDO_INSERT = 3;
    private static final byte UNDO_REMOVE = 4;
    private final DBObject objTmp;
    private CatalogPage catalogPage;

    public CatalogMethod(Segment segment, Core core, DBObject prototype) {
        super(segment, core);
        objTmp = prototype.copy();
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
            throw new DatabaseException("Failed to create CatalogMethod.");
        }
    }

    @Override
    public synchronized void dropSegmentMethod(Transaction trans) {
    }

    /**
     * Insert the object onto the page.
     */
    public synchronized void insert(Transaction trans, DBObject obj)
            throws DatabaseException {
        try {
            try {
                init(logPin());

                catalogPage.insert(obj);

                attachUndo(UNDO_INSERT, obj);
                attachRedo(REDO_INSERT, obj);
            } finally {
                unPin(trans, BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new DatabaseException("Failed to insert obj.");
        }
    }

    /**
     * Remove the obj from the page.
     */
    public synchronized void remove(Transaction trans, DBObject obj)
            throws DatabaseException {
        try {
            try {
                init(logPin());

                if (catalogPage.remove(obj)) {
                    attachUndo(UNDO_REMOVE, obj);
                    attachRedo(REDO_REMOVE, obj);
                }
            } finally {
                unPin(trans, BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new DatabaseException("Failed to remove object.");
        }
    }

    /**
     * Replace the object on the page.
     */
    public synchronized void update(Transaction trans, DBObject obj)
            throws DatabaseException {
        try {
            try {
                init(logPin());

                // remove
                if (catalogPage.remove(obj)) {
                    attachUndo(UNDO_REMOVE, obj);
                    attachRedo(REDO_REMOVE, obj);
                }

                // insert
                catalogPage.insert(obj);

                attachUndo(UNDO_INSERT, obj);
                attachRedo(REDO_INSERT, obj);
            } finally {
                unPin(trans, BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new DatabaseException("Failed to remove object.", e);
        }
    }

    /**
     * Read the object identified by its objectId from the page.
     */
    public synchronized boolean select(DBObject obj) throws DatabaseException {
        try {
            try {
                init(readPin());

                return catalogPage.select(obj);
            } finally {
                unPin(BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new DatabaseException("Failed to remove object.");
        }
    }

    /**
     * Fetch the next object passing the filter, starting at the given index
     * position. Returns the index of the fetched object or -1 if none found.
     */
    public synchronized int select(DBObject obj, int index, Filter filter)
            throws DatabaseException {
        try {
            try {
                init(readPin());

                int count = catalogPage.getCount();
                for (int i = index; i < count; i++) {
                    catalogPage.get(i, obj);
                    if (filter.passes(obj))
                        return i;
                }
                return -1;
            } finally {
                unPin(BufferPool.Affinity.LOVED);
            }
        } catch (Exception e) {
            throw new DatabaseException("Failed to remove object.");
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

                        case REDO_INSERT:
                            objTmp.readFrom(entry.getData());
                            catalogPage.insert(objTmp);
                            break;

                        case REDO_REMOVE:
                            objTmp.readFrom(entry.getData());
                            catalogPage.remove(objTmp);
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
                case UNDO_INSERT:
                    if (writeCLR)
                        attachCLR(entry);

                    // remove as undo action of insert
                    objTmp.readFrom(entry.getData());
                    catalogPage.remove(objTmp);
                    break;

                case UNDO_REMOVE:
                    if (writeCLR)
                        attachCLR(entry);

                    // insert as undo action of remove
                    objTmp.readFrom(entry.getData());
                    catalogPage.insert(objTmp);
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

                case UNDO_REMOVE:
                    objTmp.readFrom(entry.getData());
                    catalogPage.insert(objTmp);
                    break;

                case UNDO_INSERT:
                    objTmp.readFrom(entry.getData());
                    catalogPage.remove(objTmp);
                    break;

                default:
                    break;
            }

            entry = entry.next();
        }
    }

    // debugging: print all objects
    public void dump() {
        try {
            Filter filter = o -> true;
            int i = 0;
            while ((i = select(objTmp, i, filter)) != -1) {
                System.out.println(objTmp.toString());
                ++i;
            }
        } catch (Exception e) {
            System.err.print(e.getMessage());
        }
    }

    private void init(PageBuffer pb) {
        if (catalogPage == null)
            catalogPage = new CatalogPage(pb, objTmp);
        else
            catalogPage.setPageBuffer(pb);
    }
}
