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

package com.cooldb.access;

import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.api.Key;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Core;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.recovery.RedoException;
import com.cooldb.segment.AbstractSegmentMethod;
import com.cooldb.segment.CatalogMethod;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentDescriptor;
import com.cooldb.storage.Attachment;
import com.cooldb.transaction.RollbackException;
import com.cooldb.transaction.Transaction;

import java.io.PrintStream;

/**
 * <code>AbstractTree</code> implements the base GiST methods.
 */

public abstract class AbstractTree extends AbstractSegmentMethod implements
        GiST, Attachment {
    private final TreeDescriptor descriptor;
    private AbstractNode node;
    private Root root;

    public AbstractTree(Segment segment, Core core) throws DatabaseException {
        super(segment, core);
        descriptor = new TreeDescriptor(segment.getSegmentId());
        core.getTreeManager().select(descriptor);
    }

    @Override
    public synchronized void createSegmentMethod(Transaction trans)
            throws DatabaseException {
        core.getTreeManager().insert(trans, descriptor);

        // make sure the first page is allocated
        allocateNextPage(trans);

        getNode().createRoot(trans);
    }

    @Override
    public synchronized void dropSegmentMethod(Transaction trans)
            throws DatabaseException {
        core.getTreeManager().remove(trans, descriptor);
    }

    // GiST wrappers
    public synchronized DBObject findFirst(Transaction trans, Predicate query,
                                           Cursor cursor, boolean reverse) throws DatabaseException {
        return findFirst(trans, getRoot(), query, cursor, reverse, null);
    }

    public synchronized DBObject findFirst(Transaction trans, Predicate query,
                                           Cursor cursor, boolean reverse, Filter filter) throws DatabaseException {
        return findFirst(trans, getRoot(), query, cursor, reverse, filter);
    }

    public synchronized DBObject findNext(Transaction trans, Predicate query,
                                          Cursor cursor, boolean reverse) throws DatabaseException {
        return findNext(trans, query, cursor, reverse, null);
    }

    public synchronized void insert(Transaction trans, Entry entry)
            throws DatabaseException {
        insert(trans, getRoot(), entry, 0);
    }

    public synchronized void remove(Transaction trans, Predicate query)
            throws DatabaseException {
        remove(trans, getRoot(), query, 0);
    }

    // GiST implementation
    public synchronized Cursor allocateCursor() throws DatabaseException {
        return getNode().allocateCursor();
    }

    public synchronized void openCursor(Transaction trans, Cursor cursor) {
        getNode().openCursor(trans, cursor);
    }

    public synchronized DBObject findFirst(Transaction trans, Root root,
                                           Predicate query, Cursor cursor, boolean reverse, Filter filter)
            throws DatabaseException {
        return getNode().findFirst(trans, root, query, cursor, reverse, filter);
    }

    public synchronized DBObject findNext(Transaction trans, Predicate query,
                                          Cursor cursor, boolean reverse, Filter filter) throws DatabaseException {
        return getNode().findNext(trans, query, cursor, reverse, filter);
    }

    public synchronized void insert(Transaction trans, Root root, Entry entry,
                                    int level) throws DatabaseException {
        getNode().insert(trans, root, entry, level);
    }

    public synchronized void remove(Transaction trans, Root root,
                                    Predicate query, int level) throws DatabaseException {
        getNode().remove(trans, root, query, level);
    }

    // SegmentMethod implementation
    public CatalogMethod getCatalogMethod() {
        return core.getTreeManager();
    }

    public SegmentDescriptor getDescriptor() {
        return descriptor;
    }

    // SpaceDelegate implementation
    public void didAllocatePage(Transaction trans, FilePage page)
            throws DatabaseException {
        // initialize the new page, flush immediately
        AbstractNode node = getNode().getSubNode();
        try {
            node.writePin(page, true);
            node.create(FilePage.NULL, FilePage.NULL, descriptor.getFreePage());
        } finally {
            node.unPin(BufferPool.Affinity.LIKED);
        }

        // update the descriptor
        descriptor.setFreePage(page);

        getCatalogMethod().update(trans, descriptor);
    }

    // RecoveryDelegate implementation
    public synchronized void redo(RedoLog log) throws RedoException {
        getNode().redo(log);
    }

    public synchronized void undo(UndoLog log, Transaction trans)
            throws RollbackException {
        try {
            AbstractNode node = getNode();
            if (log.getPage().isNull()) {
                node.logicalUndo(log, trans);
                return;
            }
            try {
                node.undoPin(log);
                node.undo(log, trans);
            } finally {
                node.unPin(trans, BufferPool.Affinity.LIKED);
            }
        } catch (Exception e) {
            throw new RollbackException("Undo failed", e);
        }
    }

    public synchronized void undo(UndoLog log, PageBuffer pageBuffer)
            throws RollbackException {
        getNode().undo(log, pageBuffer);
    }

    public void print(Key key) {

    }

    /**
     * Find or allocate an empty page and remove it from the list of free pages.
     * This must be wrapped by the caller in a nested-top-action to avoid losing
     * pages.
     */
    FilePage allocatePage(Transaction trans, AbstractNode node)
            throws DatabaseException {
        try {
            FilePage freePage = descriptor.getFreePage();

            if (freePage.isNull())
                allocateNextPage(trans);

            FilePage newPage = new FilePage(freePage);

            node.logPin(newPage);

            // page is no longer free
            node.getNextFreePage(freePage);
            node.updateNextFreePage(trans, FilePage.NULL);

            // update the descriptor
            getCatalogMethod().update(trans, descriptor);

            return newPage;
        } finally {
            node.unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    /**
     * Push an empty page onto the free-page list. Assume the node is already
     * log-pinned.
     */
    void freePage(Transaction trans, AbstractNode node) throws DatabaseException {
        // Clear the page
        node.empty();

        // Push page onto the free list
        node.updateNextFreePage(trans, descriptor.getFreePage());
        descriptor.setFreePage(node.getPage());

        getCatalogMethod().update(trans, descriptor);
    }

    void registerRoot(Transaction trans, Root root) throws DatabaseException {
        descriptor.setRootPage(root.getPage());
        descriptor.setHeight(root.getLevel());

        getCatalogMethod().update(trans, descriptor);

        this.root = root;
    }

    Root getRoot() {
        if (root == null)
            root = new Root(descriptor.getRootPage(), descriptor.getHeight());
        return root;
    }

    /**
     * Allocate a new AbstractNode object of the type specific to this
     * implementation.
     */
    abstract AbstractNode newNode();

    AbstractNode getNode() {
        if (node == null)
            node = newNode();
        return node;
    }

    public void print(PrintStream out, String linePrefix) {
        out.print(linePrefix + getClass().getSimpleName() + "(");
        out.print("root:" + descriptor.getRootPage() + "/");
        out.print("height:" + descriptor.getHeight());
        out.println(")");
        getNode().print(out, linePrefix + "  ");
    }
}
