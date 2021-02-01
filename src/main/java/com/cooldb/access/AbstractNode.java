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
import com.cooldb.api.UniqueConstraintException;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;
import com.cooldb.log.LogData;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.segment.BinarySearch;
import com.cooldb.segment.DBObjectArray;
import com.cooldb.segment.PageBroker;
import com.cooldb.storage.DirPage;
import com.cooldb.storage.RowHeader;
import com.cooldb.storage.StorageException;
import com.cooldb.transaction.TransactionManager;
import com.cooldb.transaction.NestedTopAction;
import com.cooldb.transaction.Transaction;

import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.util.Comparator;

/**
 * <code>AbstractNode</code> implements the base GiST.Node methods.
 */

public abstract class AbstractNode extends DirPage implements GiST.Node,
        DBObjectArray {

    // log data types
    private static final byte REDO_LEAF_REMOVE = NEXT_LOGDATA_TYPE;
    private static final byte UNDO_LEAF_REMOVE = NEXT_LOGDATA_TYPE + 1;
    private static final byte REDO_BRANCH_REMOVE = NEXT_LOGDATA_TYPE + 2;
    private static final byte UNDO_BRANCH_REMOVE = NEXT_LOGDATA_TYPE + 3;
    private static final byte REDO_LEAF_INSERT = NEXT_LOGDATA_TYPE + 4;
    private static final byte UNDO_LEAF_INSERT = NEXT_LOGDATA_TYPE + 5;
    private static final byte REDO_BRANCH_INSERT = NEXT_LOGDATA_TYPE + 6;
    private static final byte UNDO_BRANCH_INSERT = NEXT_LOGDATA_TYPE + 7;
    private static final byte REDO_TRUNCATE = NEXT_LOGDATA_TYPE + 8;
    private static final byte UNDO_TRUNCATE = NEXT_LOGDATA_TYPE + 9;
    private static final byte REDO_REPLACE_BP = NEXT_LOGDATA_TYPE + 10;
    private static final byte UNDO_REPLACE_BP = NEXT_LOGDATA_TYPE + 11;
    private final FilePage nextPage = new FilePage();
    private final FilePage prevPage = new FilePage();
    private final Comparator<Entry> entryComparator = new EntryComparator();
    final AbstractTree tree;
    private AbstractNode subNode;
    private BinarySearch.Search search;
    private BinarySearch.Search leafSearch;
    private BinarySearch.Search branchSearch;
    private Entry[] path;

    public AbstractNode(AbstractTree tree,
                        TransactionManager transactionManager, PageBroker pageBroker) {
        super(transactionManager, pageBroker);
        this.tree = tree;
    }

    abstract Predicate allocatePredicate() throws DatabaseException;

    abstract void initRoot(Transaction trans, FilePage root, int level)
            throws DatabaseException;

    public Cursor allocateCursor() throws DatabaseException {
        return new TreeCursor(newEntry(0));
    }

    public void openCursor(Transaction trans, Cursor cursor) {
        cursor.setCusp(trans.getUndoNxtLSN().getLSN());
        cursor.setLeaf(FilePage.NULL);
    }

    public DBObject findFirst(Transaction trans, Root root, Predicate query,
                              Cursor cursor, boolean reverse, Filter filter)
            throws DatabaseException {
        FilePage node = descend(root, query, 0);

        try {
            logPin(node);

            boolean found = bsearch(query, 0);

            // maintain cursor
            cursor.setLeaf(node);
            cursor.setIndex(search.index);
            cursor.setPageLSN(getPageLSN());

            if (search.index >= 0 && search.index < getCount()) {
                LeafEntry entry = (LeafEntry) cursor.getEntry();

                if (found)
                    entry.assign(search.value);
                else
                    get(search.index, entry);

                if (consistent(entry, query)
                        && observable(trans, entry, cursor)
                        && passes(entry, filter))
                    return entry.getPointer();
            }
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }

        // if the entry found by bsearch is not consistent with the query
        // predicate, then
        // try the next one (because if the entry is equal to the query start
        // key and the
        // condition is "greater-than", then the entry found by the binary
        // search will not
        // be consistent with the query but the next one will be consistent).
        // Also if the
        // identified entry is not observable, then we need to scan for the next
        // observable,
        // consistent entry.
        return findNext(trans, query, cursor, reverse, filter);
    }

    public DBObject findNext(Transaction trans, Predicate query, Cursor cursor,
                             boolean reverse, Filter filter) throws DatabaseException {
        try {
            if (cursor.getLeaf().isNull())
                return null;

            readPin(cursor.getLeaf());

            // if the page has changed since the last read, then we
            // need to reposition on the last key, which may have been moved to
            // the right
            if (getPageLSN() != cursor.getPageLSN()) {
                while (!bsearch(cursor.getEntry().getPredicate(), 0)) {
                    // move to the next page and search there
                    if (!moveToNextPage(trans, cursor, reverse))
                        throw new DatabaseException("Lost cursor position.");
                }
                cursor.setIndex(search.index);
            }

            // scan forward looking for the next observable key or end-of-file;
            // return the entry pointer if key is consistent or null otherwise
            while (true) {
                if (!advance(trans, cursor, reverse))
                    return null;

                LeafEntry entry = (LeafEntry) cursor.getEntry();
                get(cursor.getIndex(), entry);

                if (consistent(entry, query)) {
                    if (observable(trans, entry, cursor)
                            && passes(entry, filter)) {
                        return entry.getPointer();
                    }
                } else
                    return null;
            }
        } finally {
            unPin(BufferPool.Affinity.LIKED);
        }
    }

    public void insert(Transaction trans, Root root, Entry entry, int level)
            throws DatabaseException {
        nodeInsert(trans, root, descend(root, entry.getPredicate(), level),
                   entry, level);
    }

    public void remove(Transaction trans, Root root, Predicate query, int level)
            throws DatabaseException {
        nodeRemove(trans, descend(root, query, level), query, level);
    }

    // DBObjectArray implementation
    public int getCount() {
        return dir.getCount() - 1;
    }

    public DBObject get(int index, DBObject obj) {
        int loc = dir.loc(ix(index));
        pageBuffer.get(loc, obj);
        return obj;
    }

    // DirPage implementation
    @Override
    protected void redo(RedoLog log, LogData entry) throws DatabaseException {
        switch (entry.getType()) {
            case REDO_TRUNCATE:
                redoTruncate(entry.getData());
                break;

            case REDO_REPLACE_BP:
                redoReplaceBoundingPredicate(entry.getData());
                break;

            case REDO_LEAF_REMOVE:
                redoLeafRemove(entry.getData());
                break;

            case REDO_BRANCH_REMOVE:
                redoBranchRemove(entry.getData());
                break;

            case REDO_LEAF_INSERT:
                redoLeafInsert(entry.getData());
                break;

            case REDO_BRANCH_INSERT:
                redoBranchInsert(entry.getData());
                break;

            default:
                break;
        }
    }

    @Override
    protected void undo(UndoLog log, LogData entry, boolean writeCLR)
            throws DatabaseException {
        switch (entry.getType()) {
            case UNDO_TRUNCATE:
                if (writeCLR)
                    attachCLR(entry);
                redoTruncate(entry.getData());
                break;

            case UNDO_REPLACE_BP:
                if (writeCLR)
                    attachCLR(entry);
                redoReplaceBoundingPredicate(entry.getData());
                break;

            case UNDO_BRANCH_REMOVE:
                if (writeCLR)
                    attachCLR(entry);
                undoBranchRemove(entry.getData());
                break;

            case UNDO_BRANCH_INSERT:
                if (writeCLR)
                    attachCLR(entry);
                undoBranchInsert(entry.getData());
                break;

            default:
                break;
        }
    }

    /**
     * Apply the undo log to a possibly different page than the one that was
     * modified originally, by first descending the tree to find the page now
     * containing the modified key.
     */
    void logicalUndo(UndoLog log, Transaction trans) throws DatabaseException {
        // Read key predicate from the log entry (assume a single log entry)
        LogData ld = log.getData();
        ByteBuffer bb = ld.getData();
        Entry e = newEntry(0);
        e.readFrom(bb);

        // Descend the tree to find the key page; set the page in the log record
        FilePage node = descend(tree.getRoot(), e.getPredicate(), 0);
        log.setPage(node);

        // Apply the undo to the page
        try {
            undoPin(log);

            attachCLR(ld);

            switch (ld.getType()) {
                case UNDO_LEAF_REMOVE:
                    undoLeafRemove(bb, e);
                    break;

                case UNDO_LEAF_INSERT:
                    undoLeafInsert(bb, e);
                    break;

                default:
                    break;
            }
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    @Override
    protected void redoCLR(RedoLog clr, LogData entry) throws DatabaseException {
        switch (entry.getType()) {
            case UNDO_TRUNCATE:
                redoTruncate(entry.getData());
                break;

            case UNDO_REPLACE_BP:
                redoReplaceBoundingPredicate(entry.getData());
                break;

            case UNDO_LEAF_REMOVE:
                undoLeafRemove(entry.getData());
                break;

            case UNDO_BRANCH_REMOVE:
                undoBranchRemove(entry.getData());
                break;

            case UNDO_LEAF_INSERT:
                undoLeafInsert(entry.getData());
                break;

            case UNDO_BRANCH_INSERT:
                undoBranchInsert(entry.getData());
                break;

            default:
                break;
        }
    }

    @Override
    protected int compact() {
        return compact(false);
    }

    @Override
    protected long lock(Transaction trans, int index) {
        try {
            return super.lock(trans, ix(index));
        } catch (Exception e) {
            // should not be possible
            throw new RuntimeException(
                    "Internal error: unexpected exception in attempt to acquire lock");
        }
    }

    @Override
    protected int setDeleted(short index, boolean b) {
        return super.setDeleted((short) ix(index), b);
    }

    /**
     * Create a new root at the specified level with a single entry pointing to
     * the oldRoot.
     */
    void createRoot(Transaction trans) throws DatabaseException {
        createNewRoot(trans, new Root(new FilePage(), -1));
    }

    AbstractNode getSubNode() {
        if (subNode == null)
            subNode = tree.newNode();
        return subNode;
    }

    Entry newEntry(int level) throws DatabaseException {
        if (level == 0)
            return new LeafEntry(allocatePredicate());
        else
            return new BranchEntry(allocatePredicate());
    }

    // DirectoryArea wrappers hiding bounding predicate at location zero
    int getLocation(int index) {
        return dir.loc(ix(index));
    }

    void insertAt(int index, int size) {
        dir.insertAt(ix(index), (short) size);
    }

    void replaceAt(int index, int size) {
        dir.replaceAt(ix(index), (short) size);
    }

    void removeAt(int index) {
        dir.removeAt(ix(index));
    }

    /**
     * Note: Bounding predicates are all LeafEntries (even those on Branch
     * nodes) because the compact method examines the flags of all slots, even
     * this one to determine whether to reclaim its space, and the root branch
     * node can be converted into a leaf node when all keys have been removed.
     * The bounding predicate really should have its own distinct slot.
     */
    void insertBoundingPredicate(Predicate bp) throws DatabaseException {
        Entry entry = newEntry(0);
        entry.setPredicate(bp);
        dir.insertAt(0, (short) entry.storeSize());
        pageBuffer.put(dir.loc(0), entry);
    }

    void getBoundingPredicate(Predicate bp) throws DatabaseException {
        Entry entry = newEntry(0);
        int loc = dir.loc(0);
        pageBuffer.get(loc, entry);
        bp.assign(entry.getPredicate());
    }

    void moveRange(AbstractNode toNode, int fromI, int toI) {
        toNode.dir.copyRange(this.dir, ix(fromI), ix(toI));
        truncate(ix(fromI));
    }

    void empty() {
        // remove the bounding predicate as well as any remaining keys
        truncate(0);
    }

    private FilePage descend(Root root, Predicate query, int level)
            throws DatabaseException {
        FilePage nodePage = root.getPage();
        int nodeLevel = root.getLevel();

        // Make sure we have an array large enough to record the path taken
        if (path == null || path.length < nodeLevel + 1)
            path = new Entry[nodeLevel + 1];

        path[nodeLevel] = null;

        while (nodeLevel > level) {
            Entry choice = null;

            try {
                readPin(nodePage);

                if (!bsearch(query, nodeLevel) && nodeLevel > 0)
                    --search.index;
                if (search.index < getCount()) {
                    choice = newEntry(nodeLevel);
                    get(search.index, choice);
                }
            } finally {
                // branch nodes are more likely to be needed, especially the
                // root page
                unPin(BufferPool.Affinity.LOVED);
            }

            if (choice == null)
                throw new DatabaseException(
                        "Internal error: no choice of subtree satisfactory");

            path[--nodeLevel] = choice;
            nodePage = (FilePage) choice.getPointer();
        }

        return nodePage;
    }

    private boolean canHold(int size, int level) {
        if (level == 0)
            return leafCanHold(size);
        else
            return branchCanHold(size);
    }

    private boolean leafCanHold(int size) {
        return super.canHold(size, (byte) 100);
    }

    private boolean branchCanHold(int size) {
        return size <= insertSpace((byte) 100);
    }

    private void nodeInsert(Transaction trans, Root root, FilePage node,
                            Entry entry, int level) throws DatabaseException {
        try {
            logPin(node);

            int esize = entry.storeSize();
            if (canHold(esize, level))
                pageInsert(trans, entry, level, esize);
            else {
                // split the node into 2 nodes, which leaves the node unpinned
                split(trans, root, level);

                // re-descend the tree to determine whether
                // to insert into the old or the new node
                insert(trans, root, entry, level);
            }
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    private void nodeRemove(Transaction trans, FilePage node,
                            Predicate query, int level) throws DatabaseException {
        try {
            logPin(node);
            pageRemove(trans, query, level);
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    private void split(Transaction trans, Root root, int level)
            throws DatabaseException {
        // make sure this action is atomic if the split is a leaf-level split
        NestedTopAction savePoint = null;
        if (level == 0)
            savePoint = tree.beginNestedTopAction(trans);

        try {
            if (level == root.getLevel())
                createNewRoot(trans, root);

            // create a newNode; assume node is currently log-pinned
            Entry bp;
            AbstractNode rightNode = getSubNode();
            FilePage newPage = tree.allocatePage(trans, rightNode);
            try {
                rightNode.writePin(newPage, false);

                bp = pickSplit(rightNode, level);

                rightNode.setPrevPage(getPage());
                getNextPage(nextPage);
                rightNode.setNextPage(nextPage);
                updateNextPage(trans, rightNode.getPage());
            } finally {
                rightNode.unPin(BufferPool.Affinity.LIKED);
            }
            unPin(trans, BufferPool.Affinity.LIKED);

            if (!nextPage.isNull())
                updatePrevPage(trans, nextPage, newPage);

            // insert separator into parent
            ++level;
            if (level < root.getLevel() && path[level] != null)
                nodeInsert(trans, root, (FilePage) path[level].getPointer(),
                           bp, level);
            else
                nodeInsert(trans, root, root.getPage(), bp, level);

            if (savePoint != null)
                tree.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            if (savePoint != null)
                tree.rollbackNestedTopAction(trans, savePoint);
            throw new DatabaseException("Failed to merge empty node.", e);
        }
    }

    private void merge(Transaction trans, Root root, int level)
            throws DatabaseException {
        // make sure this action is atomic if the split is a leaf-level merge
        NestedTopAction savePoint = null;
        if (level == 0)
            savePoint = tree.beginNestedTopAction(trans);

        try {
            // if this is the root itself, then set the root level to zero if
            // necessary;
            // assume node is currently log-pinned
            if (root.getPage().equals(getPage())) {
                if (root.getLevel() > 0) {
                    root.setLevel(0);
                    tree.registerRoot(trans, root);
                }
                return;
            }

            Entry bp = newEntry(level);
            getBoundingPredicate(bp.getPredicate());

            getNextPage(nextPage);
            getPrevPage(prevPage);

            tree.freePage(trans, this);

            unPin(trans, BufferPool.Affinity.LIKED);

            if (!prevPage.isNull())
                updateNextPage(trans, prevPage, nextPage);

            if (!nextPage.isNull())
                updatePrevPage(trans, nextPage, prevPage);

            // remove separator from parent
            remove(trans, root, bp.getPredicate(), level + 1);

            if (savePoint != null)
                tree.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            if (savePoint != null)
                tree.rollbackNestedTopAction(trans, savePoint);
            throw new DatabaseException("Failed to merge empty node.", e);
        }
    }

    void pageInsert(Transaction trans, Entry entry, int level, int size)
            throws DatabaseException {
        boolean found = bsearch(entry.getPredicate(), level);

        if (level == 0)
            leafInsert(trans, (LeafEntry) entry, size, found);
        else
            branchInsert(trans, (BranchEntry) entry, size, search.index, found);
    }

    private void leafInsert(Transaction trans, LeafEntry entry, int size,
                            boolean found) throws DatabaseException {
        int loc;
        LeafEntry foundEntry = null;
        long holder = 0;

        if (found) {
            // handle various cases of transaction isolation resulting from
            // inserting over an existing key
            foundEntry = (LeafEntry) search.value;
            if (!RowHeader.isDeleted(foundEntry.getRowHeaderFlags()))
                throw new UniqueConstraintException("Key already exists.");

            if (RowHeader.isLocked(foundEntry.getRowHeaderFlags())) {
                holder = foundEntry.getLockHolder();
                if (holder != trans.getTransId() && !trans.isCommitted(holder))
                    throw new UniqueConstraintException("Key already exists.");

                if (!transactionManager.isUniversallyCommitted(holder))
                    (entry).setRowHeaderFlags(RowHeader.setReplace((entry)
                                                                           .getRowHeaderFlags(), true));
            }

            // Set the row status to NOT DELETED
            setDeleted((short) search.index, false);
        } else
            insertAt(search.index, size);

        loc = getLocation(search.index);

        if (foundEntry != null
                && RowHeader.isReplace(entry.getRowHeaderFlags())) {
            // Write UNDO info, which is the existing entry plus trans id of
            // previous holder
            ByteBuffer bb = attachLogicalUndo(UNDO_LEAF_INSERT, size + 8)
                    .getData();
            foundEntry.writeTo(bb);
            bb.putLong(holder);
        } else
            // Write UNDO info, which is the new entry so it can be found and
            // removed if necessary
            entry.writeTo(attachLogicalUndo(UNDO_LEAF_INSERT, size).getData());

        // Write REDO info, which is the entry, its index id, and whether it
        // replaces an existing entry
        entry.writeTo(attachRedo(REDO_LEAF_INSERT, size + 3).getData()
                              .putShort((short) search.index).put((byte) (found ? 1 : 0)));

        // Flush the log records and set the log address in the entry to support
        // versioning
        (entry).setUndo(writeUndoRedo(trans));

        // insert the entry
        pageBuffer.put(loc, entry);

        // Lock the entry, which registers this transaction as its owner
        lock(trans, search.index);
    }

    private void branchInsert(Transaction trans, BranchEntry entry, int size,
                              int index, boolean found) {
        if (found)
            throw new RuntimeException(
                    "Internal Error: unexpected attempt to insert duplicate separator into a branch node.");

        insertAt(index, size);
        int loc = getLocation(index);

        // Write UNDO info, which is the index of the new key so it can be found
        // and removed if necessary
        attachUndo(UNDO_BRANCH_INSERT, 2).getData().putShort((short) index);

        // Write REDO info, which is the entry and its index id
        entry.writeTo(attachRedo(REDO_BRANCH_INSERT, size + 2).getData()
                              .putShort((short) index));

        // insert the entry
        pageBuffer.put(loc, entry);
    }

    private void pageRemove(Transaction trans, Predicate query, int level)
            throws DatabaseException {
        if (!bsearch(query, level))
            return;

        if (level == 0)
            leafRemove(trans, query, (LeafEntry) search.value, search.index);
        else
            branchRemove(trans, query, level, search.index);
    }

    /**
     * Mark the entry as deleted, set replace = false if previous version will
     * never again be needed, lock the entry with the current transaction, and
     * record in the entry the undo pointer needed to reconstruct the previous
     * version if it might be needed.
     */
    private void leafRemove(Transaction trans, Predicate query,
                            LeafEntry entry, int index) throws DatabaseException {
        int loc = getLocation(index);
        if (RowHeader.isDeleted(pageBuffer, loc))
            return;

        // Get the current lock holder, if needed
        long previousHolder = 0;
        if (RowHeader.isLocked(entry.getRowHeaderFlags())) {
            if (!transactionManager.isUniversallyCommitted(entry
                                                                   .getLockHolder()))
                previousHolder = entry.getLockHolder();
        }

        // Write UNDO info, which is the entry needed to undo the remove, plus
        // the previous holder
        ByteBuffer bb = attachLogicalUndo(UNDO_LEAF_REMOVE,
                                          entry.storeSize() + 8).getData();
        entry.writeTo(bb);
        bb.putLong(previousHolder);

        // Write REDO info, which is the index id
        attachRedo(REDO_LEAF_REMOVE, 2).getData().putShort((short) index);

        // Flush the log records and set the log address in the entry to support
        // versioning
        entry.setUndo(writeUndoRedo(trans));

        // overwrite the entry to store the undo pointer
        pageBuffer.put(loc, entry);

        // Set the row status to DELETED
        setDeleted((short) index, true);

        RowHeader.setReplace(pageBuffer, loc, previousHolder > 0);

        lock(trans, index);
    }

    private void branchRemove(Transaction trans, Predicate query, int level,
                              int index) throws DatabaseException {
        logBranchRemove(index, query);

        // merge branch node if empty
        int count = getCount();
        if (count == 0)
            merge(trans, tree.getRoot(), level);
        else {
            if (index == 0)
                adjustMinKey(trans, level);

            // lower the height of the tree if possible
            if (count == 1) {
                Root root = tree.getRoot();
                if (level == root.getLevel())
                    adjustRoot(trans, root);
            }
        }
    }

    private void logBranchRemove(int index, Predicate query) {
        removeAt(index);

        // Write UNDO info, which is the key needed to undo the remove
        ByteBuffer bb = attachUndo(UNDO_BRANCH_REMOVE, query.storeSize() + 2)
                .getData();
        bb.putShort((short) index);
        query.writeTo(bb);

        // Write REDO info, which is the index id
        attachRedo(REDO_BRANCH_REMOVE, 2).getData().putShort((short) index);
    }

    private void redoLeafRemove(ByteBuffer bb) {
        int loc = setDeleted(bb.getShort(), true);

        // make sure the row is not locked after the redo
        RowHeader.setLocked(pageBuffer, loc, false);

        // also make sure the replace flag is disabled
        RowHeader.setReplace(pageBuffer, loc, false);
    }

    private void undoLeafRemove(ByteBuffer bb) throws DatabaseException {
        Entry e = newEntry(0);
        e.readFrom(bb);
        undoLeafRemove(bb, e);
    }

    /**
     * Undo leaf remove. There are 2 cases to consider: 1) Undo remove, previous
     * insert is universally committed 2) Undo remove, previous insert is not
     * universally committed Case 1: previous insert is universally committed -
     * mark not deleted, unlocked, replace = false Case 2: previous insert is
     * not universally committed - mark not deleted, lock with previous holder
     */
    private void undoLeafRemove(ByteBuffer bb, Entry entry)
            throws DatabaseException {
        long previousHolder = bb.getLong();

        bsearch(entry.getPredicate(), 0);

        // Set the row status to NOT DELETED
        int loc = setDeleted((short) search.index, false);

        // Case 1: previous insert is universally committed
        if (previousHolder == 0
                || transactionManager.isUniversallyCommitted(previousHolder)) {
            RowHeader.setLocked(pageBuffer, loc, false);
            RowHeader.setReplace(pageBuffer, loc, false);
        } else {
            // insert the previous entry (which locks it with previous holder)
            pageBuffer.put(loc, entry);

            // The previous holder must still be in the list of transaction
            // entries on this
            // page since it is not yet universally committed, so the previous
            // lock holder
            // index attribute remains valid
        }
    }

    private void redoBranchRemove(ByteBuffer bb) {
        removeAt(bb.getShort());
    }

    private void undoBranchRemove(ByteBuffer bb) throws DatabaseException {
        BranchEntry entry = new BranchEntry(allocatePredicate());

        short index = bb.getShort();
        entry.getPredicate().readFrom(bb);

        insertAt(index, entry.storeSize());
        int loc = getLocation(index);

        pageBuffer.put(loc, entry);
    }

    private void redoLeafInsert(ByteBuffer bb) throws DatabaseException {
        short index = bb.getShort();
        boolean replace = bb.get() == 1;
        int size = bb.remaining();
        LeafEntry entry = new LeafEntry(allocatePredicate());
        entry.readFrom(bb);

        if (replace)
            setDeleted(index, false);
        else
            insertAt(index, size);

        int loc = getLocation(index);

        // insert the entry
        pageBuffer.put(loc, entry);

        // make sure the row is not locked after the redo
        RowHeader.setLocked(pageBuffer, loc, false);

        // also make sure the replace flag is disabled
        RowHeader.setReplace(pageBuffer, loc, false);
    }

    // Entry point for redo of undo of leaf insert
    private void undoLeafInsert(ByteBuffer bb) throws DatabaseException {
        Entry e = newEntry(0);
        e.readFrom(bb);
        undoLeafInsert(bb, e);
    }

    /**
     * Undo leaf insert. There are 3 cases to consider here: 1) Undo insert, no
     * previous entry 2) Undo insert, previous delete is universally committed
     * 3) Undo insert, previous delete is not universally committed Case 1: no
     * previous entry - mark deleted, unlocked, replace = false Case 2: previous
     * delete is universally committed - mark deleted, unlocked, replace = false
     * Case 3: previous delete is not universally committed - insert the
     * previous entry, increment delete count
     */
    private void undoLeafInsert(ByteBuffer bb, Entry entry)
            throws DatabaseException {
        long previousHolder = 0;
        if (bb.remaining() > 0)
            previousHolder = bb.getLong();

        bsearch(entry.getPredicate(), 0);
        int loc = getLocation(search.index);

        // Cases 1 and 2: no previous entry, or previous delete is universally
        // committed
        if (previousHolder == 0
                || transactionManager.isUniversallyCommitted(previousHolder)) {
            RowHeader.setLocked(pageBuffer, loc, false);
            RowHeader.setReplace(pageBuffer, loc, false);
        } else {
            // Reinsert the previous entry if there was one there for which a
            // prior version
            // could still be observed (which would require an undo of the
            // deletion).
            // Assume the size is the same since the value is a fixed-size
            // pointer.
            // TODO: if the value is not a pointer, deal with space allocation
            // during undo.

            // insert the previous entry
            pageBuffer.put(loc, entry);

            // The previous holder must still be in the list of transaction
            // entries on this
            // page since it is not yet universally committed, so the previous
            // lock holder
            // index attribute remains valid
        }

        // In any case, maintain the delete count
        setDeleted((short) search.index, true);
    }

    private void redoBranchInsert(ByteBuffer bb) throws DatabaseException {
        short index = bb.getShort();
        int size = bb.remaining();
        BranchEntry entry = new BranchEntry(allocatePredicate());
        entry.readFrom(bb);

        insertAt(index, size);

        int loc = getLocation(index);
        pageBuffer.put(loc, entry);
    }

    private void undoBranchInsert(ByteBuffer bb) {
        removeAt(bb.getShort());
    }

    private void createNewRoot(Transaction trans, Root root)
            throws DatabaseException {
        AbstractNode newRootNode = getSubNode();

        FilePage newPage = tree.allocatePage(trans, newRootNode);
        try {
            newRootNode.writePin(newPage, false);

            newRootNode.initRoot(trans, root.getPage(), root.getLevel());

            root.setPage(newPage);
            root.setLevel(root.getLevel() + 1);
            tree.registerRoot(trans, root);
        } finally {
            newRootNode.unPin(BufferPool.Affinity.LIKED);
        }
    }

    private void truncate(int newCount) {
        attachUndo(UNDO_TRUNCATE, 2).getData().putShort(dir.getCount());

        dir.setCount(newCount);

        attachRedo(REDO_TRUNCATE, 2).getData().putShort((short) newCount);
    }

    private void redoTruncate(ByteBuffer bb) {
        short count = bb.getShort();
        dir.setCount(count);
    }

    private void redoReplaceBoundingPredicate(ByteBuffer bb)
            throws DatabaseException {
        Entry e = newEntry(0);
        e.readFrom(bb);
        dir.replaceAt(0, (short) e.storeSize());
        pageBuffer.put(dir.loc(0), e);
    }

    private int ix(int index) {
        return index + 1;
    }

    /**
     * Advance to the next key in a forward or backward scan of leaf pages.
     */
    private boolean advance(Transaction trans, Cursor cursor, boolean reverse)
            throws DatabaseException {
        while (true) {
            int index = cursor.getIndex() + (reverse ? -1 : 1);
            if (index >= 0 && index < getCount()) {
                cursor.setIndex(index);
                return true;
            }

            // move to the next page if there is one and try again
            if (!moveToNextPage(trans, cursor, reverse))
                return false;
        }
    }

    private boolean observable(Transaction trans, LeafEntry entry, Cursor cursor)
            throws StorageException {
        long holder = 0;
        if (RowHeader.isLocked(entry.getRowHeaderFlags()))
            holder = entry.getLockHolder();

        return observable(trans, entry, cursor, holder);
    }

    /**
     * Return true if the entry is observable by the transaction and cursor.
     * Also, interpret or return a prior version of the entry if necessary.
     */
    private boolean observable(Transaction trans, LeafEntry entry,
                               Cursor cursor, long holder) throws StorageException {
        // If the entry is not locked then it must be observable if not deleted
        if (!RowHeader.isLocked(entry.getRowHeaderFlags()))
            return !RowHeader.isDeleted(entry.getRowHeaderFlags());

        // If the transaction holding the lock is this transaction and the
        // version
        // is prior to the cursor stability point then it is observable if not
        // deleted.
        if (holder == trans.getTransId()) {
            if (entry.getUndo().getLSN() <= cursor.getCusp())
                return !RowHeader.isDeleted(entry.getRowHeaderFlags());
        } else if (trans.isCommitted(holder)) {
            // If the entry is committed with respect to the start of this
            // transaction,
            // then it is observable if not deleted.
            return !RowHeader.isDeleted(entry.getRowHeaderFlags());
        }

        // A prior version is needed:

        // Try to interpret the prior version first before visiting the log
        // history:
        // If the "replace" flag is false, meaning that the prior version is
        // known
        // to be committed to all transactions, then return true if the current
        // version is deleted (meaning that the prior version was NOT deleted)
        // and
        // return false if the current version is not deleted (meaning that the
        // prior version WAS deleted or that no prior version existed).
        if (!RowHeader.isReplace(entry.getRowHeaderFlags()))
            return RowHeader.isDeleted(entry.getRowHeaderFlags());

        // The replace flag is true, which means the previous version is
        // not guaranteed to be committed with respect to this transaction, so:

        // Reconstruct the prior version from the logs
        long previousHolder = getPreviousVersion(trans, entry);

        // Return observable status of the prior version
        return observable(trans, entry, cursor, previousHolder);
    }

    private long getPreviousVersion(Transaction trans, LeafEntry entry)
            throws StorageException {
        // retrieve the previous version of the entry, from log records
        UndoLog log = new UndoLog();
        log.setAddress(entry.getUndo());
        try {
            transactionManager.readUndo(log);
        } catch (Exception e) {
            throw new StorageException(
                    "Failed to retrieve previous version from log.");
        }
        ByteBuffer bb = log.getData().getData();
        entry.readFrom(bb);
        if (bb.remaining() > 0)
            return bb.getLong();
        return 0;
    }

    private boolean moveToNextPage(Transaction trans, Cursor cursor,
                                   boolean reverse) throws DatabaseException {
        if (reverse)
            getPrevPage(cursor.getLeaf());
        else
            getNextPage(cursor.getLeaf());

        int count = getCount();
        int deleteCount = getDeleteCount();
        FilePage page = getPage();

        // since this is part of a scan, hint that we probably won't be needing
        // this page again for awhile
        unPin(BufferPool.Affinity.HATED);

        // housecleaning
        if (deleteCount > 0 || count == 0)
            compactMerge(trans, page);

        if ((cursor.getLeaf()).isNull())
            return false;

        readPin(cursor.getLeaf());

        cursor.setIndex(reverse ? getCount() : -1);
        cursor.setPageLSN(getPageLSN());

        return true;
    }

    private void compactMerge(Transaction trans, FilePage leaf)
            throws DatabaseException {
        try {
            logPin(leaf);

            compact();

            if (getCount() > 0)
                return;

            merge(trans, tree.getRoot(), 0);
        } finally {
            unPin(trans, BufferPool.Affinity.HATED);
        }
    }

    private boolean bsearch(Predicate query, int level)
            throws DatabaseException {
        if (level == 0) {
            if (leafSearch == null)
                leafSearch = new BinarySearch.Search(newEntry(level),
                                                     newEntry(level));

            search = leafSearch;
        } else {
            if (branchSearch == null)
                branchSearch = new BinarySearch.Search(newEntry(level),
                                                       newEntry(level));

            search = branchSearch;
        }

        ((Entry) search.key).setPredicate(query);
        return BinarySearch.bSearch(search, this, entryComparator);
    }

    private void updateNextPage(Transaction trans, FilePage node,
                                FilePage nextPage) throws DatabaseException {
        try {
            logPin(node);

            updateNextPage(trans, nextPage);
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    private void updatePrevPage(Transaction trans, FilePage node,
                                FilePage prevPage) throws DatabaseException {
        try {
            logPin(node);

            updatePrevPage(trans, prevPage);
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    /**
     * Replace the first key on the page with the node's bounding predicate
     * (assume branch node only), then replace the bounding predicate in the
     * child node. This is safe since it only expands the range covered by the
     * child.
     */
    private void adjustMinKey(Transaction trans, int level)
            throws DatabaseException {
        if (level <= 0)
            return;

        // remove the current minsep key
        BranchEntry minsep = (BranchEntry) newEntry(level);
        get(0, minsep);
        logBranchRemove(0, minsep.getPredicate());

        // read the bounding predicate into the minsep key, then insert back
        // onto the page as new minsep key
        getBoundingPredicate(minsep.getPredicate());

        // make sure we have room on the page to fit the adjusted minsep key
        int esize = minsep.storeSize();
        if (!branchCanHold(esize)) {
            // split the node into 2 nodes, then repin the node
            FilePage node = getPage();
            split(trans, tree.getRoot(), level);
            logPin(node);
        }

        // now insert the adjusted minsep key
        branchInsert(trans, minsep, esize, 0, false);

        unPin(trans, BufferPool.Affinity.LIKED);

        // finally replace the child's bounding predicate with the new minsep
        // key
        adjustBoundingPredicate(trans, (FilePage) minsep.getPointer(), minsep
                .getPredicate(), level - 1);
    }

    /**
     * Replace the bounding predicate on the node pointed to by the entry. If
     * the node is a branch node, then adjust the minimum key as well.
     */
    private void adjustBoundingPredicate(Transaction trans, FilePage node,
                                         Predicate bp, int level) throws DatabaseException {
        try {
            logPin(node);

            // read the old bounding predicate
            Entry oldEntry = newEntry(0);
            getBoundingPredicate(oldEntry.getPredicate());
            int oldsz = oldEntry.storeSize();

            // make sure we have room on the page to fit the adjusted bounding
            // predicate
            Entry entry = newEntry(0);
            entry.setPredicate(bp);
            int esize = entry.storeSize();
            if (esize > oldsz) {
                if (!canHold(esize - oldsz, level)) {
                    // split the node into 2 nodes, then repin the node
                    split(trans, tree.getRoot(), level);
                    logPin(node);
                }
            }

            // now replace the adjusted bounding predicate, and log the action
            oldEntry.writeTo(attachUndo(UNDO_REPLACE_BP, oldsz).getData());

            dir.replaceAt(0, (short) esize);
            pageBuffer.put(dir.loc(0), entry);

            entry.writeTo(attachRedo(REDO_REPLACE_BP, esize).getData());

            if (level > 0)
                adjustMinKey(trans, level);
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }
    }

    /**
     * Lower the height of the tree if possible.
     */
    private void adjustRoot(Transaction trans, Root root)
            throws DatabaseException {
        if (root.getLevel() == 0)
            return;

        unPin(trans, BufferPool.Affinity.LIKED);

        try {
            logPin(root.getPage());

            int count = getCount();
            if (count != 1)
                return;

            Entry e = newEntry(root.getLevel());
            get(0, e);

            root.setLevel(root.getLevel() - 1);
            root.setPage((FilePage) e.getPointer());

            tree.freePage(trans, this);
            tree.registerRoot(trans, root);
        } finally {
            unPin(trans, BufferPool.Affinity.LIKED);
        }

        adjustRoot(trans, root);
    }

    public void print(PrintStream out, String linePrefix) {
        try {
            print(out, linePrefix, tree.getRoot().getPage(), tree.getRoot()
                    .getLevel(), true);
        } catch (DatabaseException e) {
            out.println("Error printing BTree: " + e.getMessage());
        }
    }

    private void print(PrintStream out, String linePrefix, FilePage nodePage,
                       int nodeLevel, boolean pin) throws DatabaseException {
        Entry entry = newEntry(nodeLevel);
        try {
            if (pin) {
                readPin(nodePage);
            }
            super.print(out, linePrefix);

            int count = dir.getCount();
            for (int i = 0; i < count; i++) {
                pageBuffer.get(dir.loc(i), entry);
                out.println(linePrefix + "Entry" + i + ":" + entry);
            }
        } finally {
            // branch nodes are more likely to be needed, especially the
            // root page
            if (pin) {
                unPin(BufferPool.Affinity.HATED);
            }
        }
    }

    private class EntryComparator implements Comparator<Entry> {
        public int compare(Entry o1, Entry o2) {
            return AbstractNode.this.compare(o1, o2);
        }
    }
}
