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

package com.cooldb.core;

import com.cooldb.access.BTree;
import com.cooldb.access.TreeDescriptor;
import com.cooldb.api.DatabaseException;
import com.cooldb.api.Key;
import com.cooldb.api.Row;
import com.cooldb.api.impl.RowImpl;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.segment.SpaceManager;
import com.cooldb.storage.Dataset;
import com.cooldb.storage.DatasetCursor;
import com.cooldb.storage.Rowid;
import com.cooldb.transaction.NestedTopAction;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;

/**
 * TableManager is a central point of control for creating, dropping, accessing,
 * and indexing tables.
 */

public class TableManager {
    private final Dataset tables;
    private final TransactionManager transactionManager;
    private final SegmentFactory segmentFactory;
    private final SpaceManager spaceManager;

    TableManager(Dataset tables, TransactionManager transactionManager,
                 SegmentFactory segmentFactory, SpaceManager spaceManager) {
        this.tables = tables;
        this.transactionManager = transactionManager;
        this.segmentFactory = segmentFactory;
        this.spaceManager = spaceManager;
    }

    public synchronized Dataset createTable(Transaction trans, String name,
                                            String types) throws DatabaseException {
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            TableDescriptor arg = new TableDescriptor();
            TableDescriptor res = new TableDescriptor();
            arg.setName(name);
            DatasetCursor cursor = new DatasetCursor();
            tables.openCursor(trans, cursor);
            if (tables.fetchNext(trans, cursor, res, arg))
                throw new DatabaseException("Table already exists: " + name);

            Segment segment = spaceManager.createSegment(trans);

            // insert descriptor into tables dataset
            arg.setTypes(types);
            arg.setSegmentId(segment.getSegmentId());
            tables.insert(trans, arg);

            Dataset ds = (Dataset) segmentFactory.createSegmentMethod(trans,
                                                                      segment, Dataset.class);

            transactionManager.commitNestedTopAction(trans, savePoint);

            // grab exclusive table lock (after the commitNestedTopAction!)
            ds.writeLock(trans).lock();

            return ds;
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint,
                                                       segmentFactory);
            throw new DatabaseException("Failed to create table: " + name, e);
        }
    }

    public void dropTable(Transaction trans, String name)
            throws DatabaseException {
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            TableDescriptor arg = new TableDescriptor();
            TableDescriptor td = new TableDescriptor();
            arg.setName(name);
            DatasetCursor cursor = new DatasetCursor();
            tables.openCursor(trans, cursor);
            if (!tables.fetchNextForUpdate(trans, cursor, td, arg))
                throw new DatabaseException("Table does not exist: " + name);

            Dataset dataset = (Dataset) segmentFactory.getSegmentMethod(td
                                                                                .getSegmentId());

            // Grab the table lock exclusively
            dataset.writeLock(trans).lock();

            // Drop indexes
            while (td.getIndexCount() > 0)
                dropIndex(trans, dataset, td, cursor, td.getIndex(0)
                        .getKeyMap());

            segmentFactory.removeSegmentMethod(trans, dataset);

            // remove descriptor from tables dataset
            tables.remove(trans, cursor.getRowid(), arg);

            spaceManager.dropSegment(trans, dataset.getSegment());

            transactionManager.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint,
                                                       segmentFactory);
            throw new DatabaseException("Failed to drop table: " + name, e);
        }
    }

    public synchronized TableDescriptor getTableDescriptor(Transaction trans,
                                                           String name) throws DatabaseException {
        TableDescriptor arg = new TableDescriptor();
        TableDescriptor res = new TableDescriptor();
        arg.setName(name);
        DatasetCursor cursor = new DatasetCursor();
        tables.openCursor(trans, cursor);
        if (tables.fetchNext(trans, cursor, res, arg))
            return res;
        return null;
    }

    public Dataset getTable(TableDescriptor td)
            throws DatabaseException {
        Dataset dataset = (Dataset) segmentFactory.getSegmentMethod(td.getSegmentId());

        //noinspection SynchronizationOnLocalVariableOrMethodParameter
        synchronized (dataset) {
            // Load indexes if necessary
            if (dataset.getAttachmentCount() == 0) {
                for (int i = 0; i < td.getIndexCount(); i++)
                    dataset.attach(getIndex(td, td.getIndex(i).getKeyMap()));
            }
        }

        return dataset;
    }

    public BTree createIndex(Transaction trans, String tableName,
                             byte[] keyMap, boolean isUnique) throws DatabaseException {
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            // Lookup table descriptor
            TableDescriptor arg = new TableDescriptor();
            TableDescriptor td = new TableDescriptor();
            arg.setName(tableName);
            DatasetCursor cursor = new DatasetCursor();
            tables.openCursor(trans, cursor);
            if (!tables.fetchNextForUpdate(trans, cursor, td, arg))
                throw new DatabaseException("Table not found: " + tableName);

            // Make sure no index with the same keyMap already exists
            if (getIndex(td, keyMap) != null)
                throw new DatabaseException("Index already exists");

            // Save rowid for later
            Rowid tdrowid = (Rowid) cursor.getRowid().copy();

            // Create the index, inform it with key types, and record its
            // segmentId in the indexDescriptor
            Segment segment = spaceManager.createSegment(trans);
            IndexDescriptor id = new IndexDescriptor();
            id.setSegmentId(segment.getSegmentId());
            id.setKeyMap(keyMap);
            id.setUnique(isUnique);
            BTree bt = (BTree) segmentFactory.loadSegmentMethod(segment,
                                                                BTree.class);
            bt.init(keyMap);
            TreeDescriptor treeDescriptor = (TreeDescriptor) bt.getDescriptor();
            treeDescriptor.setKeyTypes(extractKeyTypes(keyMap, td.getTypes()));
            treeDescriptor.setUnique(isUnique);
            segmentFactory.createSegmentMethod(trans, bt);

            // Insert a key for each row in the table
            Dataset ds = getTable(td);

            // Grab the table lock exclusively
            ds.writeLock(trans).lock();

            ds.openCursor(trans, cursor);
            Row row = new TableRow(Column.createColumns(td.getTypes()));
            Key key = row.createKey(keyMap, (isUnique ? null : cursor
                    .getRowid()));
            while (ds.fetchNext(trans, cursor, row, null))
                bt.insert(trans, key, cursor.getRowid());

            // update the table descriptor to add the new index descriptor
            td.addIndex(id);
            tables.update(trans, td, tdrowid, arg);

            // Attach the index to the table
            ds.attach(bt);

            transactionManager.commitNestedTopAction(trans, savePoint);

            return bt;
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint,
                                                       segmentFactory);
            throw new DatabaseException("Failed to create index on table: "
                                                + tableName, e);
        }
    }

    public void dropIndex(Transaction trans, String tableName,
                          byte[] keyMap) throws DatabaseException {
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            TableDescriptor arg = new TableDescriptor();
            TableDescriptor td = new TableDescriptor();
            arg.setName(tableName);
            DatasetCursor cursor = new DatasetCursor();
            tables.openCursor(trans, cursor);
            if (!tables.fetchNextForUpdate(trans, cursor, td, arg))
                throw new DatabaseException("Table not found: " + tableName);

            dropIndex(trans, getTable(td), td, cursor, keyMap);

            transactionManager.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint,
                                                       segmentFactory);
            throw new DatabaseException("Failed to drop index from table: "
                                                + tableName, e);
        }
    }

    public BTree getIndex(TableDescriptor td, byte[] keyMap)
            throws DatabaseException {
        IndexDescriptor id = td.getIndex(keyMap);
        if (id == null)
            return null;

        BTree btree = (BTree) segmentFactory.getSegmentMethod(id.getSegmentId());
        btree.init(id.getKeyMap());

        return btree;
    }

    private void dropIndex(Transaction trans, Dataset ds, TableDescriptor td,
                           DatasetCursor tdCursor, byte[] keyMap) throws DatabaseException {
        // Make sure an index with the same keyMap exists
        BTree bt = getIndex(td, keyMap);
        if (bt == null)
            throw new DatabaseException("Index does not exist");

        // Grab the table lock exclusively
        ds.writeLock(trans).lock();

        // Detach the index from the table
        ds.detach(bt);

        // Remove the index from the segment factory
        segmentFactory.removeSegmentMethod(trans, bt);

        // update table descriptor
        td.removeIndex(keyMap);
        tables.update(trans, td, tdCursor.getRowid(), new TableDescriptor());

        // free the index segment
        spaceManager.dropSegment(trans, bt.getSegment());
    }

    private String extractKeyTypes(byte[] keyMap, String types) {
        char[] keyTypes = new char[keyMap.length];
        for (int i = 0; i < keyTypes.length; i++)
            keyTypes[i] = types.charAt(keyMap[i] & 0xff);
        return new String(keyTypes);
    }

    private static class TableRow extends RowImpl {
        TableRow(Column[] cols) {
            super(cols);
        }
    }
}
