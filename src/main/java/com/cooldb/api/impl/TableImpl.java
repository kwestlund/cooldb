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

import com.cooldb.access.BTree;
import com.cooldb.api.*;
import com.cooldb.storage.Dataset;
import com.cooldb.storage.Rowid;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TableImpl implements Table {

    final SessionImpl session;
    final String name;
    final String types;
    final Row tmprow;
    private final List<IndexImpl> indexes = Collections.synchronizedList(new ArrayList<>());
    private Dataset dataset;

    TableImpl(SessionImpl session, Dataset dataset, String name, String types) {
        this.session = session;
        this.dataset = dataset;
        this.name = name;
        this.types = types;
        tmprow = new RowImpl(this, types);
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public Row allocateRow() {
        return new RowImpl(this, types);
    }

    @Override
    public RID insert(Row row) throws DatabaseException {
        if (((RowImpl) row).table != this)
            throw new DatabaseException("Row does not belong to this table.");
        try {
            return getDataset().insert(session.getTransaction(), row);
        } catch (UniqueConstraintException ucv) {
            throw new UniqueConstraintException(
                    "Insert failed due to unique constraint violation.", ucv);
        } catch (Exception e) {
            throw new DatabaseException("Insert failed", e);
        }
    }

    @Override
    public void remove(RID rowid) throws DatabaseException {
        try {
            getDataset()
                    .remove(session.getTransaction(), (Rowid) rowid, tmprow);
        } catch (Exception e) {
            throw new DatabaseException("Remove failed", e);
        }
    }

    @Override
    public void update(RID rowid, Row row) throws DatabaseException {
        if (((RowImpl) row).table != this)
            throw new DatabaseException("Row does not belong to this table.");
        try {
            getDataset().update(session.getTransaction(), row, (Rowid) rowid,
                                tmprow);
        } catch (Exception e) {
            throw new DatabaseException("Update failed", e);
        }
    }

    @Override
    public boolean fetch(RID rowid, Row row) throws DatabaseException {
        if (((RowImpl) row).table != this)
            throw new DatabaseException("Row does not belong to this table.");
        try {
            return getDataset().fetch(session.getTransaction(), row,
                                      (Rowid) rowid);
        } catch (Exception e) {
            throw new DatabaseException("Fetch failed", e);
        }
    }

    @Override
    public boolean fetchForUpdate(RID rowid, Row row) throws DatabaseException {
        if (((RowImpl) row).table != this)
            throw new DatabaseException("Row does not belong to this table.");
        try {
            return getDataset().fetchForUpdate(session.getTransaction(), row,
                                               (Rowid) rowid);
        } catch (Exception e) {
            throw new DatabaseException("Fetch failed", e);
        }
    }

    @Override
    public TableCursor allocateCursor() {
        return new TableCursorImpl(this);
    }

    @Override
    public Index createIndex(byte[] keyMap, boolean isUnique)
            throws DatabaseException {
        IndexImpl index = new IndexImpl(this, session.createIndex(name, keyMap,
                                                                  isUnique), keyMap);
        indexes.add(index);
        return index;
    }

    @Override
    public void dropIndex(byte[] keyMap) throws DatabaseException {
        session.dropIndex(name, keyMap);
    }

    @Override
    public Index getIndex(byte[] keyMap) throws DatabaseException {
        // Return the index from our list of indexes if present already
        synchronized (indexes) {
            for (IndexImpl index : indexes) {
                if (Arrays.equals(index.keyMap, keyMap))
                    return index;
            }
        }
        BTree btree = session.getIndex(name, keyMap);
        if (btree == null)
            return null;

        IndexImpl index = new IndexImpl(this, btree, keyMap);
        indexes.add(index);
        return index;
    }

    synchronized Dataset getDataset() throws DatabaseException {
        if (dataset == null)
            throw new DatabaseException("Table dropped.");
        return dataset;
    }

    void didDropIndex(byte[] keyMap) {
        synchronized (indexes) {
            for (int i = 0; i < indexes.size(); i++) {
                IndexImpl index = indexes.get(i);
                if (Arrays.equals(index.keyMap, keyMap)) {
                    indexes.remove(i);
                    index.drop();
                    break;
                }
            }
        }
    }

    synchronized void drop() {
        dataset = null;

        synchronized (indexes) {
            for (IndexImpl index : indexes) {
                index.drop();
            }
            indexes.clear();
        }
    }
}
