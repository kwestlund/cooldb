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
import com.cooldb.access.TreeDescriptor;
import com.cooldb.api.*;

public class IndexImpl implements Index {

    final TableImpl table;
    final byte[] keyMap;
    final boolean isUnique;
    BTree btree;
    String keyTypes;

    IndexImpl(TableImpl table, BTree btree, byte[] keyMap) throws DatabaseException {
        TreeDescriptor td = (TreeDescriptor) btree.getDescriptor();

        this.table = table;
        this.btree = btree;
        this.keyMap = keyMap;
        this.isUnique = td.isUnique();
        try {
            this.keyTypes = td.getKeyTypes();
        } catch (TypeException e) {
            throw new DatabaseException(
                    "Failed to get key types from tree descriptor.");
        }
    }

    @Override
    public Key allocateKey() throws DatabaseException {
        Key key = getTree().allocateKey();
        ((KeyImpl) key).index = this;
        return key;
    }

    @Override
    public RID fetch(Key key, Row row) throws DatabaseException {
        if (((KeyImpl) key).index != this)
            throw new DatabaseException("Key does not belong to this index.");
        try {
            RID rid = getTree().fetch(table.session.getTransaction(), key);
            if (rid != null && table.fetch(rid, row))
                return rid;
            return null;
        } catch (Exception e) {
            throw new DatabaseException("Fetch failed", e);
        }
    }

    @Override
    public RID fetchForUpdate(Key key, Row row) throws DatabaseException {
        if (((KeyImpl) key).index != this)
            throw new DatabaseException("Key does not belong to this index.");
        try {
            RID rid = getTree().fetch(table.session.getTransaction(), key);
            if (rid != null && table.fetchForUpdate(rid, row))
                return rid;
            return null;
        } catch (Exception e) {
            throw new DatabaseException("Fetch for-update failed", e);
        }
    }

    @Override
    public IndexCursor allocateCursor() throws DatabaseException {
        return new IndexCursorImpl(this);
    }

    /**
     * Print the contents of the index to the console.
     */
    @SuppressWarnings("SameParameterValue")
    void print(String prefix) {
        btree.print(prefix);
    }

    synchronized BTree getTree() throws DatabaseException {
        if (btree == null)
            throw new DatabaseException("Index dropped.");
        return btree;
    }

    synchronized void drop() {
        btree = null;
    }
}
