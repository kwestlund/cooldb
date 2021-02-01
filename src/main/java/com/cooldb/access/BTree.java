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
import com.cooldb.api.Key;
import com.cooldb.api.impl.KeyImpl;
import com.cooldb.api.Row;
import com.cooldb.buffer.DBObject;
import com.cooldb.core.Column;
import com.cooldb.core.Core;
import com.cooldb.segment.Segment;
import com.cooldb.storage.Rowid;
import com.cooldb.transaction.Transaction;

/**
 * BTree is an implementation of a prefix B-Tree with support for ACID-compliant
 * transactions.
 */

public class BTree extends AbstractTree {
    private Entry entry;
    private Cursor cursor;
    private Key keytype;
    private byte[] keyMap;

    public BTree(Segment segment, Core core) throws DatabaseException {
        super(segment, core);
    }

    public synchronized void init(byte[] keyMap) {
        this.keyMap = keyMap;
    }

    public synchronized void insert(Transaction trans, Key key, Rowid value)
            throws DatabaseException {
        insert(trans, makeEntry(key, value));
    }

    public synchronized void remove(Transaction trans, Key key)
            throws DatabaseException {
        BTreePredicate p = makePredicate(key);
        p.setStartOp(BTreePredicate.EQ);
        remove(trans, p);
    }

    public synchronized Rowid fetch(Transaction trans, Key key)
            throws DatabaseException {
        BTreePredicate p = makePredicate(key);
        p.setStartOp(BTreePredicate.EQ);
        Cursor cursor = getCursor();
        openCursor(trans, cursor);
        return (Rowid) findFirst(trans, p, cursor, false);
    }

    public Key allocateKey() throws DatabaseException {
        if (keytype == null)
            keytype = makeKey();
        return (Key) keytype.copy();
    }

    public BTreePredicate allocatePredicate() throws DatabaseException {
        return new BTreePredicate(allocateKey(), allocateKey());
    }

    // Attachment implementation
    public void didInsert(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException {
        insert(trans, extractKey((Row) obj, rowid), rowid);
    }

    public void didRemove(Transaction trans, Rowid rowid, DBObject obj)
            throws DatabaseException {
        remove(trans, extractKey((Row) obj, rowid));
    }

    public void didUpdate(Transaction trans, DBObject obj, Rowid rowid,
                          DBObject old) throws DatabaseException {
        // no need to update the index if the key did not change
        Key oldKey = extractKey((Row) old, rowid);
        Key newKey = extractKey((Row) obj, rowid);
        if (!oldKey.equals(newKey)) {
            remove(trans, oldKey);
            insert(trans, newKey, rowid);
        }
    }

    public void print(String prefix) {
        print(System.out, prefix);
    }

    @Override
    AbstractNode newNode() {
        return new BTreeNode(this, core.getTransactionManager(),
                             allocPageBroker());
    }

    Entry makeEntry(Key key, Rowid value) throws DatabaseException {
        if (entry == null)
            entry = getNode().newEntry(0);
        BTreePredicate p = (BTreePredicate) entry.getPredicate();
        p.setStartKey(key);
        entry.setPointer(value);
        return entry;
    }

    BTreePredicate makePredicate(Key key) throws DatabaseException {
        if (entry == null)
            entry = getNode().newEntry(0);

        BTreePredicate p = (BTreePredicate) entry.getPredicate();
        p.setStartKey(key);

        return p;
    }

    Key makeKey() throws DatabaseException {
        TreeDescriptor td = (TreeDescriptor) getDescriptor();
        return new IndexKey(Column.createColumns(td.getKeyTypes()), td
                .isUnique() ? null : new Rowid());
    }

    Key extractKey(Row row, Rowid rowid) {
        TreeDescriptor td = (TreeDescriptor) getDescriptor();
        return new IndexKey(row.createKey(keyMap, td.isUnique() ? null : rowid));
    }

    Cursor getCursor() throws DatabaseException {
        if (cursor == null)
            cursor = allocateCursor();
        return cursor;
    }

    private static class IndexKey extends KeyImpl {
        IndexKey(Column[] cols, Rowid rowid) {
            super(cols, rowid);
        }

        IndexKey(Key key) {
            super((KeyImpl)key);
        }
    }
}
