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
import com.cooldb.core.DBSequence;
import com.cooldb.core.TableDescriptor;
import com.cooldb.sort.Sort;
import com.cooldb.transaction.Transaction;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class SessionImpl implements Session {

    private final List<TableImpl> tables = Collections.synchronizedList(new ArrayList<>());
    private DatabaseImpl database;
    private Transaction trans;
    private boolean serializable;
    private Sort sort;

    SessionImpl(DatabaseImpl database) {
        this.database = database;
    }

    @Override
    public boolean isSerializable() {
        return serializable;
    }

    @Override
    public void setSerializable(boolean serializable) {
        this.serializable = serializable;
        if (trans != null)
            trans.setSerializable(serializable);
    }

    @Override
    public void commit() throws DatabaseException {
        if (trans != null) {
            getDatabase().commit(trans);
            trans = null;
        }
    }

    @Override
    public void rollback() throws DatabaseException {
        if (trans != null) {
            getDatabase().rollback(trans);
        }
    }

    @Override
    public Table createTable(String name, String types)
            throws DatabaseException {
        validateTypes(types);
        TableImpl table = new TableImpl(this, getDatabase().createTable(
                getTransaction(), name, types), name, types);
        tables.add(table);
        return table;
    }

    @Override
    public void dropTable(String name) throws DatabaseException {
        getDatabase().dropTable(getTransaction(), name);
    }

    @Override
    public Table getTable(String name) throws DatabaseException {
        // Return the table from our list of tables if present already
        synchronized (tables) {
            for (TableImpl table : tables) {
                if (table.name.equals(name))
                    return table;
            }
        }
        TableDescriptor td = getDatabase().getTableDescriptor(getTransaction(),
                                                              name);
        if (td == null) {
            return null;
        }
        TableImpl table = new TableImpl(this, getDatabase().getTable(td), name, td.getTypes());
        tables.add(table);
        return table;
    }

    @Override
    public Sequence createSequence(String name) throws DatabaseException {
        return new SequenceImpl(this, getDatabase().createSequence(
                getTransaction(), name));
    }

    @Override
    public void dropSequence(String name) throws DatabaseException {
        getDatabase().dropSequence(getTransaction(), name);
    }

    @Override
    public Sequence getSequence(String name) throws DatabaseException {
        DBSequence sequence = getDatabase().getSequence(getTransaction(), name);
        if (sequence == null) {
            return null;
        }
        return new SequenceImpl(this, sequence);
    }

    @Override
    public RowStream sort(RowStream input, SortDelegate options) throws DatabaseException {
        if (sort == null) {
            sort = getDatabase().createSort();
        }
        try {
            return sort.sort(getTransaction(), input, options);
        } catch (InterruptedException e) {
            throw new DatabaseException("Sort interrupted", e);
        }
    }

    BTree createIndex(String tableName, byte[] keyMap, boolean isUnique)
            throws DatabaseException {
        return getDatabase().createIndex(getTransaction(), tableName, keyMap,
                                         isUnique);
    }

    void dropIndex(String tableName, byte[] keyMap) throws DatabaseException {
        getDatabase().dropIndex(getTransaction(), tableName, keyMap);
    }

    BTree getIndex(String tableName, byte[] keyMap) throws DatabaseException {
        TableDescriptor td = getDatabase().getTableDescriptor(getTransaction(),
                                                              tableName);
        if (td == null) {
            return null;
        }
        return getDatabase().getIndex(td, keyMap);
    }

    synchronized Transaction getTransaction() throws DatabaseException {
        if (trans == null) {
            trans = getDatabase().beginTransaction();
            trans.setSerializable(serializable);
        }
        return trans;
    }

    synchronized DatabaseImpl getDatabase() throws DatabaseException {
        if (database == null) {
            throw new DatabaseException("Session closed.");
        }
        return database;
    }

    synchronized void close() {
        database = null;
        trans = null;
    }

    void didDropTable(String name) {
        synchronized (tables) {
            for (int i = 0; i < tables.size(); i++) {
                TableImpl table = tables.get(i);
                if (table.name.equals(name)) {
                    tables.remove(i);
                    table.drop();
                    break;
                }
            }
        }
    }

    void didDropIndex(String tableName, byte[] keyMap) {
        synchronized (tables) {
            for (TableImpl table : tables) {
                if (table.name.equals(tableName))
                    table.didDropIndex(keyMap);
            }
        }
    }

    private void validateTypes(String types) throws DatabaseException {
        if (types.length() > Byte.MAX_VALUE) {
            throw new DatabaseException("Too many columns (> " + Byte.MAX_VALUE
                                                + ")");
        }

        for (int i = 0; i < types.length(); i++) {
            char c = types.charAt(i);
            switch (c) {
                case 's':
                case 'S':
                case 'b':
                case 'i':
                case 'I':
                case 'l':
                case 'f':
                case 'd':
                case 'a':
                case 'A':
                    break;
                default:
                    throw new DatabaseException("Unrecognized type: '" + c + "'");
            }
        }
    }
}
