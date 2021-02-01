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
import com.cooldb.api.Database;
import com.cooldb.api.DatabaseException;
import com.cooldb.api.Session;
import com.cooldb.core.Core;
import com.cooldb.core.DBSequence;
import com.cooldb.core.TableDescriptor;
import com.cooldb.sort.Sort;
import com.cooldb.storage.Dataset;
import com.cooldb.transaction.Transaction;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DatabaseImpl implements Database {
    private final File path;
    private final List<SessionImpl> sessions = Collections.synchronizedList(new ArrayList<>());
    private Core core;
    private volatile boolean started;

    public DatabaseImpl(File path) {
        this.path = path;
        this.core = new Core(path);
    }

    @Override
    public File getPath() {
        return path;
    }

    @Override
    public synchronized boolean databaseExists() {
        return core.databaseExists();
    }

    @Override
    public synchronized boolean databaseInUse() {
        return core.databaseInUse();
    }

    @Override
    public synchronized void createDatabase() throws DatabaseException {
        createDatabase(0);
    }

    @Override
    public synchronized void createDatabase(long initialSize)
            throws DatabaseException {
        checkStarted(false);

        try {
            core.createDatabase((int) (initialSize / core.getPageSize()));
        } catch (Exception e) {
            throw new DatabaseException("Create Database failed", e);
        }

        started = true;
    }

    @Override
    public synchronized void replaceDatabase() throws DatabaseException {
        replaceDatabase(0);
    }

    @Override
    public synchronized void replaceDatabase(long initialSize)
            throws DatabaseException {
        checkStarted(false);

        try {
            core.createDatabase(true);
        } catch (Exception e) {
            throw new DatabaseException("Replace Database failed", e);
        }

        started = true;
    }

    @Override
    public synchronized void startDatabase() throws DatabaseException {
        if (!started) {
            try {
                core.startDatabase();
            } catch (Exception e) {
                throw new DatabaseException("Start Database failed", e);
            }
            started = true;
        }
    }

    @Override
    public boolean stopDatabase(long timeout) {
        if (started) {
            try {
                if (!core.stopDatabase(timeout))
                    return false;
            } catch (Exception e) {
                return false;
            }
            stopDatabase();
        }
        return true;
    }

    @Override
    public synchronized void stopDatabase() {
        if (started) {
            started = false;
            closeSessions();
            core.stopDatabase();
        }
    }

    @Override
    public synchronized Session createSession() throws DatabaseException {
        checkStarted(true);

        SessionImpl session = new SessionImpl(this);
        synchronized (sessions) {
            sessions.add(session);
        }
        return session;
    }

    public synchronized void destroyDatabase() {
        stopDatabase();
        core.destroyDatabase();
        core = null;
    }

    Transaction beginTransaction() throws DatabaseException {
        checkStarted(true);

        try {
            return core.getTransactionManager().beginTransaction();
        } catch (Exception e) {
            throw new DatabaseException("Begin Transaction failed", e);
        }
    }

    void commit(Transaction trans) throws DatabaseException {
        checkStarted(true);

        try {
            core.getTransactionManager().commitTransaction(trans);
        } catch (Exception e) {
            throw new DatabaseException("Commit Transaction failed", e);
        }
    }

    void rollback(Transaction trans) throws DatabaseException {
        checkStarted(true);

        try {
            core.getTransactionManager().rollback(trans, core);
            commit(trans);
        } catch (Exception e) {
            throw new DatabaseException("Rollback Transaction failed", e);
        }
    }

    Dataset createTable(Transaction trans, String name, String types)
            throws DatabaseException {
        checkStarted(true);

        try {
            return core.getTableManager().createTable(trans, name, types);
        } catch (Exception e) {
            throw new DatabaseException("Create Table failed", e);
        }
    }

    void dropTable(Transaction trans, String name) throws DatabaseException {
        checkStarted(true);

        try {
            core.getTableManager().dropTable(trans, name);
            synchronized (sessions) {
                for (SessionImpl session : sessions) session.didDropTable(name);
            }
        } catch (Exception e) {
            throw new DatabaseException("Drop Table failed", e);
        }
    }

    TableDescriptor getTableDescriptor(Transaction trans, String name)
            throws DatabaseException {
        checkStarted(true);

        try {
            return core.getTableManager().getTableDescriptor(trans, name);
        } catch (Exception e) {
            throw new DatabaseException("Get Table Descriptor failed", e);
        }
    }

    Dataset getTable(TableDescriptor td)
            throws DatabaseException {
        checkStarted(true);

        try {
            return core.getTableManager().getTable(td);
        } catch (Exception e) {
            throw new DatabaseException("Get Table failed", e);
        }
    }

    BTree createIndex(Transaction trans, String tableName, byte[] keyMap,
                      boolean isUnique) throws DatabaseException {
        checkStarted(true);

        try {
            return core.getTableManager().createIndex(trans, tableName, keyMap,
                                                      isUnique);
        } catch (Exception e) {
            throw new DatabaseException("Create Index failed", e);
        }
    }

    void dropIndex(Transaction trans, String tableName, byte[] keyMap)
            throws DatabaseException {
        checkStarted(true);

        try {
            core.getTableManager().dropIndex(trans, tableName, keyMap);
            synchronized (sessions) {
                for (SessionImpl session : sessions) session.didDropIndex(tableName, keyMap);
            }
        } catch (Exception e) {
            throw new DatabaseException("Drop Index failed", e);
        }
    }

    BTree getIndex(TableDescriptor td, byte[] keyMap) throws DatabaseException {
        checkStarted(true);

        try {
            return core.getTableManager().getIndex(td, keyMap);
        } catch (Exception e) {
            throw new DatabaseException("Get Index failed", e);
        }
    }

    DBSequence createSequence(Transaction trans, String name)
            throws DatabaseException {
        checkStarted(true);

        try {
            return core.getSequenceManager().createSequence(trans, name);
        } catch (Exception e) {
            throw new DatabaseException("Create sequence failed", e);
        }
    }

    void dropSequence(Transaction trans, String name) throws DatabaseException {
        checkStarted(true);

        try {
            core.getSequenceManager().dropSequence(trans, name);
        } catch (Exception e) {
            throw new DatabaseException("Drop sequence failed", e);
        }
    }

    DBSequence getSequence(Transaction trans, String name)
            throws DatabaseException {
        checkStarted(true);

        try {
            return core.getSequenceManager().getSequence(trans, name);
        } catch (Exception e) {
            throw new DatabaseException("Get sequence failed", e);
        }
    }

    Sort createSort() {
        return new Sort(core.getSortManager());
    }

    private void checkStarted(boolean s) throws DatabaseException {
        if (!s && started) {
            throw new DatabaseException(
                    "This database instance has been started.");
        } else if (s && !started) {
            throw new DatabaseException(
                    "This database instance has not been started.");
        }
    }

    private void closeSessions() {
        // close all sessions
        synchronized (sessions) {
            for (SessionImpl session : sessions) {
                session.close();
            }
            sessions.clear();
        }
    }
}
