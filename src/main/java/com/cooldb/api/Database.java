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

package com.cooldb.api;

import java.io.File;

/**
 * <code>Database</code> is used to reference a specific database obtained from {@link CoolDB}.
 * <p>
 * <code>Database</code> provides methods to create and destroy a database, to
 * start and stop core services, and to create {@link Session} objects that can
 * be used to define, access and modify database structures within the context
 * of ACID-compliant transactions.
 * <p>
 * Use of a particular <code>Database</code> involves the following steps:
 * <ol>
 * <li>First, obtain a Database instance from {@link CoolDB#getDatabase}, then:
 * <li>Call <code>createDatabase</code> or <code>replaceDatabase</code> to
 * create and start a new database, or <code>startDatabase</code> to start an
 * existing database.
 * <li>Call <code>createSession</code> to create a <code>Session</code>(s) that
 * can be used to access and modify the database.
 * <li>Call <code>stopDatabase</code> to stop the database or
 * <code>destroyDatabase</code> to stop and destroy it.
 * </ol>
 *
 * @see CoolDB
 * @see Session
 * @see Table
 * @see TableCursor
 */

public interface Database {
    /**
     * Returns the location of the database in the file system
     *
     * @return path of this database in the file system
     */
    File getPath();

    /**
     * Does this database exist?
     *
     * @return true if the database exists, false otherwise
     * @throws DatabaseException if the method fails
     */
    boolean databaseExists() throws DatabaseException;

    /**
     * Is this database in use by another process?
     *
     * @return true if the database is in use by another process.
     * @throws DatabaseException if the method fails
     */
    boolean databaseInUse() throws DatabaseException;

    /**
     * Creates a minimally sized database and starts the database.
     *
     * @throws DatabaseException if the database already exists or cannot be created for some
     *                           other reason
     */
    void createDatabase() throws DatabaseException;

    /**
     * Creates and starts the database.
     *
     * @param initialSize The size of the database in bytes
     * @throws DatabaseException if the database already exists or cannot be created for some
     *                           other reason
     */
    void createDatabase(long initialSize)
            throws DatabaseException;

    /**
     * Creates a minimally sized database and starts the database, overwriting
     * any existing database files.
     *
     * @throws DatabaseException if the database cannot be created
     */
    void replaceDatabase() throws DatabaseException;

    /**
     * Creates and starts the database, overwriting any existing database files.
     *
     * @param initialSize The size of the database in bytes
     * @throws DatabaseException if the database cannot be created
     */
    void replaceDatabase(long initialSize)
            throws DatabaseException;

    /**
     * Starts the database core services and performs restart recovery.
     *
     * @throws DatabaseException if the database does not exist or cannot be started for some
     *                           other reason
     */
    void startDatabase() throws DatabaseException;

    /**
     * Performs a soft shutdown of the database by waiting for upto
     * <code>timeout</code> milliseconds for ongoing transactions to finish and
     * then by taking a final checkpoint prior to stopping core services.
     *
     * @param timeout the maximum time to wait for a soft shutdown
     * @return true if successful, false if soft shutdown fails
     */
    boolean stopDatabase(long timeout);

    /**
     * Performs a hard stop of all core database services without first waiting
     * for ongoing transactions to finish and without taking a final checkpoint.
     */
    void stopDatabase();

    /**
     * Creates a database Session.
     *
     * @return a new database session.
     * @throws DatabaseException if the session cannot be created
     */
    Session createSession() throws DatabaseException;
}
