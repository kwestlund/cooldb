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

/**
 * Table defines methods to insert, update, remove and fetch rows from a data
 * store, and to create and obtain indexes on the data.
 * <p>
 * A <code>Table</code> can be obtained through a {@link Session}.
 *
 * @see Database
 * @see Session
 * @see TableCursor
 * @see Index
 * @see Row
 */

public interface Table {
    /**
     * Gets the name of this table.
     *
     * @return the name of this table.
     */
    String getName();

    /**
     * Allocates a <code>Row</code> container that can be used to insert,
     * remove, update, and fetch values stored in the table.
     *
     * @return a <code>Row</code> with the type structure of the table
     */
    Row allocateRow();

    /**
     * Inserts the row and returns its unique rowid.
     *
     * @param row the row to be inserted
     * @return the <code>RID</code> assigned to the new row
     * @throws UniqueConstraintException if the insert fails because of a unique key violation
     * @throws DatabaseException         if the insert fails for some other reason
     */
    RID insert(Row row) throws DatabaseException,
            UniqueConstraintException;

    /**
     * Deletes the row identified by the specified rowid.
     *
     * @param rowid the <code>RID</code> of the row to be deleted
     * @throws DatabaseException if the remove fails
     */
    void remove(RID rowid) throws DatabaseException;

    /**
     * Replaces the stored value of the row identified by the specified
     * <code>RID</code> with the value of the provided <code>Row</code>.
     *
     * @param rowid the <code>RID</code> of the row to be replaced
     * @param row   the new value of the row to replace the existing one
     * @throws DatabaseException if the update fails
     */
    void update(RID rowid, Row row) throws DatabaseException;

    /**
     * Fetches the row stored at the given rowid into the provided
     * <code>Row</code>.
     *
     * @param rowid the <code>RID</code> of the row to be fetched
     * @param row   the <code>Row</code> that will hold the returned value of
     *              the fetched row
     * @return true if the row is found, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    boolean fetch(RID rowid, Row row) throws DatabaseException;

    /**
     * Fetches the row stored at the given rowid into the provided
     * <code>Row</code> and locks the row exclusively for commit duration.
     *
     * @param rowid the <code>RID</code> of the row to be fetched
     * @param row   the <code>Row</code> that will hold the returned value of
     *              the fetched row
     * @return true if the row is found, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    boolean fetchForUpdate(RID rowid, Row row) throws DatabaseException;

    /**
     * Allocates a new TableCursor associated with this table.
     *
     * @return the new <code>TableCursor</code>
     */
    TableCursor allocateCursor();

    /**
     * Creates an index identified by the given key column map associated with
     * this table.
     *
     * @param keyMap   the key column map in which each byte specifies the position
     *                 of a column in this table
     * @param isUnique a flag that is true if the index key can uniquely identify
     *                 rows in this table
     * @return the new <code>Index</code>
     * @throws DatabaseException if the index already exists or cannot be created for some
     *                           other reason
     */
    Index createIndex(byte[] keyMap, boolean isUnique) throws DatabaseException;

    /**
     * Drops an index identified by the given key column map from the database.
     *
     * @param keyMap the key column map in which each byte specifies the position
     *               of a column in this table
     * @throws DatabaseException if the index does not exist or cannot be dropped for some
     *                           other reason
     */
    void dropIndex(byte[] keyMap) throws DatabaseException;

    /**
     * Gets an <code>Index</code>, identified by the given key column map,
     * that can be used to access rows in this table using the index key.
     *
     * @param keyMap the key column map in which each byte specifies the position
     *               of a column in this table
     * @return the <code>Index</code> or null if the <code>Index</code>
     * cannot be found
     * @throws DatabaseException if <code>getIndex</code> fails
     */
    Index getIndex(byte[] keyMap) throws DatabaseException;
}
