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
 * <code>Session</code> provides methods to access and modify a database
 * within the context of a single ACID-compliant transaction.
 * <p>
 * Each <code>Session</code> is associated with a single {@link Database}.
 * <p>
 * All session operations on the database occur within the context of the active
 * transaction. The active transaction exists implicitly, but operations on the
 * database must be explicitly committed or cancelled using the
 * <code>Session</code> methods <code>commit</code> and
 * <code>rollback</code> respectively. A new transaction automatically becomes
 * active and is associated with this <code>Session</code> when needed.
 * <p>
 * Since a <code>Session</code> defines a single transaction context that is
 * not thread-safe, a separate <code>Session</code> must be created for each
 * transactional thread of execution operating on the database concurrently.
 * <p>
 * Specifically, the <code>Session</code> can be used to:
 * <ul>
 * <li>Set the transaction isolation level.
 * <li><code>Commit</code> and <code>rollback</code> the active
 * transaction.
 * <li>Create, drop, or access a {@link Table} in the database associated with
 * a specific table name.
 * <li>Create, drop, or access a {@link Sequence} in the database associated
 * with a specific sequence name.
 * <li>Sort a row stream such as {@link TableCursor} such that the resulting row stream
 * is consistent with the current transaction.
 * </ul>
 *
 * @see Database
 * @see Table
 * @see Sequence
 * @see SortDelegate
 * @see RowStream
 */

public interface Session {
    /**
     * Gets the transaction isolation level.
     *
     * @return true if transactions run at serializable isolation, false if at
     * repeatable-read isolation (default: false)
     */
    boolean isSerializable();

    /**
     * Sets the transaction isolation level to serializable or repeatable-read.
     *
     * @param serializable if true, transactions run at serializable isolation, otherwise
     *                     at repeatable-read isolation
     */
    void setSerializable(boolean serializable);

    /**
     * Commits the current transaction.
     *
     * @throws DatabaseException if the commit fails
     */
    void commit() throws DatabaseException;

    /**
     * Cancels the current transaction. All changes to the database made by the
     * transaction are undone, then the transaction is committed as in
     * <code>commit</code>.
     *
     * @throws DatabaseException if the rollback fails
     * @see #commit
     */
    void rollback() throws DatabaseException;

    /**
     * Creates a new <code>Table</code> that can be used to insert, remove,
     * update, and fetch rows from a named data store.
     * <p>
     * The structure of the table must be specified as a list of column type
     * codes. The list of column type codes is specified as a
     * <code>String</code> of codes from the following list of codes:
     * <ul>
     * <li>s : small variable-length string (&lt;= 255 maximum UTF-8 bytes)
     * <li>S : big variable-length string (&gt; 255 maximum UTF-8 bytes)
     * <li>b : byte
     * <li>i : short integer (2 bytes)
     * <li>I : integer (4 bytes)
     * <li>l : long integer (8 bytes)
     * <li>d : double precision float (8 byte)
     * <li>a : small variable-length byte array (&lt;= 255 maximum bytes)
     * <li>A : big variable-length byte array (&gt; 255 maximum bytes)
     * </ul>
     *
     * @param name  the name that will be associated with the new table
     * @param types the column type codes that define the structure of the table
     * @return the newly created <code>Table</code>
     * @throws DatabaseException if the table already exists or cannot be created for some
     *                           other reason
     */
    Table createTable(String name, String types) throws DatabaseException;

    /**
     * Drops a <code>Table</code> and all of its contents from the database.
     *
     * @param name the name of the table to be dropped from the database
     * @throws DatabaseException if the table does not exist or cannot be dropped for some
     *                           other reason
     */
    void dropTable(String name) throws DatabaseException;

    /**
     * Gets a <code>Table</code> that can be used to insert, remove, update,
     * and fetch rows from the named data store.
     *
     * @param name the name of the table to be accessed
     * @return the <code>Table</code> or null if the <code>Table</code>
     * cannot be found
     * @throws DatabaseException if <code>getTable</code> fails
     */
    Table getTable(String name) throws DatabaseException;

    /**
     * Creates a new named <code>Sequence</code> that can be used to generate
     * unique numbers starting from zero and running to the maximum value for a
     * signed long integer, not necessarily consecutively.
     *
     * @param name the name that will be associated with the new table
     * @return the newly created <code>Sequence</code>
     * @throws DatabaseException if the sequence already exists or cannot be created for some
     *                           other reason
     */
    Sequence createSequence(String name) throws DatabaseException;

    /**
     * Drops a <code>Sequence</code> from the database.
     *
     * @param name the name of the sequence to be dropped from the database
     * @throws DatabaseException if the sequence does not exist or cannot be dropped for some
     *                           other reason
     */
    void dropSequence(String name) throws DatabaseException;

    /**
     * Gets the named <code>Sequence</code>, which can be used to generate
     * unique numbers.
     *
     * @param name the name of the sequence to be accessed
     * @return the <code>Sequence</code> or null if the <code>Sequence</code>
     * cannot be found
     * @throws DatabaseException if <code>getSequence</code> fails
     */
    Sequence getSequence(String name) throws DatabaseException;

    /**
     * Sorts the given row stream and returns the result as another row stream. The result is guaranteed to be
     * consistent within the current transaction.
     *
     * <p>Sort options can be applied through a delegate. Current options include:</p>
     * <dl>
     *     <dt>isDistinct</dt>
     *     <dd>Requires the sort output to be distinct. Duplicates are discarded during the sort so that the
     *     resulting output is guaranteed to be distinct. (default: false)</dd>
     *     <dt>isUnique</dt>
     *     <dd>Requires the sort input to be unique. An exception is generated and the
     *     sort is abandoned if duplicates are detected during the sort. (default: false)</dd>
     *     <dt>topN</dt>
     *     <dd>Restricts the sort output to the top N records. If zero, no restriction applies. (default: 0)</dd>
     * </dl>
     *
     * @param input   the row stream to be sorted
     * @param options the delegate providing sort options, or null for default behavior
     * @return the sorted row stream
     * @throws DatabaseException if the sort is interrupted or fails any uniqueness requirement
     */
    RowStream sort(RowStream input, SortDelegate options) throws DatabaseException;
}
