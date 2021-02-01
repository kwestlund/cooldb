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

/**
 * This package is the API specification for the CoolDB database system.
 *
 * <H2>CoolDB API Overview</H2>
 * <ul>
 *     <li>{@link com.cooldb.api.CoolDB} -- singleton entry point; gives you a database given a file folder
 *     location</li>
 *     <li>{@link com.cooldb.api.Database} -- has create, start, stop database operations; gives you a session</li>
 *     <li>{@link com.cooldb.api.Session} -- provides transaction context; create, get, drop named tables and
 *     sequences</li>
 *     <li>{@link com.cooldb.api.Table} and {@link com.cooldb.api.TableCursor} -- insert, update, remove, and fetch
 *     rows from a table; create indexes on a table</li>
 *     <li>{@link com.cooldb.api.Index} and {@link com.cooldb.api.IndexCursor} -- provides keyed and range query
 *     access to the rows of an associated table</li>
 *     <li>{@link com.cooldb.api.Sequence} -- a named, persistent, sequence number generator</li>
 * </ul>
 *
 * <H3>CoolDB</H3>
 * <p>
 * {@link com.cooldb.api.CoolDB} provides an entry point to one or more cooldb databases. To get started, first use
 * {@link com.cooldb.api.CoolDB#getInstance()} to get the singleton CoolDB instance, then you can:
 * <ol>
 *     <li>Get a {@link com.cooldb.api.Database} associated with a file folder
 *     <li>Destroy a {@link com.cooldb.api.Database} associated with a file folder
 * </ol>
 *
 * <H3>Database</H3>
 * <p>
 * To run the database system, an application must first obtain a {@link com.cooldb.api.Database}
 * instance using {@link com.cooldb.api.CoolDB#getDatabase(java.io.File)} with the file system directory path of the
 * database. The <code>Database</code> instance can then be used to:
 * <ol>
 *     <li>Create a new database by invoking <code>createDatabase</code> or <code>replaceDatabase</code> or,
 *     <li>Start an existing database by invoking <code>startDatabase</code>
 *     <li>Create a {@link com.cooldb.api.Session}(s) by invoking <code>createSession</code> (see
 *     below
 *         for more on what to do with a <code>Session</code>)
 *     <li>Stop the running database instance by invoking <code>stopDatabase</code> or,
 * </ol>
 *
 * <H3>Sessions</H3>
 * <p>
 * Each <code>Session</code> instance provides methods to define, access and modify database structures within the
 * context
 * of a single ACID-compliant transaction. Since a <code>Session</code> defines a single transaction context that is not
 * thread-safe, a separate <code>Session</code> must be created for each transactional thread of execution operating
 * on the
 * database concurrently.
 * <p>
 *     Specifically, the <code>Session</code> can be used to:
 * <ul>
 *     <li>Set the transaction isolation level to Serializable (default is Read-Committed).
 *     <li><code>commit</code> and <code>rollback</code> the active transaction.
 *     <li>Create a new {@link com.cooldb.api.Table} in the database associated with a specific table
 *         name.
 *     <li>Drop an existing table and all of its contents from the database by name.
 *     <li>Access an existing <code>Table</code> by name.
 * </ul>
 *
 * <H3>Tables and Cursors</H3>
 *
 * <code>Table</code> defines methods to insert, update, remove and fetch {@link com.cooldb.api.Row}s
 * from a
 * specific data store and can be used to allocate instances of {@link com.cooldb.api.TableCursor}.
 * <p>
 *     <code>TableCursor</code> provides methods to scan forward or backward through the rows of the
 *     <code>Table</code> and
 *     to conditionally fetch, update, or remove those rows. The cursor maintains read consistency in the presence of
 *     other
 *     concurrent transactions.
 * <p>
 *     Fetch operations made through an instance of <code>TableCursor</code> will "see" only the following:
 * <ul>
 *     <li>Committed updates made by transactions that committed before the start of the cursor transaction.
 *     <li>Uncommitted updates made by the cursor transaction before the cursor was opened.
 * </ul>
 * <p>
 *     Fetch operations made through an instance of <code>TableCursor</code> will <i>not</i> see the following:
 * <ul>
 *     <li>Uncommitted updates made by transactions other than the cursor one.
 *     <li>Committed updates made by transactions that committed after the start of the cursor transaction.
 *     <li>Uncommitted updates made by the cursor transaction after the cursor was opened.
 * </ul>
 * <p>
 *     <code>Table</code> can also be used to create, drop, and access indexes associated with it.
 *
 * <H3>Indexing</H3>
 * <p>
 * An {@link com.cooldb.api.Index}
 * can be obtained from a <code>Table</code>. First, an <code>Index</code> must be created, which will allocate and
 * intialize external storage for the index.
 * Subsequently, it can be retrieved for use in accessing the rows of the <code>Table</code>. The <code>Index</code> is
 * identified by a key map, which is simply an array of bytes in which each byte specifies the position of a column
 * in the
 * table that will contribute to the value of the composite index key in the order specified in the key map. The
 * specified
 * key need not uniquely identify rows if the uniqueness property is set to false when the index is created. Null values
 * are also permitted within keys.
 * <p>
 *     Once created, an index is automatically maintained so that it remains consistent with the associated table as
 *     rows
 *     are inserted, updated, and deleted from the table.
 * <p>
 *     <code>Index</code> can be used to allocate a {@link com.cooldb.api.Key} of the proper type
 *     structure
 *     for the index. That key can then be set with a specific value and used to fetch the row containing that key value
 *     from the associated table. Range scans, which facilitate access to all rows whose keys fall within a specific
 *     range,
 *     are possible through an {@link com.cooldb.api.IndexCursor}, which can be can be
 *     obtained from
 *     the <code>Index</code>.
 * <p>
 *     <code>IndexCursor</code> supports keyed access to the rows of a table and in particular supports arbitrarily
 *     bounded
 *     range scans. It provides methods to scan forward or backward through the rows of a table and to conditionally
 *     fetch,
 *     update, or remove those rows. Scrolling forward returns rows in ascending order according to the natural sort
 *     order
 *     of the index key while scrolling backward returns rows in descending index key order.
 * <p>
 *     CoolDB indexes maintain read consistency and support recovery and isolation semantics in cooperation with their
 *     associated tables.
 *
 * <H3><A href="doc-files/examples.html">Examples</A></H3>
 *
 * <ul>
 *     <li><a href="doc-files/examples.html#example1">Example 1: Table Insert</a></li>
 *     <li><a href="doc-files/examples.html#example2">Example 2: Update and Delete</a></li>
 *     <li><a href="doc-files/examples.html#example3">Example 3: Indexing</a></li>
 * </ul>
 */
package com.cooldb.api;
