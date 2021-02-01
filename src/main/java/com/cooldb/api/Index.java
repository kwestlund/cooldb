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
 * <code>Index</code> represents an external balanced-tree storage structure
 * that permits keyed access to an associated <code>Table</code>.
 * <p>
 * An <code>Index</code> and its external storage structure must initially be
 * created in the database through {@link Table#createIndex} after which it can
 * be obtained through {@link Table#getIndex}.
 * <p>
 * <code>Index</code> can be used to allocate a {@link Key} of the proper type
 * structure for this index. That key can then be set with a specific value and
 * used to {@link #fetch} the row containing that key value from the associated
 * table. Range scans, which facilitate access to all rows whose keys fall
 * within a specific range, are possible through an {@link IndexCursor}, which
 * can be can be obtained from this <code>Index</code> using
 * {@link #allocateCursor}.
 *
 * @see Row
 * @see Table
 * @see Key
 * @see IndexCursor
 */

public interface Index {
    /**
     * Allocates a <code>Key</code> that can be set and used as a key to fetch
     * corresponding rows from the indexed table.
     *
     * @return a <code>Key</code> with the proper type structure for this
     * index.
     * @throws DatabaseException if the key cannot be allocated
     */
    Key allocateKey() throws DatabaseException;

    /**
     * Fetches the row identified by the key into the provided <code>Row</code>.
     *
     * @param key the <code>Key</code> uniquely identifying the row to be
     *            fetched
     * @param row the <code>Row</code> that will hold the returned value of
     *            the fetched row
     * @return the <code>RID</code> of the row if found, null otherwise
     * @throws DatabaseException if the fetch fails
     */
    RID fetch(Key key, Row row) throws DatabaseException;

    /**
     * Fetches the row identified by the key into the provided <code>Row</code>
     * and locks the row exclusively for commit duration.
     *
     * @param key the <code>Key</code> uniquely identifying the row to be
     *            fetched
     * @param row the <code>Row</code> that will hold the returned value of
     *            the fetched row
     * @return the <code>RID</code> of the row if found, null otherwise
     * @throws DatabaseException if the fetch fails
     */
    RID fetchForUpdate(Key key, Row row) throws DatabaseException;

    /**
     * Allocates a new Cursor associated with this index that can be used
     * to perform range scans
     *
     * @return the new <code>IndexCursor</code>
     * @throws DatabaseException if the cursor cannot be allocated
     */
    IndexCursor allocateCursor() throws DatabaseException;
}
