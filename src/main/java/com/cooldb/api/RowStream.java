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
 * <code>RowStream</code> defines methods to scan a source of <code>Row</code>s.
 * The <code>RowStream</code> supports repeatable, conditional forward-only
 * fetch operations that return rows one-by-one from the source. A
 * {@link #rewind} method is provided to allow scans to be repeated, and a
 * <code>Filter</code> can be provided to each invocation of {@link #fetchNext}
 * to filter rows at the lowest level possible.
 * <p>
 * Before <code>RowStream</code> can be used, it must be opened by a call to
 * {@link #open}, which begins a repeatable scan of the row source, and after
 * <code>RowStream</code> is no longer needed, it should be closed by calling
 * {@link #close}, which frees any resources held by the <code>RowStream</code>
 * and invalidates it.
 *
 * @see Row
 * @see RID
 */

public interface RowStream {

    /**
     * Allocates a <code>Row</code> container that can be used to fetch rows
     * from the RowStream.
     *
     * @return a <code>Row</code> with the type structure of the row stream
     */
    Row allocateRow();

    /**
     * Opens this <code>RowStream</code> to begin a repeatable scan of the data
     * store.
     *
     * @throws DatabaseException if the open fails
     */
    void open() throws DatabaseException;

    /**
     * Closes the <code>RowStream</code> and releases any resources associated
     * with it.
     *
     * @throws DatabaseException if the close fails
     */
    void close() throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the next row in the stream of
     * rows from the underlying source.
     *
     * @param row the <code>Row</code> that will hold the returned value of the
     *            fetched row
     * @return true if a row is found, false if there are no more rows.
     * @throws DatabaseException if the fetch fails
     */
    boolean fetchNext(Row row) throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the next row in the stream of
     * rows from the underlying source that passes the provided
     * <code>Filter</code>.
     *
     * @param row    the <code>Row</code> that will hold the returned value of the
     *               fetched row, if found
     * @param filter the <code>Filter</code> used to filter rows
     * @return true if a row is found to pass the filter, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    boolean fetchNext(Row row, Filter filter) throws DatabaseException;

    /**
     * Rewinds the <code>RowStream</code> to repeat a scan of the row source and
     * guarantees that the same set of rows will be returned with each scan even
     * if other transactions are simultaneously and independently modifying the
     * database.
     *
     * @throws DatabaseException if the rewind fails
     */
    void rewind() throws DatabaseException;
}
