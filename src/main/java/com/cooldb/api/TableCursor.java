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
 * <code>TableCursor</code> permits repeatable, scrollable, conditional fetch,
 * update and remove operations on the rows of a specific table.
 * <p>
 * A <code>TableCursor</code> can be obtained through
 * {@link Table#allocateCursor}.
 * <p>
 * <code>TableCursor</code> provides methods to scan forward or backward
 * through the rows of the table and to conditionally fetch, update, or remove
 * those rows.
 * <p>
 * By default, <code>TableCursor</code> maintains read consistency in the
 * presence of other concurrent transactions by returning the appropriate
 * version of each row, which may be a version prior to the current version.
 * However, a stricter level of of isolation can be achieved by setting the
 * cursor "FOR UPDATE", which returns the current version of each row already
 * locked exclusively for commit duration such that the row can safely be used
 * in a subsequent update operation.
 * <p>
 * Before the cursor can be used, it must be opened by a call to {@link #open},
 * which begins a repeatable scan of the data store.
 * <p>
 * The methods {@link #fetchNext} and {@link #fetchPrev} return the next row and
 * the previous row, pointed to by the cursor, respectively.
 * <p>
 * The methods {@link #removeCurrent} and {@link #updateCurrent} remove and
 * update, respectively, the last row returned by either of the fetch methods.
 * <p>
 * The methods {@link #rewind} and {@link #forward} position the cursor before
 * the first row or after the last row in the data store, respectively.
 * <p>
 * The cursor maintains read consistency in the presence of concurrent updates
 * by effectively taking a snapshot of the data store at the time the cursor is
 * opened.
 * <p>
 * The cursor persists across transaction boundaries, but becomes invalid
 * following each Session commit or rollback until the next call to
 * <code>open</code>.
 * <p>
 * Fetch operations through the cursor will "see" only the following:
 * <ul>
 * <li>Committed updates made by transactions that committed before the start
 * of the current transaction.
 * <li>Uncommitted updates made by the current transaction before the cursor
 * was opened.
 * </ul>
 * <p>
 * Fetch operations through the cursor will not see the following:
 * <ul>
 * <li>Uncommitted updates made by transactions other than the current one.
 * <li>Committed updates made by transactions that committed after the start of
 * the current transaction.
 * <li>Uncommitted updates made by the current transaction after the cursor was
 * opened.
 * </ul>
 * <p>
 * Closing the cursor by calling {@link #close} frees any resources held by the
 * cursor and invalidates it.
 *
 * @see Database
 * @see Session
 * @see Table
 * @see Row
 * @see RID
 */

public interface TableCursor extends Cursor {
    /**
     * Opens this <code>TableCursor</code> to begin a repeatable scan of the
     * data store. The cursor keeps a pointer to the last row returned by either
     * {@link #fetchNext} or {@link #fetchPrev} so that scrolling back and forth
     * through the data store is possible.
     * <p>
     * The cursor maintains read consistency in the presence of concurrent
     * updates by effectively taking a snapshot of the data store at the time
     * the cursor is opened.
     * <p>
     * The cursor persists across transaction boundaries, but becomes invalid
     * following each Session commit or rollback until the next call to
     * <code>open</code>.
     * <p>
     * Fetch operations through the cursor will "see" only the following:
     * <ul>
     * <li>Committed updates made by transactions that committed before the
     * start of the current transaction.
     * <li>Uncommitted updates made by the current transaction before the
     * cursor was opened.
     * </ul>
     * <p>
     * Fetch operations through the cursor will not see the following:
     * <ul>
     * <li>Uncommitted updates made by transactions other than the current one.
     * <li>Committed updates made by transactions that committed after the
     * start of the current transaction.
     * <li>Uncommitted updates made by the current transaction after the cursor
     * was opened.
     * </ul>
     */
    @Override
    void open() throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the row forward of the
     * current cursor position and moves the cursor position to point to that
     * row.
     * <p>
     * If this cursor is set FOR UPDATE, then the returned row is locked
     * exclusively for commit duration.
     *
     * @param row the <code>Row</code> that will hold the returned value of
     *            the fetched row
     * @return true if a row is found, false if there are no more rows.
     * @throws DatabaseException if the fetch fails
     */
    @Override
    boolean fetchNext(Row row) throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the next row passing the
     * specified <code>Filter</code>, starting at the current cursor
     * position.
     * <p>
     * If a matching row is found, sets the rowid in the cursor so that a
     * subsequent call to <code>fetchNext</code> will return the next row
     * matching the filter.
     * <p>
     * If this cursor is set FOR UPDATE, then the returned row is locked
     * exclusively for commit duration.
     *
     * @param row    the <code>Row</code> that will hold the returned value of
     *               the fetched row, if found
     * @param filter the <code>Filter</code> used to filter rows
     * @return true if a row is found to pass the filter, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    @Override
    boolean fetchNext(Row row, Filter filter) throws DatabaseException;

    /**
     * Fetches into the provided row the row previous to the current cursor
     * position and moves the cursor position to point to that row.
     * <p>
     * If this cursor is set FOR UPDATE, then the returned row is locked
     * exclusively for commit duration.
     *
     * @param row the <code>Row</code> that will hold the returned value of
     *            the fetched row
     * @return true if a row is found, false if there are no more rows.
     * @throws DatabaseException if the fetch fails
     */
    @Override
    boolean fetchPrev(Row row) throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the previous row passing the
     * specified <code>Filter</code>, starting at the current cursor
     * position.
     * <p>
     * <code>fetchPrev</code> is semantically equivalent to
     * <code>fetchNext</code> except that consecutive calls return rows in a
     * backward scan of the data store rather than in a forward scan. The
     * <code>forward</code> method can be used after opening a cursor to
     * position the cursor at the end of the data store to begin such a backward
     * scan.
     * <p>
     * If this cursor is set FOR UPDATE, then the returned row is locked
     * exclusively for commit duration.
     *
     * @param row    the <code>Row</code> that will hold the returned value of
     *               the fetched row, if found
     * @param filter the <code>Filter</code> used to filter rows
     * @return true if a row is found to pass the filter, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    @Override
    boolean fetchPrev(Row row, Filter filter) throws DatabaseException;
}
