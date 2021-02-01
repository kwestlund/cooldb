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
 * <code>IndexCursor</code> supports keyed access to the rows of a table and in
 * particular supports arbitrarily bounded range scans.
 * <p>
 * An <code>IndexCursor</code> can be obtained through
 * {@link Index#allocateCursor}.
 * <p>
 * <code>IndexCursor</code> provides methods to scan forward or backward through
 * the rows of a table and to conditionally fetch, update, or remove those rows.
 * Scrolling forward returns rows in ascending order according to the natural
 * sort order of the index key while scrolling backward returns rows in
 * descending index key order.
 * <p>
 * <code>IndexCursor</code> provides methods {@link #lowerKey} and {@link #upperKey}
 * to set the lower and upper key bounds, respectively, on a range. {@link #setInclusive}
 * can be used to indicate whether each bound is inclusive or exclusive.
 * <p>
 * Before the cursor can be used, it must be opened by a call to {@link #open},
 * which begins a repeatable scan of the data store.
 * <p>
 * The methods {@link #rewind} and {@link #forward} position the cursor before
 * the first row or after the last row in the defined range.
 * <p>
 * The methods {@link #fetchNext} and {@link #fetchPrev} return the next row
 * pointed to by the cursor in ascending order and the previous row pointed to
 * by the cursor in descending order, respectively.
 * <p>
 * The methods {@link #removeCurrent} and {@link #updateCurrent} remove and
 * respectively update the row last returned by either of the fetch methods.
 * <p>
 * Closing the cursor by calling {@link #close} frees any resources held by the
 * cursor and invalidates it.
 * <p>
 * The cursor persists across transaction boundaries, but becomes invalid
 * following each Session commit or rollback until the next call to
 * <code>open</code>.
 * <p>
 * <code>IndexCursor</code> maintains read consistency in the presence of other
 * concurrent transactions that may be modifying the same rows returned by this
 * cursor. Fetch operations through the cursor will "see" only the following:
 * <ul>
 * <li>Committed updates made by transactions that committed before the start of
 * the current transaction.
 * <li>Uncommitted updates made by the current transaction before the cursor was
 * opened.
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
 *
 * @see Row
 * @see Index
 * @see TableCursor
 * @see Key
 */

public interface IndexCursor extends Cursor {
    /**
     * Opens this <code>IndexCursor</code> to begin a repeatable scan of the
     * data store in keyed order and sets the initial bounds for any subsequent
     * range scan at the minimum start key and maximum end key to include all
     * possible rows in the result set.
     * <p>
     * The cursor keeps a pointer to the last key returned by either
     * {@link #fetchNext} or {@link #fetchPrev} so that scrolling back and forth
     * through the data store is possible. Scrolling forward returns rows in
     * ascending order according to the natural sort order of the index key
     * while scrolling backward returns rows in descending index key order.
     * <p>
     * Like the <code>TableCursor</code>, this cursor maintains read consistency
     * in the presence of concurrent updates by effectively taking a snapshot of
     * the data store at the time the cursor is opened.
     * <p>
     * The cursor persists across transaction boundaries, but becomes invalid
     * following each Session commit or rollback until the next call to
     * <code>open</code>.
     */
    @Override
    void open() throws DatabaseException;

    /**
     * Gets the lower key, which can be modified to set the lower bound of the
     * range scan. The lower bound, by default, is the minimum possible value.
     *
     * @return the lower key in the range for this cursor
     */
    Key lowerKey();

    /**
     * Gets the upper key, which can be modified to set the upper bound of the
     * range scan. The upper bound, by default, is the maximum possible value.
     *
     * @return the upper key in the range for this cursor
     */
    Key upperKey();

    /**
     * Sets whether the lower and upper bounds are inclusive.
     *
     * @param lower true if the lower bound is inclusive (default = true)
     * @param upper true if the upper bound is inclusive (default = true)
     */
    void setInclusive(boolean lower, boolean upper);

    /**
     * Fetches into the provided <code>Row</code> the next row with a key value
     * that is greater than the one at the current cursor position and that
     * falls within the bounds of the range scan.
     *
     * @param row the <code>Row</code> that will hold the returned value of the
     *            fetched row
     * @return true if a row is found, false if at the end of the range
     * @throws DatabaseException if the fetch fails
     */
    @Override
    boolean fetchNext(Row row) throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the next row with a key value
     * that is greater than the one at the current cursor position, that falls
     * within the bounds of the range scan, and that passes the given filter.
     *
     * @param row    the <code>Row</code> that will hold the returned value of the
     *               fetched row
     * @param filter the <code>Filter</code> used to filter rows
     * @return true if a row is found to pass the filter, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    @Override
    boolean fetchNext(Row row, Filter filter) throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the next row with a key value
     * that is less than the one at the current cursor position and that falls
     * within the bounds of the range scan.
     *
     * @param row the <code>Row</code> that will hold the returned value of the
     *            fetched row
     * @return true if a row is found, false if at the end of the range
     * @throws DatabaseException if the fetch fails
     */
    boolean fetchPrev(Row row) throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the next row with a key value
     * that is less than the one at the current cursor position, that falls
     * within the bounds of the range scan, and that passes the given filter.
     *
     * @param row    the <code>Row</code> that will hold the returned value of the
     *               fetched row
     * @param filter the <code>Filter</code> used to filter rows
     * @return true if a row is found to pass the filter, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    boolean fetchPrev(Row row, Filter filter) throws DatabaseException;

    /**
     * Positions the cursor before the first row in the key range without
     * re-opening the cursor.
     */
    @Override
    void rewind();

    /**
     * Positions the cursor after the last row in the key range without
     * re-opening the cursor.
     */
    @Override
    void forward();
}
