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

package com.cooldb.storage;

import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.buffer.DBObject;
import com.cooldb.segment.SegmentMethod;
import com.cooldb.transaction.Transaction;

/**
 * StorageMethod defines methods to insert, remove, update, and fetch Tuples
 * from a data store.
 */

public interface StorageMethod extends SegmentMethod {

    /**
     * Insert the tuple and assign it a unique identifier, which is returned.
     */
    Rowid insert(Transaction trans, DBObject obj) throws DatabaseException,
            InterruptedException;

    /**
     * Delete the row identified by the specified rowid. The deleted row, if
     * there is one, is returned in the provided object. Returns false if the
     * row could not be found.
     */
    boolean remove(Transaction trans, Rowid rowid, DBObject obj)
            throws DatabaseException, InterruptedException;

    /**
     * Replace the value of the stored tuple using the specified rowid.
     */
    void update(Transaction trans, DBObject obj, Rowid rowid, DBObject old)
            throws DatabaseException, InterruptedException;

    /**
     * Read the stored value of the tuple from the database using the specified
     * Rowid.
     */
    boolean fetch(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException;

    /**
     * Read the stored value of the tuple from the database using the specified
     * Rowid and lock the row exclusively for commit duration.
     */
    boolean fetchForUpdate(Transaction trans, DBObject obj, Rowid rowid)
            throws DatabaseException, InterruptedException;

    /**
     * Open a cursor to begin a scan of the data store. The cursor keeps a
     * pointer to the last row returned by either fetchNext or fetchPrev so that
     * scrolling back and forth through the data store is possible.
     * <p>
     * The cursor maintains read consistency in the presence of concurrent
     * updates by effectively taking a snapshot of the data store at the time
     * the cursor is opened.
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
    void openCursor(Transaction trans, DatasetCursor cursor) throws DatabaseException;

    /**
     * Close the cursor and release any resources associated with it.
     */
    void closeCursor(DatasetCursor cursor) throws DatabaseException;

    /**
     * Fetch into the provided object the next row passing the specified Filter,
     * starting at the current cursor position.
     * <p>
     * If a row is found to pass the filter then update the cursor so that a
     * subsequent call to fetchNext will return the next row passing the Filter,
     * and return true; otherwise return false if there is no match.
     */
    boolean fetchNext(Transaction trans, DatasetCursor cursor, DBObject obj,
                      Filter filter) throws DatabaseException;

    /**
     * Fetch into the provided object the next row passing the specified Filter,
     * starting at the current cursor position, and lock the row exclusively for
     * commit duration.
     * <p>
     * If a row is found to pass the filter then update the cursor so that a
     * subsequent call to fetchNext will return the next row passing the Filter,
     * and return true; otherwise return false if there is no match.
     */
    boolean fetchNextForUpdate(Transaction trans, DatasetCursor cursor,
                               DBObject obj, Filter filter) throws DatabaseException,
            InterruptedException;

    /**
     * FetchPrev is semantically equivalent to fetchNext except that consecutive
     * calls return tuples in a backward scan of the data store rather than in a
     * forward scan. The forward method can be used after opening a cursor to
     * position the cursor at the end of the data store to begin such a backward
     * scan.
     */
    boolean fetchPrev(Transaction trans, DatasetCursor cursor, DBObject obj,
                      Filter filter) throws DatabaseException;

    /**
     * FetchPrevForUpdate is semantically equivalent to fetchNextForUpdate
     * except that consecutive calls return tuples in a backward scan of the
     * data store rather than in a forward scan. The forward method can be used
     * after opening a cursor to position the cursor at the end of the data
     * store to begin such a backward scan.
     */
    boolean fetchPrevForUpdate(Transaction trans, DatasetCursor cursor,
                               DBObject obj, Filter filter) throws DatabaseException,
            InterruptedException;

    /**
     * Position the cursor before the first tuple in the data store.
     */
    void rewind(DatasetCursor cursor) throws DatabaseException;

    /**
     * Position the cursor after the last tuple in the data store.
     */
    void forward(DatasetCursor cursor) throws DatabaseException;
}
