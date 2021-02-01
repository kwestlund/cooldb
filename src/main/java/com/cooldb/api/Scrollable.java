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
 * <code>Scrollable</code> extends <code>RowStream</code> to permit backward
 * as well as forward scans of an underlying source of <code>Row</code>s.
 *
 * @see RowStream
 * @see Row
 */

public interface Scrollable extends RowStream {

    /**
     * Fetches into the provided <code>Row</code> the previous row (moving
     * <i>upstream</i>) in the stream of rows from the underlying source,
     *
     * @param row the <code>Row</code> that will hold the returned value of
     *            the fetched row
     * @return true if a row is found, false if there are no more rows.
     * @throws DatabaseException if the fetch fails
     */
    boolean fetchPrev(Row row) throws DatabaseException;

    /**
     * Fetches into the provided <code>Row</code> the previous row (moving
     * <i>upstream</i>) in the stream of rows from the underlying source that
     * passes the provided <code>filter</code>.
     *
     * @param row    the <code>Row</code> that will hold the returned value of
     *               the fetched row, if found
     * @param filter the <code>Filter</code> used to filter rows
     * @return true if a row is found to pass the filter, false otherwise
     * @throws DatabaseException if the fetch fails
     */
    boolean fetchPrev(Row row, Filter filter) throws DatabaseException;

    /**
     * Advances to the end of a <code>RowStream</code> to begin a backward
     * scan of the rows.
     *
     * @throws DatabaseException if the forward fails
     */
    void forward() throws DatabaseException;
}
