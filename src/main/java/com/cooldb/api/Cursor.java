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
 * <code>Cursor</code> permits repeatable, scrollable, conditional fetch,
 * update and remove operations on the rows of a specific table.
 * <code>Cursor</code> extends both <code>RowStream</code> and
 * <code>Scrollable</code> with methods that permit the underlying source of
 * rows to be updated through a pointer to the current row.
 * <p>
 * The <code>Cursor</code> can be set <i>FOR UPDATE</i>, which causes any row
 * returned by one of the <i>fetch</i> methods to have been locked exclusively
 * for commit duration.
 * <p>
 * The methods {@link #removeCurrent} and {@link #updateCurrent} remove and
 * update, respectively, the last row returned by either of the fetch methods.
 *
 * @see Scrollable
 */

public interface Cursor extends Scrollable {

    /**
     * Gets the FOR UPDATE flag for this cursor.
     *
     * @return the FOR UPDATE flag
     */
    boolean isForUpdate();

    /**
     * Sets the FOR UPDATE flag for this cursor, which is false by default. If
     * set to true, then all subsequent rows returned by the cursor will be
     * locked exclusively for commit duration.
     *
     * @param isForUpdate the FOR UPDATE flag
     */
    void setForUpdate(boolean isForUpdate);

    /**
     * Deletes the row at the current cursor location.
     *
     * @throws DatabaseException if the remove fails
     */
    void removeCurrent() throws DatabaseException;

    /**
     * Updates the row at the current cursor location.
     *
     * @param row the row to replace the one at the current cursor location
     * @throws DatabaseException if the update fails
     */
    void updateCurrent(Row row) throws DatabaseException;

    /**
     * Gets the <code>RID</code> of the current row, which is the last one
     * fetched.
     *
     * @return the row identifier of the current row
     * @throws DatabaseException if there is no current row
     */
    RID getCurrentRID() throws DatabaseException;
}
