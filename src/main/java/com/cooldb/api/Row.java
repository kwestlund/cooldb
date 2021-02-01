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

import com.cooldb.buffer.DBObject;
import com.cooldb.sort.Sortable;
import com.cooldb.storage.Rowid;

/**
 * Row is an array of column values that can be stored in or fetched from a
 * specific <code>Table</code> of the database.
 * <p>
 * A <code>Row</code> can be obtained through a {@link Table} and remains
 * associated with that <code>Table</code> for the life of the <code>Row</code>.
 * It cannot be used with any other <code>Table</code>, and if the
 * <code>Table</code> becomes invalid, so does the row.
 * <p>
 * <code>Row</code> provides various type-specific methods to get/set column
 * values by the absolute positions of the columns within the row. There are
 * also methods provided to set any column to null or to its minimum or maximum
 * value. These are special values that reside outside the range of the column's
 * domain. <code>Row</code> will implicitly cast values where possible or throw
 * a TypeException if the type of the column at the specified position is
 * incompatible with the method type. In general, if a column is null then
 * casting to a string will return an empty string and casting to a number will
 * return zero. If a column that is null is set with some non-null value, then
 * the column is implicitly non-null. Null values compare higher than all values
 * other than the maximum value, which can only be set by invoking
 * {@link #setMaxValue}.
 *
 * @see Varchar
 * @see Table
 */

public interface Row extends DBObject, Sortable {
    /**
     * Creates a copy of this row
     *
     * @return a copy of this row
     */
    Row copyRow();

    /**
     * Copies the given row into this one
     *
     * @param row the row to be copied into this one
     */
    void assignRow(Row row);

    /**
     * Constructs a <code>Row</code> whose columns are a projection of the
     * columns of this row. This row is used as backing store for the new row,
     * so subsequent changes to this row will be reflected in the value of the
     * new row and vice versa.
     *
     * @param colMap the column map in which each byte specifies the position of a
     *               column in this row
     * @return a row whose columns are mapped to this row's columns
     */
    Row project(byte[] colMap);

    /**
     * Constructs a <code>Row</code> whose columns are a projection of the
     * columns of this row. This row is used as backing store for the new row,
     * so subsequent changes to this row will be reflected in the value of the
     * new row and vice versa. The keys parameter specifies the number of
     * columns, starting from the colMap zero offset, that are to be considered
     * key columns in the projected row.
     *
     * @param colMap  the column map in which each byte specifies the position of a
     *                column in this row
     * @param keyCols the number of columns that comprise the key of the returned
     *                row
     * @return a row whose columns are mapped to this row's columns
     */
    Row project(byte[] colMap, int keyCols);

    /**
     * Constructs a key that uniquely identifies this row, mapped to this row's
     * columns. If the rowid parameter is non-null, it will be used to ensure
     * the uniqueness of the key. This row and any rowid parameter are used as
     * backing store for the key, so subsequent changes to either the key
     * columns in the row or to the rowid object will be reflected in the value
     * of the key.
     *
     * @param colMap the key map in which each byte specifies the position of a
     *               column in this row
     * @param rowid  the rowid appended to the other key components to guarantee
     *               uniqueness
     * @return a key that uniquely identifies the row
     */
    Key createKey(byte[] colMap, Rowid rowid);

    /**
     * Compares the key columns of two rows.
     *
     * @param obj the row to compare this one with
     * @return (&lt; 0, 0, &gt; 0)
     */
    int compareKeyTo(Row obj);

    /**
     * Gets the number of columns in this row.
     *
     * @return the number of columns in this row.
     */
    int getColumnCount();

    /**
     * Gets the null indicator from the column at the specified position.
     *
     * @param pos the absolute position of the column
     * @return true if the value of the column is null
     */
    boolean isNull(int pos);

    /**
     * Sets the null indicator of the column at the specified position.
     *
     * @param pos the absolute position of the column
     * @param val the value of the null indicator
     */
    void setNull(int pos, boolean val);

    /**
     * Gets the sort direction for the column at the specified position.
     *
     * @param pos the absolute position of the column
     * @return the sort direction
     */
    Direction getDirection(int pos);

    /**
     * Sets the sort direction of the column at the specified position.
     *
     * @param pos           the absolute position of the column
     * @param sortDirection the sort direction
     */
    void setDirection(int pos, Direction sortDirection);

    /**
     * Sets the minimum value of the column at the specified position.
     *
     * @param pos the absolute position of the column
     */
    void setMinValue(int pos);

    /**
     * Sets the maximum value of the column at the specified position.
     *
     * @param pos the absolute position of the column
     */
    void setMaxValue(int pos);

    /**
     * Gets the Varchar column at the specified position.
     *
     * @param pos the absolute position of the column
     * @return the Varchar column object
     * @throws TypeException if the type of the column at the specified position in this
     *                       row is not a Varchar
     */
    Varchar getVarchar(int pos) throws TypeException;

    /**
     * Gets the value of the column at the given position cast as a
     * <code>String</code>
     *
     * @param pos the absolute position of the column
     * @return the value of the column at the given position cast as a
     * <code>String</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the return type
     */
    String getString(int pos) throws TypeException;

    /**
     * Gets the value of the column at the given position cast as a signed
     * decimal <code>byte</code>
     *
     * @param pos the absolute position of the column
     * @return the value of the column at the given position cast as a signed
     * decimal <code>byte</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the return type
     */
    byte getByte(int pos) throws TypeException;

    /**
     * Gets the value of the column at the given position cast as a signed
     * decimal <code>short</code>
     *
     * @param pos the absolute position of the column
     * @return the value of the column at the given position cast as a signed
     * decimal <code>short</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the return type
     */
    short getShort(int pos) throws TypeException;

    /**
     * Gets the value of the column at the given position cast as a signed
     * decimal <code>int</code>
     *
     * @param pos the absolute position of the column
     * @return the value of the column at the given position cast as a signed
     * decimal <code>int</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the return type
     */
    int getInt(int pos) throws TypeException;

    /**
     * Gets the value of the column at the given position cast as a signed
     * decimal <code>long</code>
     *
     * @param pos the absolute position of the column
     * @return the value of the column at the given position cast as a signed
     * decimal <code>long</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the return type
     */
    long getLong(int pos) throws TypeException;

    /**
     * Gets the value of the column at the given position cast as a
     * <code>double</code> precision float
     *
     * @param pos the absolute position of the column
     * @return the value of the column at the given position cast as a
     * <code>double</code> precision float
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the return type
     */
    double getDouble(int pos) throws TypeException;

    /**
     * Gets the value of the column at the given position cast as an array of
     * bytes.
     *
     * @param pos the absolute position of the column
     * @return the value of the column at the given position cast as an array of
     * bytes.
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the return type
     */
    byte[] getBytes(int pos) throws TypeException;

    /**
     * Sets the value of the column at the given position cast from a
     * <code>String</code> argument.
     *
     * @param pos the absolute position of the column
     * @param val the value of the column at the given position represented as a
     *            <code>String</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the argument type
     */
    void setString(int pos, String val) throws TypeException;

    /**
     * Sets the value of the column at the given position cast from a signed
     * decimal <code>byte</code> argument.
     *
     * @param pos the absolute position of the column
     * @param val the value of the column at the given position represented as a
     *            signed decimal <code>byte</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the argument type
     */
    void setByte(int pos, byte val) throws TypeException;

    /**
     * Sets the value of the column at the given position cast from a signed
     * decimal <code>short</code> argument.
     *
     * @param pos the absolute position of the column
     * @param val the value of the column at the given position represented as a
     *            signed decimal <code>short</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the argument type
     */
    void setShort(int pos, short val) throws TypeException;

    /**
     * Sets the value of the column at the given position cast from a signed
     * decimal <code>int</code> argument.
     *
     * @param pos the absolute position of the column
     * @param val the value of the column at the given position represented as a
     *            signed decimal <code>int</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the argument type
     */
    void setInt(int pos, int val) throws TypeException;

    /**
     * Sets the value of the column at the given position cast from a signed
     * decimal <code>long</code> argument.
     *
     * @param pos the absolute position of the column
     * @param val the value of the column at the given position represented as a
     *            signed decimal <code>long</code>
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the argument type
     */
    void setLong(int pos, long val) throws TypeException;

    /**
     * Sets the value of the column at the given position cast from a
     * <code>double</code> precision float argument.
     *
     * @param pos the absolute position of the column
     * @param val the value of the column at the given position represented as a
     *            <code>double</code> precision float
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the argument type
     */
    void setDouble(int pos, double val) throws TypeException;

    /**
     * Sets the value of the column at the given position cast from an array of
     * bytes. This will always work to set the value of the column if the
     * argument is the value returned by {@link #getBytes}.
     *
     * @param pos the absolute position of the column
     * @param val the value of the column at the given position represented as
     *            an array of bytes
     * @throws TypeException if the type of the column at the given position is
     *                       incompatible with the argument type
     */
    void setBytes(int pos, byte[] val) throws TypeException;
}
