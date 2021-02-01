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

package com.cooldb.core;

import com.cooldb.api.Direction;
import com.cooldb.api.Row;
import com.cooldb.api.TypeException;
import com.cooldb.api.Varchar;
import com.cooldb.buffer.DBObject;

/**
 * <code>Column</code> represents a generic column in the database.
 *
 * @see Row
 * @see Varchar
 */

public interface Column extends DBObject, Encodable {
    /**
     * Constructs an array of columns from the specified list of column types.
     * <p>
     * The list of column types is specified as a <code>String</code> of codes
     * from the following list of codes:
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
     * @param columnTypes the string of column types
     */
    static Column[] createColumns(String columnTypes) {
        Column[] cols = new Column[columnTypes.length()];
        for (int i = 0; i < cols.length; i++) {
            char c = columnTypes.charAt(i);
            switch (c) {
                case 's':
                case 'a':
                    cols[i] = new VarcharColumn(255);
                    break;
                case 'S':
                case 'A':
                    cols[i] = new VarcharColumn(Integer.MAX_VALUE);
                    break;
                case 'b':
                case 'i':
                case 'I':
                case 'l':
                case 'd':
                    cols[i] = new NumberColumn(c);
                    break;
                default:
                    break;
            }
        }
        return cols;
    }

    /**
     * Gets the null value indicator for this column.
     *
     * @return true if the column is null, false otherwise
     */
    boolean isNull();

    /**
     * Sets the value for this column to be null if <code>nullIndicator</code>
     * is true or to be not-null if <code>nullIndicator</code> is false.
     *
     * @param nullIndicator indicates that the column should be set to null if true,
     *                      not-null if false
     */
    void setNull(boolean nullIndicator);

    /**
     * Gets the sort direction for this column (default = ASC)
     *
     * @return the sort direction
     */
    Direction getDirection();

    /**
     * Sets the sort direction for this column.
     *
     * @param sortDirection the sort direction
     */
    void setDirection(Direction sortDirection);

    /**
     * Sets the value for this column to that which is less than all possible
     * values for the column domain.
     */
    void setMinValue();

    /**
     * Sets the value for this column to that which is greater than all possible
     * values for the column domain.
     */
    void setMaxValue();

    /**
     * Gets the value of this column cast as a <code>String</code>
     *
     * @return the value of the column cast as a <code>String</code>
     * @throws TypeException if the type of this column is incompatible with the return
     *                       type
     */
    String getString() throws TypeException;

    /**
     * Sets the value of this column cast from a <code>String</code> argument.
     *
     * @param val the value of the column represented as a <code>String</code>
     * @throws TypeException if the type of this column is incompatible with the argument
     *                       type
     */
    void setString(String val) throws TypeException;

    /**
     * Gets the value of this column cast as a signed decimal <code>byte</code>
     *
     * @return the value of the column cast as a signed decimal
     * <code>byte</code>
     * @throws TypeException if the type of this column is incompatible with the return
     *                       type
     */
    byte getByte() throws TypeException;

    /**
     * Sets the value of this column cast from a signed decimal
     * <code>byte</code> argument.
     *
     * @param val the value of the column represented as a signed decimal
     *            <code>byte</code>
     * @throws TypeException if the type of this column is incompatible with the argument
     *                       type
     */
    void setByte(byte val) throws TypeException;

    /**
     * Gets the value of this column cast as a signed decimal <code>short</code>
     *
     * @return the value of the column cast as a signed decimal
     * <code>short</code>
     * @throws TypeException if the type of this column is incompatible with the return
     *                       type
     */
    short getShort() throws TypeException;

    /**
     * Sets the value of this column cast from a signed decimal
     * <code>short</code> argument.
     *
     * @param val the value of the column represented as a signed decimal
     *            <code>short</code>
     * @throws TypeException if the type of this column is incompatible with the argument
     *                       type
     */
    void setShort(short val) throws TypeException;

    /**
     * Gets the value of this column cast as a signed decimal <code>int</code>
     *
     * @return the value of the column cast as a signed decimal <code>int</code>
     * @throws TypeException if the type of this column is incompatible with the return
     *                       type
     */
    int getInt() throws TypeException;

    /**
     * Sets the value of this column cast from a signed decimal <code>int</code>
     * argument.
     *
     * @param val the value of the column represented as a signed decimal
     *            <code>int</code>
     * @throws TypeException if the type of this column is incompatible with the argument
     *                       type
     */
    void setInt(int val) throws TypeException;

    /**
     * Gets the value of this column cast as a signed decimal <code>long</code>
     *
     * @return the value of the column cast as a signed decimal
     * <code>long</code>
     * @throws TypeException if the type of this column is incompatible with the return
     *                       type
     */
    long getLong() throws TypeException;

    /**
     * Sets the value of this column cast from a signed decimal
     * <code>long</code> argument.
     *
     * @param val the value of the column represented as a signed decimal
     *            <code>long</code>
     * @throws TypeException if the type of this column is incompatible with the argument
     *                       type
     */
    void setLong(long val) throws TypeException;

    /**
     * Gets the value of this column cast as a <code>double</code> precision
     * float
     *
     * @return the value of the column cast as a <code>double</code> precision
     * float
     * @throws TypeException if the type of this column is incompatible with the return
     *                       type
     */
    double getDouble() throws TypeException;

    /**
     * Sets the value of this column cast from a <code>double</code> precision
     * float argument.
     *
     * @param val the value of the column represented as a <code>double</code>
     *            precision float
     * @throws TypeException if the type of this column is incompatible with the argument
     *                       type
     */
    void setDouble(double val) throws TypeException;

    /**
     * Gets the value of this column cast as an array of bytes.
     *
     * @return the value of the column cast as an array of bytes.
     * @throws TypeException if the type of this column is incompatible with the return
     *                       type
     */
    byte[] getBytes() throws TypeException;

    /**
     * Sets the value of this column cast from an array of bytes. This will
     * always work to set the value of the column if the argument is the value
     * returned by {@link #getBytes}.
     *
     * @param val the value of the column represented as an array of bytes.
     * @throws TypeException if the type of this column is incompatible with the argument
     *                       type
     */
    void setBytes(byte[] val) throws TypeException;
}
