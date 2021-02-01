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

package com.cooldb.api.impl;

import com.cooldb.api.*;
import com.cooldb.buffer.DBObject;
import com.cooldb.core.Column;
import com.cooldb.core.VarcharColumn;
import com.cooldb.storage.Rowid;

import java.nio.ByteBuffer;

public class RowImpl implements Row {

    protected final Column[] cols;
    Table table;
    int keyCols;

    /**
     * Constructs a row with the specified set of columns.
     *
     * @param cols an array of columns defining the row type
     */
    protected RowImpl(Column[] cols) {
        this.cols = cols;
    }

    /**
     * Constructs a row from the specified types belonging to the specified
     * table.
     *
     * @param table       the table to be associated with the row
     * @param columnTypes the column types of the table
     * @see Column#createColumns(String columnTypes)
     */
    RowImpl(Table table, String columnTypes) {
        this.table = table;
        cols = Column.createColumns(columnTypes);
    }

    /**
     * Constructs a row as a copy of another row.
     *
     * @param row the row to be copied into this one
     */
    protected RowImpl(RowImpl row) {
        table = row.table;
        keyCols = row.keyCols;
        cols = new Column[row.cols.length];
        for (int i = 0; i < cols.length; i++)
            cols[i] = (Column) row.cols[i].copy();
    }

    @Override
    public int getColumnCount() {
        return cols.length;
    }

    @Override
    public boolean isNull(int pos) {
        return cols[pos].isNull();
    }

    @Override
    public void setNull(int pos, boolean val) {
        cols[pos].setNull(val);
    }

    @Override
    public Direction getDirection(int pos) {
        return cols[pos].getDirection();
    }

    @Override
    public void setDirection(int pos, Direction sortDirection) {
        cols[pos].setDirection(sortDirection);
    }

    @Override
    public void setMinValue(int pos) {
        cols[pos].setMinValue();
    }

    @Override
    public void setMaxValue(int pos) {
        cols[pos].setMaxValue();
    }

    @Override
    public Varchar getVarchar(int pos) throws TypeException {
        if (!(cols[pos] instanceof VarcharColumn))
            throw new TypeException("Column is not a Varchar.");
        return (Varchar) cols[pos];
    }

    @Override
    public String getString(int pos) throws TypeException {
        return cols[pos].getString();
    }

    @Override
    public byte getByte(int pos) throws TypeException {
        return cols[pos].getByte();
    }

    @Override
    public short getShort(int pos) throws TypeException {
        return cols[pos].getShort();
    }

    @Override
    public int getInt(int pos) throws TypeException {
        return cols[pos].getInt();
    }

    @Override
    public long getLong(int pos) throws TypeException {
        return cols[pos].getLong();
    }

    @Override
    public double getDouble(int pos) throws TypeException {
        return cols[pos].getDouble();
    }

    @Override
    public byte[] getBytes(int pos) throws TypeException {
        return cols[pos].getBytes();
    }

    @Override
    public void setString(int pos, String val) throws TypeException {
        cols[pos].setString(val);
    }

    @Override
    public void setByte(int pos, byte val) throws TypeException {
        cols[pos].setByte(val);
    }

    @Override
    public void setShort(int pos, short val) throws TypeException {
        cols[pos].setShort(val);
    }

    @Override
    public void setInt(int pos, int val) throws TypeException {
        cols[pos].setInt(val);
    }

    @Override
    public void setLong(int pos, long val) throws TypeException {
        cols[pos].setLong(val);
    }

    @Override
    public void setDouble(int pos, double val) throws TypeException {
        cols[pos].setDouble(val);
    }

    @Override
    public void setBytes(int pos, byte[] val) throws TypeException {
        cols[pos].setBytes(val);
    }

    @Override
    public RowImpl project(byte[] colMap) {
        Column[] newCols = new Column[colMap.length];
        for (int i = 0; i < colMap.length; i++)
            newCols[i] = cols[colMap[i] & 0xff];
        return new RowImpl(newCols);
    }

    @Override
    public Row project(byte[] colMap, int keyCols) {
        RowImpl row = project(colMap);
        row.keyCols = keyCols;
        return row;
    }

    @Override
    public Key createKey(byte[] colMap, Rowid rowid) {
        Column[] newCols = new Column[colMap.length];
        for (int i = 0; i < colMap.length; i++)
            newCols[i] = cols[colMap[i] & 0xff];
        return new KeyImpl(newCols, rowid);
    }

    @Override
    public int writeSortable(byte[] buffer, int offset) {
        for (Column col : cols) offset = col.encode(buffer, offset);
        return offset;
    }

    @Override
    public int readSortable(byte[] buffer, int offset) {
        for (Column col : cols) offset = col.decode(buffer, offset);
        return offset;
    }

    @Override
    public int sortableSize() {
        int size = 0;
        for (Column col : cols) size += col.encodingSize();
        return size;
    }

    @Override
    public int keySize() {
        if (keyCols == 0) {
            return sortableSize();
        }
        int size = 0;
        for (int i = 0; i < keyCols; i++)
            size += cols[i].encodingSize();
        return size;
    }

    @Override
    public int compareKeyTo(Row obj) {
        int l = keyCols > 0 ? keyCols : cols.length;
        for (int i = 0; i < l; i++) {
            int d = cols[i].compareTo(((RowImpl) obj).cols[i]);
            if (d != 0)
                return d;
        }
        return 0;
    }

    public void assignRow(Row row) {
        assign((DBObject) row);
    }

    @Override
    public Row copyRow() {
        return new RowImpl(this);
    }

    // DBObject implementation
    @Override
    public DBObject copy() {
        return new RowImpl(this);
    }

    @Override
    public void assign(DBObject o) {
        RowImpl row = (RowImpl) o;
        for (int i = 0; i < cols.length; i++)
            cols[i].assign(row.cols[i]);
    }

    @Override
    public void writeTo(ByteBuffer bb) {
        bb.put((byte) cols.length);
        for (Column col : cols) col.writeTo(bb);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        int l = bb.get();
        int i = 0;
        while (i < l)
            cols[i++].readFrom(bb);

        // set all columns at the end of the row that are
        // not in the stored record to null
        while (i < cols.length)
            cols[i++].setNull(true);
    }

    @Override
    public int storeSize() {
        int size = 1;
        for (Column col : cols) size += col.storeSize();
        return size;
    }

    @Override
    public int compareTo(Object obj) {
        RowImpl o = (RowImpl) obj;
        int l = Math.min(cols.length, o.cols.length);
        for (int i = 0; i < l; i++) {
            int d = cols[i].compareTo(o.cols[i]);
            if (d != 0)
                return d;
        }
        return cols.length - o.cols.length;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof RowImpl && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        int hash = 0;
        for (Column col : cols) hash += col.hashCode();
        return hash;
    }

    // Debugging
    @Override
    public String toString() {
        StringBuilder sbuf = new StringBuilder();
        for (int i = 0; i < cols.length; i++) {
            sbuf.append(cols[i]);
            if (i < cols.length - 1)
                sbuf.append(",");
        }
        return sbuf.toString();
    }
}
