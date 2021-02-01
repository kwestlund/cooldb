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

import com.cooldb.api.Index;
import com.cooldb.api.Key;
import com.cooldb.api.RID;
import com.cooldb.buffer.DBObject;
import com.cooldb.core.Column;
import com.cooldb.storage.Rowid;

import java.nio.ByteBuffer;

public class KeyImpl extends RowImpl implements Key {

    Index index;
    private Rowid rowid;

    /**
     * Constructs a key that is made unique by appending the rowid if one is
     * supplied.
     *
     * @param cols  the list of columns comprising the key
     * @param rowid the optional rowid used to ensure uniqueness of the key
     */
    protected KeyImpl(Column[] cols, Rowid rowid) {
        super(cols);
        this.rowid = rowid;
    }

    protected KeyImpl(KeyImpl key) {
        super(new Column[key.cols.length]);
        this.index = key.index;
        for (int i = 0; i < cols.length; i++)
            cols[i] = (Column) key.cols[i].copy();
        if (key.rowid != null)
            rowid = new Rowid(key.rowid);
    }

    @Override
    public void setMinValue() {
        for (Column col : cols) col.setMinValue();
    }

    @Override
    public void setMaxValue() {
        for (Column col : cols) col.setMaxValue();
    }

    @Override
    public RID getRID() {
        return rowid;
    }

    @Override
    public void setRID(RID rid) {
        this.rowid = (Rowid) rid;
    }

    // DBObject implementation
    @Override
    public DBObject copy() {
        return new KeyImpl(this);
    }

    @Override
    public void assign(DBObject obj) {
        super.assign(obj);
        if (rowid != null)
            rowid.assign(((KeyImpl) obj).rowid);
    }

    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        if (rowid != null)
            rowid.writeTo(bb);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        if (rowid != null)
            rowid.readFrom(bb);
    }

    @Override
    public int storeSize() {
        if (rowid == null)
            return super.storeSize();
        return super.storeSize() + rowid.storeSize();
    }

    @Override
    public int compareTo(Object obj) {
        int d = super.compareTo(obj);
        if (d != 0)
            return d;
        if (rowid != null)
            return rowid.compareTo(((KeyImpl) obj).rowid);
        return 0;
    }

    @Override
    public int hashCode() {
        if (rowid != null)
            return rowid.hashCode();
        return super.hashCode();
    }
}
