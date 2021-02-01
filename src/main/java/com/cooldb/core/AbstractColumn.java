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
import com.cooldb.api.impl.RowImpl;
import com.cooldb.buffer.DBObject;

import java.nio.ByteBuffer;

/**
 * The abstract class <code>Column</code> is the superclass of all database
 * column objects and provides common functions such as null value handling and
 * sort direction.
 * <p>
 * Subclasses of <code>Column</code> must provide methods to read and write
 * their values in the database that incorporate the methods provided here.
 *
 * @see RowImpl
 * @see VarcharColumn
 */

public abstract class AbstractColumn implements Column {
    /**
     * Null types, in sort order: -1: minimum null value 0: not null 1: null 2:
     * maximum null value
     */
    protected byte nullIndicator;
    /**
     * -1 for descending; +1 for ascending
     */
    protected transient byte sortDirection;

    public AbstractColumn() {
        nullIndicator = (byte) 1;
        sortDirection = (byte) 1;
    }

    public AbstractColumn(Column column) {
        assign(column);
    }

    @Override
    public boolean isNull() {
        return nullIndicator != (byte) 0;
    }

    @Override
    public void setNull(boolean nullIndicator) {
        if (nullIndicator)
            this.nullIndicator = (byte) 1;
        else
            this.nullIndicator = (byte) 0;
    }

    @Override
    public Direction getDirection() {
        return sortDirection == -1 ? Direction.DESC : Direction.ASC;
    }

    @Override
    public void setDirection(Direction sortDirection) {
        this.sortDirection = (byte) (sortDirection == Direction.ASC ? 1 : -1);
    }

    @Override
    public void setMinValue() {
        nullIndicator = (byte) -1;
    }

    @Override
    public void setMaxValue() {
        nullIndicator = (byte) 2;
    }

    // DBObject implementation
    public void assign(DBObject o) {
        AbstractColumn column = (AbstractColumn) o;
        nullIndicator = column.nullIndicator;
        sortDirection = column.sortDirection;
    }

    public void writeTo(ByteBuffer bb) {
        bb.put(nullIndicator);
    }

    public void readFrom(ByteBuffer bb) {
        nullIndicator = bb.get();
    }

    public int storeSize() {
        return 1;
    }

    public int compareTo(Object obj) {
        AbstractColumn col = (AbstractColumn) obj;
        int d = nullIndicator - col.nullIndicator;
        return ((d != 0 || nullIndicator != (byte) 0) ? d : _compareTo(col))
                * sortDirection;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof AbstractColumn && compareTo(obj) == 0;
    }

    // Debugging
    @Override
    public String toString() {
        if (nullIndicator != 0)
            return "null";
        StringBuffer sbuf = new StringBuffer();
        _toString(sbuf);
        if (sortDirection == -1)
            sbuf.append(" DESC");
        return sbuf.toString();
    }

    /**
     * Subclasses provide the type-specific compare without concern for the null
     * indicator or the sort direction.
     */
    protected abstract int _compareTo(Object obj);

    /**
     * Subclasses provide the type-specific toString without concern for the
     * null indicator or the sort direction.
     */
    protected abstract void _toString(StringBuffer sbuf);
}
