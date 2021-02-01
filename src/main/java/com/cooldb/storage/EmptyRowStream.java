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
import com.cooldb.api.Row;
import com.cooldb.api.RowStream;

/**
 * Cursor for storage methods.
 */

public class EmptyRowStream implements RowStream {
    private final Row proto;

    public EmptyRowStream(Row proto) {
        this.proto = proto;
    }

    public Row allocateRow() {
        return (Row) proto.copy();
    }

    @Override
    public void open() throws DatabaseException {
    }

    @Override
    public void close() throws DatabaseException {
    }

    @Override
    public boolean fetchNext(Row row) throws DatabaseException {
        return false;
    }

    @Override
    public boolean fetchNext(Row row, Filter filter) throws DatabaseException {
        return false;
    }

    @Override
    public void rewind() throws DatabaseException {
    }
}
