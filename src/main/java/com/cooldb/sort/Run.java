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

package com.cooldb.sort;

import com.cooldb.storage.DatasetCursor;
import com.cooldb.storage.StorageMethod;
import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.api.Row;
import com.cooldb.api.RowStream;
import com.cooldb.api.impl.RowImpl;
import com.cooldb.buffer.DBObject;
import com.cooldb.core.SortManager;
import com.cooldb.transaction.Transaction;
import com.cooldb.util.ByteArrayUtils;

import java.nio.ByteBuffer;

class Run implements Comparable<Run> {
    /**
     * The sort manager
     */
    private final SortManager sortManager;
    /**
     * The number of pages allocated for this run
     */
    private int pages;
    /**
     * The number of rows inserted in this run
     */
    private int rows;
    /**
     * The actual number of bytes occupied by all rows in this run
     */
    private int storeSize;
    /**
     * Temporary external storage
     */
    private StorageMethod temp;
    /**
     * Cursor for fetching
     */
    private DatasetCursor cursor;
    /**
     * Row holder for inserting and fetching
     */
    private final SortRec rec;
    /**
     * This run represents the end-of-input
     */
    private boolean eof;

    Run(SortManager sortManager) {
        this.sortManager = sortManager;
        rec = new SortRec();
    }

    RowStream createRowStream(Transaction trans, Row row) {
        return new RunIterator(trans, row);
    }

    void open(Transaction trans, int pages) throws DatabaseException {
        this.pages = pages;
        temp = sortManager.createTemp(trans, pages);
    }

    void close(Transaction trans) throws DatabaseException {
        if (cursor != null) {
            temp.closeCursor(cursor);
        }
        sortManager.dropTemp(trans, temp);
    }

    int getPages() {
        return pages;
    }

    int getRows() {
        return rows;
    }

    int getStoreSize() {
        return storeSize;
    }

    void insert(Transaction trans, byte[] buffer, int offset, int length)
            throws DatabaseException, InterruptedException {
        try {
            rec.length = length;
            rec.offset = offset;
            rec.buffer = buffer;

            temp.insert(trans, rec);

            ++rows;
            storeSize += rec.storeSize();
        } finally {
            rec.length = rec.offset = 0;
            rec.buffer = null;
        }
    }

    boolean fetchNext(Transaction trans) throws DatabaseException {
        return fetchNext(trans, null);
    }

    boolean fetchNext(Transaction trans, Filter filter)
            throws DatabaseException {
        if (cursor == null) {
            cursor = new DatasetCursor();
            temp.openCursor(trans, cursor);
        }
        return temp.fetchNext(trans, cursor, rec, filter);
    }

    @Override
    public int compareTo(Run obj) {
        return rec.compareTo(obj.rec);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof Run && compareTo((Run) obj) == 0;
    }

    @Override
    public int hashCode() {
        return rec.hashCode();
    }

    public byte[] getBuffer() {
        return rec.buffer;
    }

    public int getLength() {
        return rec.length;
    }

    public boolean isEOF() {
        return eof;
    }

    public void setEOF(boolean eof) {
        this.eof = eof;
    }

    // DBObject implementation
    static class SortRec implements DBObject {
        /**
         * Buffer containing the next object in the run
         */
        byte[] buffer;
        /**
         * The length of the object in the buffer
         */
        int length;
        /**
         * The starting offset of the object in the buffer
         */
        int offset;

        public void writeTo(ByteBuffer bb) {
            bb.putShort((short) length);
            bb.put(buffer, offset, length);
        }

        public void readFrom(ByteBuffer bb) {
            length = bb.getShort();
            if (buffer == null || length > buffer.length)
                buffer = new byte[length];
            bb.get(buffer, offset = 0, length);
        }

        public int storeSize() {
            return 2 + length;
        }

        public DBObject copy() {
            SortRec sr = new SortRec();
            sr.assign(this);
            return sr;
        }

        public void assign(DBObject o) {
            SortRec sr = (SortRec) o;
            length = sr.length;
            offset = 0;
            if (sr.buffer == null)
                buffer = null;
            else {
                if (buffer == null || length > buffer.length)
                    buffer = new byte[length];
                System.arraycopy(sr.buffer, sr.offset, buffer, 0, length);
            }
        }

        public int compareTo(Object obj) {
            SortRec r = (SortRec) obj;
            // treat nulls as greater than all other values
            if (buffer == null || r.buffer == null) return buffer == r.buffer ? 0 : r.buffer == null ? -1 : 1;
            return SortManager.compare(buffer, offset + 2, ByteArrayUtils
                                               .getShort(buffer, offset), r.buffer, r.offset + 2,
                                       ByteArrayUtils.getShort(r.buffer, r.offset));
        }

        @Override
        public boolean equals(Object obj) {
            return obj instanceof SortRec && compareTo(obj) == 0;
        }

        @Override
        public int hashCode() {
            int hc = 0;
            for (int i = 0; i < length; i++) {
                hc += buffer[i + offset + 2];
            }
            return hc;
        }
    }

    class RunIterator implements RowStream {
        private Transaction trans;
        private final Row row;

        RunIterator(Transaction trans, Row row) {
            this.trans = trans;
            this.row = row;
        }

        @Override
        public Row allocateRow() {
            return (RowImpl) row.copy();
        }

        @Override
        public void open() throws DatabaseException {
            if (trans == null)
                throw new DatabaseException(
                        "Cannot reopen closed temporary sort run");
        }

        @Override
        public void close() throws DatabaseException {
            if (trans != null) {
                try {
                    Run.this.close(trans);
                } catch (DatabaseException e) {
                    throw new DatabaseException(e);
                } finally {
                    trans = null;
                }
            }
        }

        @Override
        public boolean fetchNext(Row row) throws DatabaseException {
            return fetchNext(row, null);
        }

        @Override
        public boolean fetchNext(Row row, Filter filter)
                throws DatabaseException {
            boolean found;
            try {
                // decode the sort record into the row
                if (found = Run.this.fetchNext(trans, filter))
                    row.readSortable(rec.buffer, 2);
            } catch (DatabaseException e) {
                throw new DatabaseException(e);
            }
            return found;
        }

        @Override
        public void rewind() throws DatabaseException {
            try {
                if (cursor != null)
                    temp.rewind(cursor);
            } catch (DatabaseException e) {
                throw new DatabaseException(e);
            }
        }
    }
}
