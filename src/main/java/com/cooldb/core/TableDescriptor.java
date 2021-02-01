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

import com.cooldb.api.Filter;
import com.cooldb.api.TypeException;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class TableDescriptor implements DBObject, Filter {

    private final VarcharColumn name; // User-assigned name of the table
    private final VarcharColumn types; // User-assigned string of column type codes
    private final FilePage segmentId; // Segment identifier
    private int nindexes; // Number of indexes
    private IndexDescriptor[] indexes; // User-assigned index key mappings

    public TableDescriptor() {
        name = new VarcharColumn();
        types = new VarcharColumn();
        segmentId = new FilePage();
        nindexes = 0;
        indexes = new IndexDescriptor[0];
    }

    public TableDescriptor(TableDescriptor tableDescriptor) {
        name = new VarcharColumn(tableDescriptor.name);
        types = new VarcharColumn(tableDescriptor.types);
        segmentId = new FilePage(tableDescriptor.segmentId);
        nindexes = tableDescriptor.nindexes;
        indexes = new IndexDescriptor[nindexes];
        for (int i = 0; i < nindexes; i++)
            indexes[i] = new IndexDescriptor(tableDescriptor.indexes[i]);
    }

    public void assign(DBObject o) {
        TableDescriptor tableDescriptor = (TableDescriptor) o;
        name.assign(tableDescriptor.name);
        types.assign(tableDescriptor.types);
        segmentId.assign(tableDescriptor.segmentId);
        nindexes = 0;
        for (int i = 0; i < tableDescriptor.nindexes; i++)
            addIndex(tableDescriptor.indexes[i]);
    }

    public String getName() {
        return name.getString();
    }

    public void setName(String name) throws TypeException {
        this.name.setString(name);
    }

    public String getTypes() {
        return types.getString();
    }

    public void setTypes(String types) throws TypeException {
        this.types.setString(types);
    }

    public FilePage getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(FilePage segmentId) {
        this.segmentId.assign(segmentId);
    }

    public int getIndexCount() {
        return nindexes;
    }

    public IndexDescriptor getIndex(int i) {
        return indexes[i];
    }

    public IndexDescriptor getIndex(byte[] keyMap) throws TypeException {
        for (int i = 0; i < nindexes; i++) {
            IndexDescriptor id = indexes[i];
            if (Arrays.equals(id.getKeyMap(), keyMap))
                return (IndexDescriptor) id.copy();
        }
        return null;
    }

    public void addIndex(IndexDescriptor indexDescriptor) {
        if (++nindexes > indexes.length) {
            IndexDescriptor[] newIndexes = new IndexDescriptor[nindexes];
            System.arraycopy(indexes, 0, newIndexes, 0, indexes.length);
            for (int i = indexes.length; i < nindexes; i++)
                newIndexes[i] = new IndexDescriptor();
            indexes = newIndexes;
        }
        indexes[nindexes - 1].assign(indexDescriptor);
    }

    public void removeIndex(byte[] keyMap) {
        for (int i = 0; i < nindexes; i++) {
            if (Arrays.equals(indexes[i].getKeyMap(), keyMap)) {
                --nindexes;
                while (i < nindexes) {
                    indexes[i].assign(indexes[i + 1]);
                    ++i;
                }
            }
        }
    }

    // DBObject methods
    public DBObject copy() {
        return new TableDescriptor(this);
    }

    public void writeTo(ByteBuffer bb) {
        name.writeTo(bb);
        types.writeTo(bb);
        segmentId.writeTo(bb);
        bb.put((byte) nindexes);
        for (int i = 0; i < nindexes; i++)
            indexes[i].writeTo(bb);
    }

    public void readFrom(ByteBuffer bb) {
        name.readFrom(bb);
        types.readFrom(bb);
        segmentId.readFrom(bb);
        nindexes = bb.get() & 0xff;
        if (nindexes > indexes.length) {
            IndexDescriptor[] newIndexes = new IndexDescriptor[nindexes];
            System.arraycopy(indexes, 0, newIndexes, 0, indexes.length);
            for (int i = indexes.length; i < nindexes; i++)
                newIndexes[i] = new IndexDescriptor();
            indexes = newIndexes;
        }
        for (int i = 0; i < nindexes; i++)
            indexes[i].readFrom(bb);
    }

    public int storeSize() {
        int isize = 1;
        for (int i = 0; i < nindexes; i++)
            isize += indexes[i].storeSize();
        return name.storeSize() + types.storeSize() + FilePage.sizeOf() + isize;
    }

    public int compareTo(Object obj) {
        TableDescriptor o = (TableDescriptor) obj;
        return name.compareTo(o.name);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof TableDescriptor && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return name.hashCode();
    }

    public boolean passes(Object obj) {
        return equals(obj);
    }
}
