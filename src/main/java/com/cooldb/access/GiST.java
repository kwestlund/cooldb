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

package com.cooldb.access;

import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;
import com.cooldb.transaction.Transaction;

/**
 * <code>GiST</code> specifies interfaces for the Generalized Index Search
 * Tree (GiST).
 */

public interface GiST {

    DBObject findFirst(Transaction trans, Root root, Predicate query,
                       Cursor cursor, boolean reverse, Filter filter) throws DatabaseException;

    DBObject findNext(Transaction trans, Predicate query, Cursor cursor,
                      boolean reverse, Filter filter) throws DatabaseException;

    void insert(Transaction trans, Root root, Entry entry, int level)
            throws DatabaseException;

    void remove(Transaction trans, Root root, Predicate query, int level)
            throws DatabaseException;

    Cursor allocateCursor() throws DatabaseException;

    void openCursor(Transaction trans, Cursor cursor);

    interface Node extends GiST {

        // Type specific methods
        boolean consistent(Entry entry, Predicate query);

        boolean passes(Entry entry, Filter filter);

        Entry pickSplit(Node newNode, int level) throws DatabaseException;

        // Ordered only
        int compare(Entry e1, Entry e2);
    }

    interface Entry extends DBObject {
        Predicate getPredicate();

        void setPredicate(Predicate p);

        DBObject getPointer();

        void setPointer(DBObject ptr);
    }

    interface Cursor {
        FilePage getLeaf();

        void setLeaf(FilePage leaf);

        Entry getEntry();

        void setEntry(Entry entry);

        int getIndex();

        void setIndex(int index);

        long getPageLSN();

        void setPageLSN(long lsn);

        long getCusp();

        void setCusp(long cusp);
    }

    interface ResultSet {
        void add(DBObject rowid);
    }

    interface Predicate extends DBObject {
    }
}
