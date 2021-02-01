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

import com.cooldb.buffer.FilePage;
import com.cooldb.api.Filter;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.segment.PageBroker;
import com.cooldb.transaction.Transaction;

public class TempPage extends RowPage {

    private final PageBroker pageBroker;

    public TempPage(PageBroker pageBroker) {
        this.pageBroker = pageBroker;
    }

    public void create(FilePage nextPage, FilePage prevPage) {
        setNextPage(nextPage);
        setPrevPage(prevPage);
        pageBuffer.position(BASE);
    }

    public void tempPin(FilePage page, Transaction trans)
            throws StorageException {
        try {
            setPageBuffer(pageBroker.tempPin(page, trans));
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void readPin(FilePage page) throws StorageException {
        try {
            setPageBuffer(pageBroker.readPin(page));
            pageBuffer.position(BASE);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    public void unPin(BufferPool.Affinity affinity) throws StorageException {
        try {
            pageBroker.unPin(affinity);
        } catch (Exception e) {
            throw new StorageException(e);
        }
    }

    /**
     * Insert the tuple into the page
     */
    public void insert(DBObject obj) {
        pageBuffer.put((byte) 1);
        pageBuffer.put(obj);
        pageBuffer.put((byte) 0);
        pageBuffer.position(pageBuffer.position() - 1);
    }

    /**
     * Read the next value of the tuple from this page that passes the filter.
     */
    public boolean fetchNext(DBObject obj, Filter filter)
            throws StorageException {
        while (pageBuffer.get() > 0) {
            // Read the tuple and size
            pageBuffer.get(obj);
            if (filter == null || filter.passes(obj))
                return true;
        }
        return false;
    }

    public boolean canHold(int size) {
        return size < pageBuffer.remaining() - 1;
    }
}
