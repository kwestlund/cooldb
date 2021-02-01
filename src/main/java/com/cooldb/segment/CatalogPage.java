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

package com.cooldb.segment;

import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.LogPage;

/**
 * CatalogPage delegates to a BSearchArea to store DBObject objects of fixed
 * length.
 */

public class CatalogPage extends LogPage implements DBObjectArray {

    private static final int BASE = LogPage.BASE;

    // DBObjectArray implementation
    private final BinarySearch.Search search;
    private final BSearchArea area;

    public CatalogPage(PageBuffer pageBuffer, DBObject prototype) {
        super(pageBuffer);
        area = new BSearchArea(CatalogPage.BASE, pageBuffer.capacity()
                - CatalogPage.BASE, prototype.storeSize());
        area.setPageBuffer(pageBuffer);
        search = new BinarySearch.Search(null, prototype.copy());
    }

    public DBObject get(int index, DBObject entry) {
        return area.get(index, entry);
    }

    public int getCount() {
        return area.getCount();
    }

    /**
     * Set the underlying PageBuffer used to store the objects.
     */
    @Override
    public void setPageBuffer(PageBuffer pageBuffer) {
        super.setPageBuffer(pageBuffer);
        area.setPageBuffer(pageBuffer);
    }

    /**
     * Insert the obj using its natural order (ie, Comparable)
     */
    public void insert(DBObject obj) {
        search.key = obj;
        if (BinarySearch.bSearch(search, area))
            area.put(search.index, obj);
        else
            area.insert(search.index, obj);
    }

    /**
     * Remove the obj.
     */
    public boolean remove(DBObject obj) {
        search.key = obj;
        if (BinarySearch.bSearch(search, area)) {
            area.remove(search.index);
            return true;
        }
        return false;
    }

    /**
     * Read the obj using its natural order.
     */
    public boolean select(DBObject obj) {
        search.key = obj;
        if (BinarySearch.bSearch(search, area)) {
            area.get(search.index, obj);
            return true;
        }
        return false;
    }

    @Override
    public int getBase() {
        return CatalogPage.BASE;
    }
}
