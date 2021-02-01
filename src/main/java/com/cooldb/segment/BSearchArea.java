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

/**
 * BSearchArea manages the space from a given offset for a given capacity in a
 * particular PageBuffer to store fixed-size objects with a binary search
 * retrieval capability.
 * <p>
 * The PageBuffer must be set before BSearchArea can be used, and it can be
 * changed at anytime.
 * <p>
 * Objects entered into the BSearchArea must implement both the DBObject and the
 * Comparable interfaces and be of uniform store size equal to the entry size
 * provided to the BSearchArea upon construction. The BSearchArea itself does
 * not impose an ordering on the objects that are inserted, but the bSearch
 * function does expect them to be in the proper order and can be used to
 * maintain the order of inserted objects.
 * <p>
 * During object insertion, BSearchArea does not check for available space. The
 * getAvailable method must be used to check for space prior to insertion.
 */

public class BSearchArea implements DBObjectArray {

    // Positions of attributes relative to the offset
    static final int ENTRY_COUNT = 0;
    // Start of object data
    static final int ENTRY_BASE = ENTRY_COUNT + 2;
    private final int offset;
    private final int entrySize;
    private final int maxCount;
    private PageBuffer pageBuffer;

    public BSearchArea(int offset, int capacity, int entrySize) {
        this.offset = offset;
        this.entrySize = entrySize;
        this.maxCount = (capacity - ENTRY_BASE) / entrySize;
    }

    /**
     * Set the underlying PageBuffer used to store entries.
     */
    public void setPageBuffer(PageBuffer pageBuffer) {
        this.pageBuffer = pageBuffer;
    }

    /**
     * Return the number of slots available for object insertion. A return value
     * of zero means there is no room left on the page.
     */
    public int getAvailable() {
        return maxCount - getCount();
    }

    /**
     * Gets the number of objects on the page
     */
    public int getCount() {
        return pageBuffer.getShort(ix(ENTRY_COUNT));
    }

    /**
     * Sets the number of objects on the page
     */
    public void setCount(short count) {
        pageBuffer.putShort(ix(ENTRY_COUNT), count);
    }

    /**
     * Gets the object at 'index' in vector
     */
    public DBObject get(int index, DBObject entry) {
        pageBuffer.get(ix(ENTRY_BASE) + index * entrySize, entry);
        return entry;
    }

    /**
     * Sets the object at 'index' in vector and ensures that the object count is
     * set to at least index + 1.
     */
    public void put(int index, DBObject entry) {
        pageBuffer.put(ix(ENTRY_BASE) + index * entrySize, entry);
        setCount((short) Math.max(index + 1, getCount()));
    }

    /**
     * Inserts the object at 'index' in vector and extends the vector if
     * necessary.
     */
    public void insert(int index, DBObject entry) {
        // Move other entrys to make room for new one
        int count = getCount();
        if (index < count) {
            pageBuffer.memmove(ix(ENTRY_BASE) + (index + 1) * entrySize,
                               ix(ENTRY_BASE) + index * entrySize, (count - index)
                                       * entrySize);
        }
        put(index, entry);
        setCount((short) (count + 1));
    }

    /**
     * Remove the entry at 'index' in vector and shorten the vector if
     * necessary.
     */
    public void remove(int index) {
        // Move other entrys over the old one
        int count = getCount() - 1;
        if (index < count) {
            pageBuffer.memmove(ix(ENTRY_BASE) + index * entrySize,
                               ix(ENTRY_BASE) + (index + 1) * entrySize, (count - index)
                                       * entrySize);
        }
        setCount((short) count);
    }

    private int ix(int i) {
        return offset + i;
    }
}
