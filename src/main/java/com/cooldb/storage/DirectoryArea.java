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

import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Printable;

import java.io.PrintStream;

/**
 * DirectoryArea manages the space from a given offset for a given capacity in a
 * particular PageBuffer to store variable-length objects. The objects are
 * stored such that they consume space from the end of the area back toward the
 * start of the area, while the directory of pointers to the stored objects
 * grows from the start of the area toward the end.
 * <p>
 * The PageBuffer must be set before DirectoryArea can be used, and it can be
 * changed at anytime.
 * <p>
 * Objects entered into the DirectoryArea must implement the Storable interface.
 */

public class DirectoryArea implements Printable {

    // Positions of attributes relative to the offset
    static final int DIR_COUNT = 0;
    // Start of object data
    static final int DIR_BASE = DIR_COUNT + 2;
    private int offset;
    private int capacity;
    private PageBuffer pageBuffer;

    /**
     * Set the underlying PageBuffer used to store entries.
     */
    public void setPageBuffer(PageBuffer pageBuffer) {
        this.pageBuffer = pageBuffer;
    }

    public void setBounds(int offset, int capacity) {
        this.offset = offset;
        this.capacity = capacity;
    }

    /**
     * Return the directory value of the entry identified by the specified
     * index.
     */
    public short get(int index) {
        return pageBuffer.getShort(ix(DIR_BASE) + 2 * index);
    }

    /**
     * Set the directory value of the entry identified by the specified index.
     */
    public void set(int index, short value) {
        pageBuffer.putShort(ix(DIR_BASE) + 2 * index, value);
    }

    /**
     * Return the number of entries in the directory.
     */
    public short getCount() {
        return pageBuffer.getShort(ix(DIR_COUNT));
    }

    /**
     * Set the number of entries in the directory.
     */
    public void setCount(int count) {
        pageBuffer.putShort(ix(DIR_COUNT), (short) count);
    }

    /**
     * Return the value at the logical end of the specified entry.
     */
    public short ceil(int index) {
        if (index >= 0)
            return get(index);
        else
            return 0;
    }

    /**
     * Return the value at the logical start of the specified entry.
     */
    public short floor(int index) {
        if (index > 0)
            return get(index - 1);
        else
            return 0;
    }

    /**
     * Return the size of the entry at the specified index.
     */
    public short sizeAt(int index) {
        if (index > 0)
            return (short) (get(index) - get(index - 1));
        else
            return get(index);
    }

    /**
     * Return true if there is at least len bytes available in the directory for
     * the insertion of new entries.
     */
    public boolean canHold(int len) {
        return len <= getUnusedSpace() - 2;
    }

    /**
     * Return the location within the page of the specified entry. This
     * calculates the location from the back of the page so that the directory
     * can be shifted without needing to modify each of the pointers.
     */
    public short loc(int index) {
        return (short) (offset + capacity - ceil(index));
    }

    /**
     * Return the index of the next location on the page that can hold size
     * bytes, increase the directory size by one, and set the offset at the top
     * of the directory.
     */
    public short push(int size) {
        short index = getCount();
        setCount((short) (index + 1));
        set(index, (short) (ceil(index - 1) + size));
        return index;
    }

    /**
     * Make room for size bytes at the specified index.
     */
    public void insertAt(int index, short size) {
        short count = getCount();
        short top = ceil(count - 1);

        // Make room for the new entry at position 'index'
        if (index < count) {
            int shift = top - floor(index);
            int from = loc(count - 1);
            pageBuffer.memmove(from - size, from, shift);
        }

        // Update index value of any relocated entry; enter new index offset
        setCount(++count);
        while (--count >= index)
            set(count, (short) (ceil(count - 1) + size));
    }

    /**
     * Remove the entry at the specified index.
     */
    public void removeAt(int index) {
        short size = sizeAt(index);
        short count = getCount();
        short top = ceil(count - 1);

        // Delete the entry at position 'index'
        if (index < count - 1) {
            int shift = top - ceil(index);
            int from = loc(count - 1);
            pageBuffer.memmove(from + size, from, shift);
        }

        // Update index value of any relocated entry
        while (++index < count)
            set(index - 1, (short) (ceil(index) - size));

        setCount(--count);
    }

    /**
     * Replace the space for the entry at the specified index with enough room
     * for size bytes.
     */
    public void replaceAt(int index, int size) {
        // Determine size differential
        int diff = size - sizeAt(index);
        short count = getCount();
        short top = ceil(count - 1);

        // Determine the amount to shift, which depends on the
        // direction of the shift. If the shift is adding space to the
        // entry at 'index', then it is a left shift and must include
        // all entries from the index to the count-1 inclusive. If on
        // the other hand the shift is subtracting space from the entry
        // at 'index', then it is a right shift and the size of the shift
        // must exclude the subtracted size so as not to overwrite the
        // adjacent entry or the end of the buffer.
        int shift;
        if (diff == 0)
            return;
        else if (diff > 0)
            shift = top - floor(index);
        else
            shift = top - floor(index) + diff;

        int from = loc(count - 1);
        pageBuffer.memmove(from - diff, from, shift);

        // Update index value of any relocated entry
        for (int i = index; i < count; i++)
            set(i, (short) (get(i) + diff));
    }

    /**
     * Remove all data associated with the entries &gt;= fromI and &lt; toI, but keep
     * the directory entries themselves unless the toI == count; Return the
     * number of bytes compacted.
     */
    public int compact(int fromI, int toI) {
        if (fromI >= toI)
            return 0;

        short count = getCount();

        if (toI >= count) {
            int size = rangeSize(fromI, count);
            setCount((short) fromI);
            return size;
        }

        // calculate shift size, leaving enough space for row headers of deleted
        // rows
        int size = rangeSize(fromI, toI) - RowHeader.getOverhead()
                * (toI - fromI);
        int from = loc(count - 1);
        int shift = ceil(count - 1) - floor(toI);
        pageBuffer.memmove(from + size, from, shift);

        // Update index value of all compacted entries, leaving space for row
        // headers
        int base = floor(fromI);
        for (int index = fromI; index < toI; index++)
            set(index, (short) (base + RowHeader.getOverhead()
                    * (index - fromI + 1)));
        for (int index = toI; index < count; index++)
            set(index, (short) (get(index) - size));

        // Set row headers of deleted, compacted rows
        for (int index = fromI; index < toI; index++) {
            int loc = loc(index);
            RowHeader.create(pageBuffer, loc);
            RowHeader.setDeleted(pageBuffer, loc, true);
        }

        return size;
    }

    /**
     * Remove all data associated with the entries &gt;= fromI and &lt; toI. Return
     * the number of bytes purged.
     */
    public int purge(int fromI, int toI) {
        if (fromI >= toI)
            return 0;

        short count = getCount();

        if (toI >= count) {
            int size = rangeSize(fromI, count);
            setCount((short) fromI);
            return size;
        }

        // calculate shift size
        int size = rangeSize(fromI, toI);
        int from = loc(count - 1);
        int shift = ceil(count - 1) - floor(toI);
        pageBuffer.memmove(from + size, from, shift);

        int purgeCount = toI - fromI;

        // Update index value of any relocated entry
        for (int index = fromI + purgeCount; index < count; index++)
            set(index - purgeCount, (short) (ceil(index) - size));

        setCount((short) (count - purgeCount));

        return size;
    }

    /**
     * Return the size of all entries &gt;= fromI and &lt; toI, excluding dir
     * overhead. Since the entries are packed from right to left, the 'fromI'
     * entry actually has a higher address.
     */
    public int rangeSize(int fromI, int toI) {
        return ceil(toI - 1) - floor(fromI);
    }

    /**
     * Copy into this directory the contents of 'dir' with index values &gt;=
     * 'fromI' and &lt; 'toI'
     */
    public void copyRange(DirectoryArea dir, int fromI, int toI) {
        if (toI <= fromI)
            return;

        // Push directory entries onto the end of this directory
        for (int i = fromI; i < toI; i++)
            push(dir.sizeAt(i));

        // Copy data
        int index = loc(getCount() - 1);
        int offset = dir.loc(toI - 1);
        int length = dir.rangeSize(fromI, toI);
        pageBuffer.put(index, dir.pageBuffer, offset, length);
    }

    /**
     * Shift the directory to the right on the page by size bytes.
     */
    public void shift(int size) {
        int count = getCount();
        pageBuffer.memmove(offset + size, offset, (count + 1) * 2);
        offset += size;
        capacity -= size;
    }

    public int getUsedSpace() {
        int count = getCount();
        return (count + 1) * 2 + ceil(count - 1);
    }

    public int getUnusedSpace() {
        return capacity - getUsedSpace();
    }

    public int getCapacity() {
        return capacity;
    }

    private int ix(int i) {
        return offset + i;
    }

    public void print(PrintStream out, String linePrefix) {
        out.print(linePrefix + getClass().getSimpleName() + "(");
        out.print("page offset:" + offset + "/");
        out.print("capacity:" + capacity + "/");
        out.print("DIR_COUNT:" + getCount());
        out.println(")");

        // directory
        int count = getCount();
        out.print(linePrefix + "  dir:");
        for (int i = 0; i < count; i++) {
            out.print(get(i));
            if (i < count - 1)
                out.print("|");
        }
        out.println();
    }
}
