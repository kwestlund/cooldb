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

import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.LogPage;

import java.util.Comparator;

/**
 * ExtentPage delegates to a BSearchArea to store extents.
 */

public class ExtentPage extends LogPage {

    private static final int BASE = LogPage.BASE;
    private final Extent lowerExtentTmp;
    private final Extent upperExtentTmp;
    private final Extent removeExtentTmp;
    private final Extent findExtentTmp;
    private final BinarySearch.Search search;
    private final BSearchArea area;

    public ExtentPage(PageBuffer pageBuffer, Extent prototype) {
        super(pageBuffer);
        area = new BSearchArea(ExtentPage.BASE, pageBuffer.capacity()
                - ExtentPage.BASE, prototype.storeSize());
        area.setPageBuffer(pageBuffer);

        lowerExtentTmp = (Extent) prototype.copy();
        upperExtentTmp = (Extent) prototype.copy();
        removeExtentTmp = (Extent) prototype.copy();
        findExtentTmp = (Extent) prototype.copy();

        search = new BinarySearch.Search(null, prototype.copy());
    }

    /**
     * Return true if the lower extent overlaps with the upper extent.
     */
    public static boolean overlaps(Extent lower, Extent upper) {
        lower.setPageId(lower.getPageId() + lower.getSize());
        try {
            int diff = lower.compareTo(upper);

            return diff > 0 && lower.getFileId() == upper.getFileId();
        } finally {
            lower.setPageId(lower.getPageId() - lower.getSize());
        }
    }

    /**
     * Set the underlying PageBuffer used to store extents.
     */
    @Override
    public void setPageBuffer(PageBuffer pageBuffer) {
        super.setPageBuffer(pageBuffer);
        area.setPageBuffer(pageBuffer);
    }

    /**
     * Place the extent onto the page, coalescing adjacent extents.
     */
    public void insertExtent(Extent extent) {
        boolean joinUpper = false;
        boolean joinLower = false;

        // Find the first extent following the extent being
        // fused into the vector of extents
        search.key = extent;
        BinarySearch.bSearch(search, area);

        // Join to upper extent?
        if (search.index < area.getCount()) {
            area.get(search.index, upperExtentTmp);
            joinUpper = joinable(extent, upperExtentTmp);
        }

        // Join to lower extent?
        if (search.index > 0) {
            area.get(search.index - 1, lowerExtentTmp);
            joinLower = joinable(lowerExtentTmp, extent);
        }

        // Coalesce.
        if (joinUpper && joinLower) {
            lowerExtentTmp.extend(extent.getSize() + upperExtentTmp.getSize());
            area.put(search.index - 1, lowerExtentTmp);
            area.remove(search.index);
        } else if (joinUpper) {
            upperExtentTmp.setPageId(extent.getPageId());
            upperExtentTmp.extend(extent.getSize());
            area.put(search.index, upperExtentTmp);
        } else if (joinLower) {
            lowerExtentTmp.extend(extent.getSize());
            area.put(search.index - 1, lowerExtentTmp);
        } else
            area.insert(search.index, extent);
    }

    /**
     * Remove the extent from the page, splitting a larger extent if necessary.
     * This reverses the action of 'insertExtent'.
     */
    public void removeExtent(Extent extent) {
        int spaceAtEnd;

        // Locate the requested extent
        search.key = extent;
        if (BinarySearch.bSearch(search, area)) {
            area.get(search.index, removeExtentTmp);

            // Split existing extent if larger than requested
            if (removeExtentTmp.getSize() > extent.getSize()) {
                removeExtentTmp.setSize(removeExtentTmp.getSize()
                                                - extent.getSize());
                removeExtentTmp.setPageId(extent.getEndPageId());
                area.put(search.index, removeExtentTmp);
            } else if (removeExtentTmp.getSize() == extent.getSize()) {
                area.remove(search.index);
            } else {
                throw new RuntimeException(
                        "Invalid extent request: extent size not available.");
            }
        }
        // "Carve" requested extent from the body of the prior extent.
        else if (search.index > 0) {
            area.get(search.index - 1, removeExtentTmp);

            // Carve from end of prior extent
            spaceAtEnd = removeExtentTmp.getEndPageId() - extent.getEndPageId();

            if (spaceAtEnd == 0) {
                // Shorten existing extent
                removeExtentTmp.setSize(removeExtentTmp.getSize()
                                                - extent.getSize());
                area.put(search.index - 1, removeExtentTmp);
            } else if (spaceAtEnd > 0) {
                // Shorten existing extent and insert a new extent
                // representing the end of the carved-up extent
                removeExtentTmp.setSize(removeExtentTmp.getSize()
                                                - extent.getSize() - spaceAtEnd);
                area.put(search.index - 1, removeExtentTmp);

                removeExtentTmp.setPageId(extent.getEndPageId());
                removeExtentTmp.setSize(spaceAtEnd);
                area.insert(search.index, removeExtentTmp);
            } else {
                throw new RuntimeException(
                        "Invalid extent request: extent size not available.");
            }
        } else {
            throw new RuntimeException(
                    "Invalid extent request: extent not available.");
        }
    }

    /**
     * Return true if the extent exists, false otherwise.
     */
    public boolean exists(Extent extent) {
        search.key = extent;

        // if an exact match to the extent is found, then return true
        if (BinarySearch.bSearch(search, area))
            return true;

        // otherwise check for its inclusion in the lower extent
        if (search.index > 0) {
            // check the lower entry for overlap with extent
            area.get(search.index - 1, findExtentTmp);
            return overlaps(findExtentTmp, extent);
        }

        return false;
    }

    /**
     * Find the first extent that passes the filter, starting a scan from the
     * location of the extent argument. If extent.isNull is true then the filter
     * will be applied to all extents.
     */
    public boolean findExtent(Extent extent, Comparator<?> filter) {
        int extents = area.getCount();
        if (extent.isNull())
            search.index = 0;
        else {
            search.key = extent;
            BinarySearch.bSearch(search, area);
        }
        for (int i = search.index; i < extents; i++) {
            area.get(i, findExtentTmp);
            if (filter.equals(findExtentTmp)) {
                area.get(i, extent);
                return true;
            }
        }
        return false;
    }

    /**
     * Find and return an extent as large as that specified in 'minSize'. Return
     * true if found, false otherwise.
     */
    public boolean findExtent(int minSize, Extent extent) {
        int extents = area.getCount();
        for (int index = 0; index < extents; index++) {
            area.get(index, findExtentTmp);
            if (findExtentTmp.getSize() >= minSize) {
                area.get(index, extent);
                return true;
            }
        }
        return false;
    }

    @Override
    public int getBase() {
        return ExtentPage.BASE;
    }

    private boolean joinable(Extent lower, Extent upper) {
        lower.setPageId(lower.getPageId() + lower.getSize());
        try {
            int diff = lower.compareTo(upper);
            if (diff == 0)
                return true;

            if (diff > 0 && lower.getFileId() == upper.getFileId())
                throw new RuntimeException(
                        "Invalid extent insertion: extent overlaps existing extent.");

            return false;
        } finally {
            lower.setPageId(lower.getPageId() - lower.getSize());
        }
    }
}
