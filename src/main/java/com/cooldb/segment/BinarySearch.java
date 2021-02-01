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

import java.util.Comparator;

/**
 * BinarySearch implements the binary search algorithm over a
 * {@link DBObjectArray}.
 */

public class BinarySearch {
    public final static DefaultComparator defaultComparator = new DefaultComparator();

    /**
     * Find an entry using the binary search algorithm.
     * <p>
     * Returns true if an entry matching the key is found and false otherwise.
     * bSearch also sets the index of the search parameter to the position of
     * the entry found or, if one is not found, to the position of the entry
     * last compared or to the position immediately following the position of
     * the entry last compared, depending on whether the key is less than or
     * greater than the entry last compared, such that a subsequent insert of
     * the object into the vector at that position will maintain the ordering of
     * the objects in the vector.
     * <p>
     * In other words, if the bSearch returns true, the index returned in the
     * search parameter is the position of the key value in the array, and if
     * the bSearch returns false, the index returned can be used to <i>insert</i>
     * a value matching the key <i>into</i> the array at the index position
     * such that key order is maintained.
     * <p>
     * Boundary conditions:
     * <p>
     * If the key &lt; first element, index = 0. <br>
     * If the key &gt; last element, index = count.
     */
    public static boolean bSearch(Search search, DBObjectArray array) {
        return bSearch(search, array, defaultComparator);
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    public static boolean bSearch(Search search, DBObjectArray array,
                                  Comparator comparator) {

        int cond = 0;
        int low = 0;
        int high = array.getCount();
        int mid = (low + high) / 2;

        // Look for an element that matches the search key
        while (low < high) {
            mid = (low + high) / 2;
            if ((cond = comparator.compare(search.key, array.get(mid, search.value))) < 0)
                high = mid;
            else if (cond > 0)
                low = mid + 1;
            else {
                search.index = mid;
                return true;
            }
        }

        if (cond < 0)
            search.index = mid;
        else if (cond > 0)
            search.index = mid + 1;
        else
            search.index = 0; // First element

        return false;
    }

    /**
     * Search provides the key and value used by the bSearch function to find a
     * specific entry index using the key. The value is used to read entries
     * from the DBObjectArray during comparisons with the key. The index will
     * contain the result of the bSearch.
     */
    public static class Search {
        public DBObject key;
        public final DBObject value;
        public int index;

        public Search(DBObject key, DBObject value) {
            this.key = key;
            this.value = value;
        }
    }

    @SuppressWarnings({"unchecked", "rawtypes"})
    public static class DefaultComparator implements Comparator {
        public int compare(Object o1, Object o2) {
            return ((Comparable) o1).compareTo(o2);
        }
    }
}
