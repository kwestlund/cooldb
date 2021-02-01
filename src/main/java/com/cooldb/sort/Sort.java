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

import com.cooldb.api.DatabaseException;
import com.cooldb.api.RowStream;
import com.cooldb.api.SortDelegate;
import com.cooldb.core.SortManager;
import com.cooldb.transaction.Transaction;

/**
 * Sort sorts RowStreams of arbitrary size using an external multi-way merge
 * sort. Sort supports composite keys, null values, ascending or descending sort
 * order. Sort makes use of order-preserving normalized key encodings to speed
 * in-memory sorts. Sort also provides built-in support for duplicate removal,
 * uniqueness guarantee, and top-n restrictions.
 */
public class Sort {

    private final SortManager sortManager;

    public Sort(SortManager sortManager) {
        this.sortManager = sortManager;
    }

    /**
     * Sorts the given input stream.
     *
     * @param trans the active transaction
     * @param input the stream to be sorted.
     * @return the sorted output stream.
     */
    public RowStream sort(Transaction trans, RowStream input, SortDelegate delegate) throws DatabaseException,
            InterruptedException {
        SortArea sortArea = sortManager.grabSortArea(trans);
        try {
            return sortArea.sort(input, delegate);
        } finally {
            sortArea.release();
        }
    }
}
