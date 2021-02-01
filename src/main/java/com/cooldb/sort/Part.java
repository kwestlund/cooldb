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

import com.cooldb.api.*;
import com.cooldb.core.SortManager;
import com.cooldb.api.*;
import com.cooldb.api.impl.RowImpl;
import com.cooldb.transaction.Transaction;
import com.cooldb.util.ByteArrayUtils;

class Part {
    /**
     * The variable-length object storage buffer
     */
    private final byte[] buffer;
    /**
     * Index of objects in the buffer
     */
    private final int[] index;
    /**
     * Median-of-Three QuickSort stack
     */
    private final int[] QSright = new int[100];
    private final int[] QSleft = new int[100];
    /**
     * Position of next object to be stored in the input run buffer
     */
    private int position;
    /**
     * The number objects in the buffer
     */
    private int nkeys;
    /**
     * This run represents the end-of-input
     */
    private boolean eof;

    Part(int nbuffers, int bsize) {
        buffer = new byte[nbuffers * bsize];
        index = new int[buffer.length / 4];

        // Sentinel record (compares less than all others and occupies the
        // leftmost position)
        index[0] = 2;
    }

    /**
     * Fetches another batch of objects from the input. Returns true if
     * end-of-input reached.
     */
    boolean fetchRun(RowStream input, Row next) throws DatabaseException {
        int keySize;
        int dataSize;

        // Clear the memory area for this run (keep sentinel record)
        position = 4;
        nkeys = 1;

        do {
            // End the run if no room left in the buffer for the next object
            // plus key and data lengths
            dataSize = next.sortableSize();
            if (buffer.length - position < dataSize + 4) {
                return eof = false;
            }

            // Write object into the buffer and index its position
            keySize = next.keySize();
            ByteArrayUtils.putShort(buffer, (short) dataSize, position);
            position += 2;
            ByteArrayUtils.putShort(buffer, (short) keySize, position);
            index[nkeys] = position; // store position of key
            position += 2;
            next.writeSortable(buffer, position);
            position += dataSize;
            ++nkeys;
        } while (input.fetchNext(next));

        return eof = true;
    }

    /**
     * Median-of-Three QuickSort
     *
     * @throws UniqueConstraintException if there is a unique constraint violation
     */
    @SuppressWarnings("StatementWithEmptyBody")
    void sortRun(SortDelegate delegate) throws UniqueConstraintException {
        int i, j, k, StackPtr;
        int LeftEnd, RightEnd, LeftPtr, RightPtr, MidPtr, MinGroup;
        int Pvalue, temp;

        LeftEnd = 1; // For the first round, the 2
        RightEnd = nkeys - 1; // ends will be the whole array
        MinGroup = 65; // Years ago this would be ~18

        if (nkeys > MinGroup) // Run quicksort until no
            StackPtr = 1; // subgroup remains larger
        else
            StackPtr = 0; // than "MinGroup" elements.

        /*
         * Start quicksort. First, set the pivot value equal to the median of
         * the array values at RandNbrs[LeftEnd+1],
         * RandNbrs[(LeftEnd+RightEnd)/2], and RandNbrs[RightEnd]. The minimum
         * of these 3 is placed at RandNbrs[LeftEnd+1] while the maximum is
         * placed at RandNbrs[RightEnd]. The value at RandNbrs[LeftEnd] is moved
         * to RandNbrs[(LeftEnd+RightEnd)/2].
         */

        while (StackPtr > 0) { // Loop until all subgroups
            // are partitioned down to
            // <= "MinGroup" size.
            LeftPtr = LeftEnd + 1; // Ptr to left end.
            RightPtr = RightEnd; // Ptr to right end.
            MidPtr = (LeftEnd + RightEnd) / 2; // Point to middle

            // Start sort of these 3
            if (compareKeys(LeftPtr, RightPtr) > 0) {
                temp = index[LeftPtr];
                index[LeftPtr] = index[RightPtr];
                index[RightPtr] = temp;
            }

            if (compareKeys(MidPtr, RightPtr) > 0) {
                Pvalue = index[RightPtr];
                index[RightPtr] = index[MidPtr];
            } else if (compareKeys(MidPtr, LeftPtr) < 0) {
                Pvalue = index[LeftPtr];
                index[LeftPtr] = index[MidPtr];
            } else
                Pvalue = index[MidPtr];
            // The 3 values are sorted and
            // and the median is in Pvalue
            index[MidPtr] = index[LeftEnd]; // Fill in hole with LeftEnd

            // Start the main loop. Move pointers inward until
            // we find 2 elements that have to be exchanged.

            while ((compareDirect(index[++LeftPtr], Pvalue)) < 0)
                ; // Set up pointers
            while (compareDirect(index[--RightPtr], Pvalue) > 0)
                ; // for 1st exchange
            while (LeftPtr < RightPtr) { // Make these
                temp = index[LeftPtr]; // statements as
                index[LeftPtr] = index[RightPtr]; // efficient as
                index[RightPtr] = temp; // possible.
                while (compareDirect(index[++LeftPtr], Pvalue) < 0)
                    ; // Continue this loop until
                while (compareDirect(index[--RightPtr], Pvalue) > 0)
                    ; // the pointers cross.
            }

            index[LeftEnd] = index[RightPtr]; // After pointers cross, fill
            index[RightPtr] = Pvalue; // left end and middle hole.

            /*
             * All values to the left of RandNbrs[RightPtr] are <= Pvalue while
             * all to the right are >= Pvalue. Next, test the 2 subgroups on
             * either side to see if they are still larger than the minimum
             * efficient size. If both are still too large, then place the
             * larger one on the stack and partition the smaller. If only one
             * needs partitioning, then partition it, otherwise get the left and
             * right ends of a subgroup stored on the stack in an earlier
             * operation.
             */

            // Move RightPtr into
            RightPtr--; // unsorted left subgroup

            if (RightPtr < MidPtr) { // If left SubGroup is smaller
                if (RightPtr - LeftEnd > MinGroup) { // If both are large
                    // then put
                    QSleft[StackPtr] = LeftPtr; // right side on the stack
                    QSright[StackPtr] = RightEnd; // and sort the left side.
                    RightEnd = RightPtr;
                    ++StackPtr; // Ready for next subgroup
                } else if (RightEnd - LeftPtr > MinGroup) // Else if just have
                    // to
                    LeftEnd = LeftPtr; // sort the right side
                else { // Else neither gets sorted. Get a
                    LeftEnd = QSleft[--StackPtr]; // prior subgroup from the
                    // stack.
                    RightEnd = QSright[StackPtr]; // (Will be garbage if all
                } // subgroups are sorted)
            } // End of "if left is smaller"

            else { // Else left side is larger
                if (RightEnd - LeftPtr > MinGroup) { // If both sides are
                    // large
                    QSleft[StackPtr] = LeftEnd; // then put left side on
                    QSright[StackPtr] = RightPtr; // the stack
                    LeftEnd = LeftPtr; // and sort the right side
                    ++StackPtr; // Ready for next subgroup
                } else if (RightPtr - LeftEnd > MinGroup) // else if left side
                    // is
                    RightEnd = RightPtr; // too large, then sort it.
                else { // Else neither gets sorted. Get a
                    LeftEnd = QSleft[--StackPtr]; // prior subgroup from the
                    // stack
                    RightEnd = QSright[StackPtr]; // (Will be garbage if all
                } // subgroups are sorted).
            } // End of "if left is larger"
        } // Repeat until all subgroups are
        // small.

        // Finish up with "Insertion Sort"
        for (i = 2; i < nkeys; i++) {
            k = i;
            j = i - 1;
            temp = index[k];
            while (compareDirect(index[j], temp) > 0) {
                index[k] = index[j];
                j--;
                k--;
            }
            index[k] = temp;
        }
        if (delegate.isDistinct()) {
            removeDuplicates();
        } else if (delegate.isUnique()) {
            ensureUniqueness();
        }
        if (delegate.topN() > 0) {
            restrictTop(delegate.topN());
        }
    }

    /**
     * Removes all but the topN records.
     */
    void restrictTop(long n) {
        if (nkeys - 1 > n) {
            nkeys = (int) n + 1;
        }
    }

    /**
     * Removes duplicate records.
     */
    void removeDuplicates() {
        int k, shift;
        for (int i = nkeys - 1; i > 1; i--) {
            k = i - 1;
            while (compareKeys(i, k) == 0) {
                --k;
            }
            shift = i - k - 1;
            if (shift > 0) {
                if (i < nkeys - 1) {
                    System.arraycopy(index, i + 1, index, k + 2, nkeys - i - 1);
                }
                nkeys -= shift;
                i -= shift;
            }
        }
    }

    /**
     * Throws an exception if the dataset is not distinct.
     *
     * @throws UniqueConstraintException if duplicates are found during the sort
     */
    void ensureUniqueness() throws UniqueConstraintException {
        for (int i = 1; i < nkeys - 1; i++) {
            if (compareKeys(i, i + 1) == 0) {
                throw new UniqueConstraintException(
                        "Found duplicate keys during sort");
            }
        }
    }

    /**
     * Save run to temporary external storage.
     */
    Run storeRun(SortManager sortManager, Transaction trans)
            throws DatabaseException, InterruptedException {
        Run run = new Run(sortManager);
        run.open(trans, sortManager.getNBuffers());

        // Skip sentinel record with k = 1
        for (int k = 1; k < nkeys; k++) {
            run.insert(trans, buffer, index[k], ByteArrayUtils.getShort(buffer,
                                                                        index[k] - 2) + 2);
        }

        if (eof)
            run.setEOF(true);

        return run;
    }

    /**
     * Creates and returns a RowStream containing the sorted rows in this
     * partition.
     *
     * @param row the row prototype
     * @return a RowStream containing the sorted rows in this partition
     * @throws InterruptedException if the operation is interrupted externally
     * @throws DatabaseException    if the operation fails for some other reason
     */
    RowStream createRowStream(SortManager sortManager, Transaction trans,
                              Row row) throws DatabaseException, InterruptedException {
        // If the number of keys is less than 1000, return an in-memory result
        // set
        if (nkeys < sortManager.getInMemoryThreshold()) {
            // Make a copy of the data without the sentinel record
            return new SortedRowStream(row);
        } else {
            // Otherwise store the results and return the stored set
            return storeRun(sortManager, trans).createRowStream(trans, row);
        }
    }

    private int compareKeys(int l, int r) {
        return SortManager.compare(buffer, index[l] + 2, ByteArrayUtils
                                           .getShort(buffer, index[l]), buffer, index[r] + 2,
                                   ByteArrayUtils.getShort(buffer, index[r]));
    }

    private int compareDirect(int l, int r) {
        return SortManager.compare(buffer, l + 2, ByteArrayUtils.getShort(
                buffer, l), buffer, r + 2, ByteArrayUtils.getShort(buffer, r));
    }

    private class SortedRowStream implements RowStream {
        private final Row row;
        private final byte[] bufferCopy;
        private final int[] indexCopy;
        private int cursor = 1;

        SortedRowStream(Row row) {
            this.row = row;
            bufferCopy = new byte[position];
            System.arraycopy(buffer, 0, bufferCopy, 0, bufferCopy.length);
            indexCopy = new int[nkeys];
            System.arraycopy(index, 0, indexCopy, 0, indexCopy.length);
        }

        @Override
        public Row allocateRow() {
            return (RowImpl) row.copy();
        }

        @Override
        public void close() throws DatabaseException {
            cursor = 1;
        }

        @Override
        public boolean fetchNext(Row row) throws DatabaseException {
            return fetchNext(row, null);
        }

        @Override
        public boolean fetchNext(Row row, Filter filter)
                throws DatabaseException {
            while (cursor < indexCopy.length) {
                row.readSortable(bufferCopy, indexCopy[cursor++] + 2);
                if (filter == null || filter.passes(row))
                    return true;
            }
            return false;
        }

        @Override
        public void open() throws DatabaseException {
            cursor = 1;
        }

        @Override
        public void rewind() throws DatabaseException {
            cursor = 1;
        }
    }
}
