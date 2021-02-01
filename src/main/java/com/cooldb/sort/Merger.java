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
import com.cooldb.transaction.Transaction;
import com.cooldb.api.*;

class Merger {
    /**
     * Sorted input runs
     */
    private final Run[] runs;
    private int nruns;
    /**
     * Merger of output runs
     */
    private Merger merger;
    /**
     * Number of sort buffers to use
     */
    private final int nway;
    /**
     * The central sort manager
     */
    private final SortManager sortManager;

    Merger(SortManager sortManager) {
        this.sortManager = sortManager;
        nway = sortManager.getNBuffers() - 1;
        runs = new Run[nway];
        nruns = 0;
    }

    void clear(Transaction trans) throws DatabaseException {
        if (merger != null) {
            merger.clear(trans);
            merger = null;
        }
        for (int i = 0; i < nway; i++) {
            if (runs[i] != null) {
                runs[i].close(trans);
                runs[i] = null;
            }
        }
        nruns = 0;
    }

    boolean mergeRun(Transaction trans, Run run, SortDelegate delegate)
            throws DatabaseException, InterruptedException {
        addRun(run);

        if (run.isEOF() || nruns == nway) {
            // Merge sorted runs into one output run
            return merge(trans, run.isEOF(), delegate);
        }

        return false;
    }

    RowStream output(Transaction trans, Row row) {
        if (merger != null) {
            return merger.output(trans, row);
        }
        RowStream rs = runs[0].createRowStream(trans, row);
        runs[0] = null;
        nruns = 0;
        return rs;
    }

    private void swapRuns(int l, int r) {
        Run t = runs[l];
        runs[l] = runs[r];
        runs[r] = t;
    }

    private void sortRuns(int l, int r) {
        int i, last;

        if (l >= r)
            return;
        swapRuns(l, (l + r) / 2);
        last = l;
        for (i = l + 1; i <= r; i++)
            if (runs[i].compareTo(runs[l]) < 0)
                swapRuns(++last, i);
        swapRuns(l, last);
        sortRuns(l, last - 1);
        sortRuns(last + 1, r);
    }

    /**
     * Find run in runs[from]...runs[to - 1]
     */
    private int bSearch(Run run, int from, int to) {
        int cond = 0;
        int mid = 0;
        int low = from;

        // Look for an element that matches the run
        while (low < to) {
            mid = (low + to) / 2;
            if ((cond = run.compareTo(runs[mid])) < 0)
                to = mid;
            else if (cond > 0)
                low = mid + 1;
            else
                return mid;
        }

        // If a match is not found,
        // return index to the element last compared or to the next position,
        // depending on whether the key is less than or greater than that
        // element,
        // such that the index is always the position immediately following
        // where the key would have been found if it were there.

        return cond < 0 ? mid : cond > 0 ? mid + 1 : from;
    }

    /**
     * Place the first run in order among the others
     */
    private void reorder() {
        // Save the first run
        Run run = runs[0];

        // Determine, using a binary search, where it should be placed
        // in the vector in order for the vector to remain sorted.
        // We can start the search with element runs[2] because we
        // already have tested and found that the removed run is greater
        // than the run now at runs[1]. This saves 1 comparison.
        int index = bSearch(run, 2, nruns) - 1;

        // Insert run back into runs
        // Shift to make room for the run in its ordered place
        System.arraycopy(runs, 1, runs, 0, index);
        runs[index] = run;
    }

    // Merge input runs to one output run.
    private void mergeTo(Transaction trans, Run out, SortDelegate delegate)
            throws DatabaseException, InterruptedException {
        // Assume the runs are sorted in ascending order to begin with
        boolean eof;
        int diff;
        Run run;
        do {
            run = runs[0];

            // Get next tuple from first stream. If the stream
            // is not depleted and the new tuple is greater than
            // the next tuple from the second stream, then reorder
            // the streams.
            eof = false;
            while (!eof && (diff = run.compareTo(runs[1])) <= 0) {
                if (diff < 0 || !delegate.isDistinct()) {// skip duplicates
                    if (diff == 0 && delegate.isUnique()) {
                        throw new UniqueConstraintException(
                                "Found duplicate keys during sort merge");
                    }
                    if (delegate.topN() == 0 || out.getRows() < delegate.topN()) {
                        out.insert(trans, run.getBuffer(), 0, run.getLength());
                    } else {
                        // skip all records after the topN records
                        eof = true;
                        break;
                    }
                }
                eof = !run.fetchNext(trans);
            }

            if (eof) {
                // Remove the run from the input array.
                run.close(trans);
                removeFirstRun();
            } else {
                reorder();
            }
        } while (nruns > 1);

        // Output the rest of the remaining input stream
        run = runs[0];
        do {
            if (delegate.topN() == 0 || out.getRows() < delegate.topN()) {
                out.insert(trans, run.getBuffer(), 0, run.getLength());
            } else {
                // skip all records after the topN records
                break;
            }
        } while (run.fetchNext(trans));

        // Remove remaining input stream
        run.close(trans);
        runs[0] = null;
        nruns = 0;
    }

    private void addRun(Run run) {
        runs[nruns++] = run;
    }

    // Merge upto nway input runs into one output run.
    private boolean merge(Transaction trans, boolean isFinal,
                          SortDelegate delegate) throws DatabaseException,
            InterruptedException {
        // If there is only one run, then merging may be complete
        if (nruns == 1) {
            // If this is not the lowest level output merger, then
            // we must merge the one run at this level with the runs
            // at the next level out.
            if (merger != null) {
                merger.addRun(runs[nruns = 0]);
                merger.merge(trans, isFinal, delegate);
            }

            // Merging is complete
            return isFinal;
        }

        // If this is not the final run, then wait until we have nway runs
        // before merging them
        if (!isFinal && nruns < nway)
            return false;

        // Read the first tuple from each input run and estimate the
        // size of output from the sum of inputs
        int storeSize = 0;
        Run run;
        for (int i = 0; i < nruns; i++) {
            run = runs[i];
            storeSize += run.getStoreSize();
            run.fetchNext(trans);
        }

        // Maybe some or all runs are empty (due to duplicate removal)
        if (storeSize == 0) return isFinal;

        // Sort input runs
        sortRuns(0, nruns - 1);
        removeEmptyRunsFromEnd();

        // Create output run from structure of input runs
        Run out = new Run(sortManager);

        // Merge input runs
        out.open(trans, storeSize / sortManager.getBufferSize() + 1);
        mergeTo(trans, out, delegate);

        // Add to output merger
        if (merger == null)
            merger = new Merger(sortManager);
        merger.addRun(out);

        // Merge output runs
        return merger.merge(trans, isFinal, delegate);
    }

    private void removeFirstRun() {
        --nruns;
        if (nruns >= 0) System.arraycopy(runs, 1, runs, 0, nruns);
        runs[nruns] = null;
    }

    private void removeEmptyRunsFromEnd() {
        while (nruns > 0 && runs[nruns - 1].getStoreSize() == 0) {
            runs[--nruns] = null;
        }
    }
}
