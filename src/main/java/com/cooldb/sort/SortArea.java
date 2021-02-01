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
import com.cooldb.api.Row;
import com.cooldb.api.RowStream;
import com.cooldb.api.SortDelegate;
import com.cooldb.core.SortManager;
import com.cooldb.storage.EmptyRowStream;
import com.cooldb.transaction.Transaction;

import java.util.concurrent.ArrayBlockingQueue;

/**
 * SortArea implements an external merge-sort algorithm.
 * <p>
 * SortArea provides a fixed amount of memory allocated and dedicated to sorting that is
 * separate from the buffer pool used by other database operations.
 * <p>
 * Any input larger than the allocated buffer size is sorted in chunks that are each
 * stored to the file system and merged together to form the result set.
 */
public class SortArea {
    /**
     * The central sort manager
     */
    private final SortManager sortManager;
    /**
     * Lock to protect access to trans
     */
    private final Object transLock = new Object();
    private final Object doneLock = new Object();
    /**
     * The completion flag
     */
    private boolean done;
    /**
     * Transaction currently holding this SortArea
     */
    private Transaction trans;
    /**
     * The sort delegate to specify sorting behavior
     */
    private SortDelegate delegate;
    /**
     * Sort Partitions
     */
    private Part part1, part2, part3;
    /**
     * Pipeline queues
     */
    private ArrayBlockingQueue<Part> fetchQ, sortQ, storeQ;
    private ArrayBlockingQueue<Run> mergeQ;
    /**
     * Pipeline threads
     */
    private SortThread sortT;
    private StoreThread storeT;
    private MergeThread mergeT;
    /**
     * This merges sorted output runs
     */
    private Merger merger;
    /**
     * Has this SortArea been explicitly stopped?
     */
    private boolean stopped;
    /**
     * Exception thrown by one of the sort threads
     */
    private DatabaseException exception;

    public SortArea(SortManager sortManager) {
        this.sortManager = sortManager;

        // Number of sort buffers to use during the sort
        int nBuffers = sortManager.getNBuffers();

        // Allocate 3 row data and index partitions for the pipeline
        part1 = new Part(nBuffers, sortManager.getBufferSize());
        part2 = new Part(nBuffers, sortManager.getBufferSize());
        part3 = new Part(nBuffers, sortManager.getBufferSize());

        // Create pipeline queues
        fetchQ = new ArrayBlockingQueue<>(3);
        sortQ = new ArrayBlockingQueue<>(3);
        storeQ = new ArrayBlockingQueue<>(3);
        mergeQ = new ArrayBlockingQueue<>(3);

        // Create the run merger
        merger = new Merger(sortManager);

        // Create pipeline threads
        sortT = new SortThread();
        storeT = new StoreThread();
        mergeT = new MergeThread();
    }

    /**
     * Permanently stops the SortArea.
     */
    public void stop() {
        synchronized (this) {
            if (!stopped) {
                stopped = true;

                interrupt();

                fetchQ.clear();
                sortQ.clear();
                storeQ.clear();
                mergeQ.clear();

                sortT = null;
                storeT = null;
                mergeT = null;
                fetchQ = null;
                sortQ = null;
                storeQ = null;
                mergeQ = null;
                part1 = null;
                part2 = null;
                part3 = null;
                merger = null;
            }
        }
    }

    /**
     * Grabs the SortArea for exclusive use by the specified transaction.
     */
    public void grab(Transaction trans) {
        synchronized (transLock) {
            this.trans = trans;
        }
    }

    /**
     * Releases the SortArea back to the pool for use by another transaction.
     */
    public void release() {
        synchronized (transLock) {
            trans = null;
        }
    }

    /**
     * Returns true if the SortArea is held by an uncommitted transaction.
     */
    public boolean isHeld() {
        synchronized (transLock) {
            return trans != null && !trans.isCommitted();
        }
    }

    /**
     * Sorts the input and returns a Scrollable with the ordered results. If
     * distinct, then remove duplicates from the result set.
     * TODO: support grouping functions, order-preserving key compression
     */
    public synchronized RowStream sort(RowStream input, SortDelegate delegate)
            throws DatabaseException, InterruptedException {
        if (stopped) {
            Thread.currentThread().interrupt();
            throw new InterruptedException();
        }
        this.delegate = delegate;

        // The next row from the input
        Row next = input.allocateRow();

        // Handle the first object in the run separately to account
        // for a possible holdover from a previous fetch
        if (!input.fetchNext(next)) {
            // Return the empty set
            return new EmptyRowStream(next);
        }

        // Try sorting the input in a single run
        if (part1.fetchRun(input, next)) {
            part1.sortRun(delegate);
            return part1.createRowStream(sortManager, trans, next);
        }

        // Otherwise use a multithreaded, pipelined merge-sort
        preparePipeline();

        try {
            // Repeat until end-of-input
            Part part;
            boolean eof;
            do {
                // Grab the next available partition space
                part = fetchQ.take();
                if (isDone())
                    break;

                // Get the next run (returns the next object to participate in a
                // subsequent run)
                eof = part.fetchRun(input, next);

                sortQ.put(part);
            } while (!eof && !isDone());

            waitUntilDone();

            if (getException() != null) {
                interrupt();
                throw getException();
            }

            return merger.output(trans, next);
        } finally {
            clearPipeline();
        }
    }

    void clearPipeline() throws DatabaseException {
        fetchQ.clear();
        sortQ.clear();
        storeQ.clear();

        // Drop any un-merged runs if left lying around
        Run run;
        while ((run = mergeQ.poll()) != null) {
            run.close(trans);
        }

        merger.clear(trans);
    }

    void preparePipeline() throws InterruptedException {
        setException(null);
        setDone(false);

        // Make sure the threads are up and running
        if (!sortT.isValid()) {
            sortT = new SortThread();
        }
        if (!storeT.isValid()) {
            storeT = new StoreThread();
        }
        if (!mergeT.isValid()) {
            mergeT = new MergeThread();
        }

        // Initialize the available partitions queue
        fetchQ.put(part2);
        fetchQ.put(part3);

        // Place the first filled partition on the sort queue
        sortQ.put(part1);
    }

    private void interrupt() {
        sortT.interrupt();
        storeT.interrupt();
        mergeT.interrupt();

        try {
            sortT.join();
            storeT.join();
            mergeT.join();
        } catch (InterruptedException ignored) {
        }
    }

    DatabaseException getException() {
        synchronized (doneLock) {
            return exception;
        }
    }

    void setException(DatabaseException exception) {
        synchronized (doneLock) {
            this.exception = exception;
            setDone(true);
            interrupt();
        }
    }

    boolean isDone() {
        synchronized (doneLock) {
            return done;
        }
    }

    void setDone(boolean b) {
        synchronized (doneLock) {
            if (done = b) {
                doneLock.notify();
            }
        }
    }

    void waitUntilDone() throws InterruptedException {
        synchronized (doneLock) {
            while (!done) {
                doneLock.wait();
            }
        }
    }

    abstract class PipelineThread extends Thread {
        /**
         * The thread is still valid
         */
        private boolean valid = true;

        PipelineThread(String name) {
            super(name);
            start();
        }

        abstract void handlePipeline() throws DatabaseException,
                InterruptedException;

        @Override
        public void run() {
            try {
                while (!Thread.interrupted()) {
                    try {
                        handlePipeline();
                    } catch (DatabaseException dbe) {
                        setException(dbe);
                    }
                }
            } catch (InterruptedException ignored) {
            } finally {
                setValid();
            }
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        synchronized boolean isValid() {
            return valid;
        }

        synchronized void setValid() {
            valid = false;
        }
    }

    /**
     * Sorts runs.
     */
    class SortThread extends PipelineThread {
        SortThread() {
            super("Cooldb - Sort - Sort");
        }

        void handlePipeline() throws InterruptedException, DatabaseException {
            Part part = sortQ.take();
            if (!isDone()) {
                part.sortRun(delegate);
                storeQ.put(part);
            }
        }
    }

    /**
     * Stores runs to temporary external storage.
     */
    class StoreThread extends PipelineThread {
        StoreThread() {
            super("Cooldb - Sort - Store");
        }

        void handlePipeline() throws InterruptedException, DatabaseException {
            try {
                Part part = storeQ.take();
                if (!isDone()) {
                    Run run = part.storeRun(sortManager, trans);
                    fetchQ.put(part);
                    mergeQ.put(run);
                }
            } catch (DatabaseException e) {
                storeQ.clear();
                throw e;
            }
        }
    }

    /**
     * Merges runs into a single output run.
     */
     class MergeThread extends PipelineThread {
        MergeThread() {
            super("Cooldb - Sort - Merge");
        }

        void handlePipeline() throws DatabaseException, InterruptedException {
            Run run = mergeQ.take();
            if (!isDone()) {
                if (merger.mergeRun(trans, run, delegate)) {
                    setDone(true);
                }
            }
        }
    }
}
