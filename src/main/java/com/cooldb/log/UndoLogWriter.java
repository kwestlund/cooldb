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

package com.cooldb.log;

import com.cooldb.buffer.*;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * UndoLogWriter manages storage and retrieval of undo log records using an
 * extendable file, linked-list, page-oriented mechanism.
 * <p>
 * The available space will grow as needed, limited only by file system
 * resources. Space is allocated and garbage collected in fixed-size chunks of
 * disk space as transactions write to the log and ultimately commit.
 * <p>
 * Growth may be constrained to a maximum file size, in which case long-running
 * transactions will be cancelled before the maximum is reached in order to
 * reclaim log space.
 */

public class UndoLogWriter {

    /**
     * Default number of pages in each log extent, the unit of space allocation
     */
    public static final short DEFAULT_EXTENT_SIZE = 64;

    /**
     * Page identifier of the page in the undo log file that contains log meta
     * data.
     */
    public static final int CONTROL_PAGE_ID = 0;
    // This must be some positive number. Zero is not allowed.
    private static final long FIRST_LSN = 1;
    private final Object commitPointLock = new Object();
    private FileManager fileManager;
    private BufferPool bufferPool;
    private short fileId;
    private ControlPage controlPage;
    private ByteBuffer stage;
    private FilePage commitPoint;
    private UndoPointer endOfLog;

    public UndoLogWriter(DBFile dbf) {
        fileManager = new FileManagerImpl();
        fileManager.addFile(fileId, dbf);
        bufferPool = new BufferPoolImpl(fileManager);
        bufferPool.ensureCapacity(DEFAULT_EXTENT_SIZE);
        bufferPool.setMaximumCapacity(DEFAULT_EXTENT_SIZE * 2);
    }

    public void start() throws InterruptedException, BufferNotFound {
        if (controlPage == null) {
            // Pin the first page of the log, which will be used to
            // manage the log space, and keep it pinned.
            controlPage = new ControlPage(bufferPool.pin(new FilePage(fileId,
                                                                      CONTROL_PAGE_ID), BufferPool.Mode.EXCLUSIVE));

            endOfLog = new UndoPointer(
                    new FilePage(fileId, CONTROL_PAGE_ID + 1),
                    (short) DataPage.LOG_DATA, FIRST_LSN);

            // If the log has never been used, then skip recovery
            FilePage head = new FilePage();
            controlPage.getHead(head);
            if (head.getPageId() > 0)
                // Recover from a failure during file extension
                recover();
        }
    }

    /**
     * Create the UndoLog for the first time and initialize meta data. if
     * (controlPage != null) { bufferPool.unPin(controlPage.pageBuffer,
     * BufferPool.Affinity.HATED); controlPage = null; }
     * <p>
     * The next LSN to be assigned to the first log record written is 1 (zero is
     * reserved), and each record thereafter is assigned an LSN sequentially
     * greater than the previous.
     */
    public void create() throws BufferNotFound, InterruptedException {
        clear();
        try {
            // Create the control page
            controlPage = new ControlPage(bufferPool.pinNew(new FilePage(
                    fileId, CONTROL_PAGE_ID)));

            endOfLog = new UndoPointer(
                    new FilePage(fileId, CONTROL_PAGE_ID + 1),
                    (short) DataPage.LOG_DATA, FIRST_LSN);
            FilePage page = new FilePage(); // null page
            controlPage.setHead(page);
            controlPage.setTail(endOfLog);
            controlPage.setFree(page);
            controlPage.setUndoFree(page);
            controlPage.setUndoPage(page);
            controlPage.setRedoGCPage(page);
            controlPage.setRedoGCNext(page);
            controlPage.setExtents(0);
            controlPage.setExtentSize(DEFAULT_EXTENT_SIZE);

            // Initialize the first extent of data pages
            allocateExtent();

            // Remove the first extent from the free list and assign it to the
            // head
            controlPage.getFree(page);
            controlPage.setHead(page);
            controlPage.setTail(endOfLog);
            page.setNull();
            controlPage.setFree(page);

            controlPage.pageBuffer.flush();
        } finally {
            clear();
        }
        start();
    }

    public void stop() {
        clear();

        bufferPool.stop();
        fileManager.clear();

        bufferPool = null;
        fileManager = null;

        stage = null;
        commitPoint = null;
        endOfLog = null;
    }

    private void clear() {
        if (controlPage != null) {
            bufferPool.unPin(controlPage.pageBuffer, BufferPool.Affinity.HATED);
            controlPage = null;
        }
    }

    public synchronized void write(UndoLog rec) throws LogExhaustedException,
            BufferNotFound, InterruptedException {
        int size = rec.storeSize();
        if (stage == null || stage.capacity() < size)
            stage = ByteBuffer.allocate(size);
        else
            stage.clear();

        rec.setAddress(endOfLog);

        rec.writeTo(stage);

        byte[] stageArray = stage.array();

        int written = 0;

        int remaining;

        while (size > 0) {
            DataPage dataPage = new DataPage(bufferPool.pin(endOfLog.getPage(),
                                                            BufferPool.Mode.EXCLUSIVE));
            try {
                dataPage.putRecordSize(endOfLog.getOffset(), size);
                endOfLog.setOffset((short) (endOfLog.getOffset() + 4));

                // determine space remaining on page: page capacity - endOfLog
                // offset
                remaining = DataPage.DATA_SPACE - endOfLog.getOffset();

                // determine the chunk of our record that will go in the space
                // remaining
                int chunk = Math.min(size, remaining);

                dataPage
                        .write(endOfLog.getOffset(), stageArray, written, chunk);

                written += chunk;
                size -= chunk;
                remaining -= chunk;

                if (remaining < 4) {
                    dataPage.getNextPage(endOfLog.getPage());
                    if (endOfLog.getPage().isNull())
                        extend(dataPage);
                    dataPage.getNextPage(endOfLog.getPage());
                    endOfLog.setOffset((short) DataPage.LOG_DATA);
                } else
                    endOfLog.setOffset((short) (endOfLog.getOffset() + chunk));

                dataPage.setLastLSN(endOfLog.getLSN());
            } finally {
                bufferPool.unPinDirty(dataPage.pageBuffer,
                                      BufferPool.Affinity.LIKED);
            }
        }
        endOfLog.setLSN(endOfLog.getLSN() + 1);
    }

    /**
     * Read the record pointed to by rec, into rec, and return a pointer to the
     * next record, or null if the requested record could not be found.
     */
    public synchronized UndoPointer read(UndoLog rec)
            throws LogNotFoundException, BufferNotFound, InterruptedException {
        long address = rec.getAddress().getLSN();

        if (address >= endOfLog.getLSN()) {
            throw new LogNotFoundException(
                    "Internal Error: attempt made to read past the end-of-log.");
        }
        if (address < getMinUndo().getLSN()) {
            throw new LogNotFoundException(
                    "Internal Error: attempt made to read an old log record that has been garbage collected.");
        }
        return _read(rec);
    }

    /**
     * Read the record pointed to by rec, into rec, and return a pointer to the
     * next record, or null if the requested record could not be found.
     */
    public synchronized UndoPointer _read(UndoLog rec) throws BufferNotFound,
            InterruptedException {
        UndoPointer address = rec.getAddress();
        FilePage page = new FilePage(address.getPage());
        short offset = address.getOffset();
        int nread = 0;
        int size;
        long lastLSN = address.getLSN();

        do {
            DataPage dataPage = new DataPage(bufferPool.pin(page,
                                                            BufferPool.Mode.SHARED));
            try {
                // Make sure we are not reading beyond the end-of-log
                if (dataPage.getLastLSN() < lastLSN)
                    return null;

                size = dataPage.getRecordSize(offset);
                offset += 4;

                if (size == 0)
                    return null;

                ensureStageCapacity(size, nread);

                byte[] stageArray = stage.array();
                int chunk = Math.min(DataPage.DATA_SPACE - offset, size);

                dataPage.read(offset, stageArray, nread, chunk);
                nread += chunk;

                if (nread < size) {
                    // Set the page to the one pointed to by nextPage
                    dataPage.getNextPage(page);
                    offset = (short) DataPage.LOG_DATA;
                    lastLSN = dataPage.getLastLSN();
                } else {
                    long requestedAddress = rec.getAddress().getLSN();
                    rec.readFrom(stage);
                    if (requestedAddress != rec.getAddress().getLSN())
                        throw new RuntimeException(
                                "Requested log-sequence-number does not match the one found at the given location.");

                    offset += size;

                    int remaining = DataPage.DATA_SPACE - offset;
                    if (remaining < 4) {
                        // Set the page to the one pointed to by nextPage
                        dataPage.getNextPage(page);
                        offset = (short) DataPage.LOG_DATA;
                    }

                    // Set the pointer to the next record
                    return new UndoPointer(page, offset, rec.getAddress()
                            .getLSN() + 1);
                }
            } finally {
                bufferPool
                        .unPin(dataPage.pageBuffer, BufferPool.Affinity.LIKED);
            }
        } while (true);
    }

    /**
     * Flush all log records written to this point in time.
     */
    public void flush() throws BufferNotFound, InterruptedException {
        // Flush all dirty pages sequentially starting with the commitPoint page
        // and continuing until we reach the current end-of-log.
        // Allow writes to continue concurrently.

        // Synchronize first on commitPoint, then read the end-of-log pointer
        // (which
        // synchronizes on the UndoLogWriter (this)) to avoid deadlock or
        // conflict
        // with any ongoing garbage collection operation
        synchronized (commitPointLock) {

            // Take a snapshot of the end-of-log only after synchronizing on
            // commitPoint
            // to ensure that the commitPoint never gets ahead of the end-of-log
            UndoPointer eol = getEndOfLog();

            while (true) {
                PageBuffer pb = bufferPool.pin(commitPoint,
                                               BufferPool.Mode.SHARED);
                try {
                    pb.flush();
                    if (commitPoint.equals(eol.getPage()))
                        break;
                    DataPage.getNextPage(pb, commitPoint);
                } finally {
                    bufferPool.unPin(pb, BufferPool.Affinity.LIKED);
                }
            }
            synchronized (this) {
                controlPage.setTail(eol);
            }
        }
    }

    /**
     * Force logs from OS buffers to disk.
     */
    public void force() {
        fileManager.force(fileId);
    }

    public synchronized UndoPointer getEndOfLog() {
        return new UndoPointer(endOfLog);
    }

    public synchronized UndoPointer getMinUndo() {
        UndoPointer minUndo = new UndoPointer();
        controlPage.getMinUndo(minUndo);
        return minUndo;
    }

    /**
     * Inform the UndoLogWriter that all log records prior to minUndo are no
     * longer needed by running transactions and may be garbage collected.
     */
    public void setMinUndo(UndoPointer minUndo) throws BufferNotFound,
            InterruptedException {
        // During garbage collection, block all other operations on the log
        // Synchronize first on commitPoint, then on UndoLogWriter (this) to
        // avoid
        // deadlock with concurrent invocations of flush
        synchronized (commitPointLock) {
            // Flush pages first
            flush();
            garbageCollect(minUndo);
        }
    }

    // Iterate over the log entries from the given starting point
    public Iterator<UndoLog> iterator(UndoLog logBuffer) {
        return new LogIterator(logBuffer);
    }

    private void ensureStageCapacity(int size, int nread) {
        if (stage == null)
            stage = ByteBuffer.allocate(size);
        else if (nread == 0) {
            if (stage.capacity() < size)
                stage = ByteBuffer.allocate(size);
            else
                stage.clear();
        } else if (stage.remaining() < size) {
            ByteBuffer bb = ByteBuffer.allocate(stage.capacity() + size);
            stage.flip();
            bb.put(stage);
            stage = bb;
        }
    }

    private synchronized void garbageCollect(UndoPointer minUndo)
            throws BufferNotFound, InterruptedException {
        // Initiate garbage collection
        controlPage.setMinUndo(minUndo);

        // Set the new head to the first page of the extent containing the new
        // start of log (minUndo)
        FilePage newHead = new FilePage(minUndo.getPage());
        newHead.setPageId(newHead.getPageId() - (newHead.getPageId() - 1)
                % DEFAULT_EXTENT_SIZE);

        // Continue to pop extents from the head of the list, adding each to the
        // free list, until the head equals the new head.
        FilePage nextPage = new FilePage();
        FilePage head = new FilePage();
        controlPage.getHead(head);

        while (!head.equals(newHead)) {

            // Pin the last page of the head extent
            nextPage.setFileId(head.getFileId());
            nextPage.setPageId(head.getPageId() + DEFAULT_EXTENT_SIZE - 1);

            DataPage dataPage = new DataPage(bufferPool.pin(nextPage,
                                                            BufferPool.Mode.EXCLUSIVE));
            try {
                // Read the nextPage of the last page of the extent
                dataPage.getNextPage(nextPage);

                // Set the head to the nextPage of the last page of the head
                // extent
                controlPage.setHead(nextPage);

                // Set the nextPage of the last page of the head extent to the
                // free list
                controlPage.getFree(nextPage);
                dataPage.setNextPage(nextPage);

                // Set the free list to the head
                controlPage.setFree(head);

                // Read the new head
                controlPage.getHead(head);

                // Record redo info
                controlPage.setRedoGCPage(dataPage.pageBuffer.getPage());
                controlPage.setRedoGCNext(nextPage);
                controlPage.pageBuffer.flush();

                // Flush end page
                dataPage.pageBuffer.flush();

                // Commit GC action by clearing redo info from control page
                commitGarbageCollect();
            } finally {
                bufferPool
                        .unPin(dataPage.pageBuffer, BufferPool.Affinity.HATED);
            }
        }
    }

    /**
     * This commits a previous beginGetFreeExtent.
     */
    private void commitGarbageCollect() {
        // Remove redo info from the control page
        FilePage nextPage = new FilePage();
        controlPage.setRedoGCPage(nextPage);
        controlPage.setRedoGCNext(nextPage);

        // Flush the control page, which commits the garbage collection
        // operation
        controlPage.pageBuffer.flush();
    }

    /**
     * Redo a previous garbageCollect
     */
    private void redoGarbageCollect() throws InterruptedException,
            BufferNotFound {
        //
        FilePage page = new FilePage();
        FilePage nextPage = new FilePage();

        controlPage.getRedoGCPage(page);
        controlPage.getRedoGCNext(nextPage);

        DataPage dataPage = new DataPage(bufferPool.pin(page,
                                                        BufferPool.Mode.EXCLUSIVE));
        try {
            dataPage.getNextPage(page);
            if (!page.equals(nextPage)) {
                dataPage.setNextPage(nextPage);
                dataPage.pageBuffer.flush();
            }
        } finally {
            bufferPool.unPin(dataPage.pageBuffer, BufferPool.Affinity.HATED);
        }

        commitGarbageCollect();
    }

    /**
     * Extend the end-of-log by using the next free extent or by allocating a
     * new one. Set the first page of the next extent in the NextPage field of
     * the dataPage.
     */
    private void extend(DataPage dataPage) throws BufferNotFound,
            InterruptedException {
        // If there are no free extents, then allocate a new one from the end of
        // the file
        // and add it to the free extents list.
        FilePage free = new FilePage();
        controlPage.getFree(free);
        if (free.isNull())
            allocateExtent();

        // Take the first extent from the free extents list.
        // Make sure the operation is atomic.
        beginGetFreeExtent(dataPage);
        commitGetFreeExtent();
    }

    /**
     * Allocate a new extent at the end of the file and insert it into the free
     * extents list.
     */
    private void allocateExtent() throws BufferNotFound, InterruptedException {
        FilePage firstPage = new FilePage(fileId, controlPage.getExtents()
                * DEFAULT_EXTENT_SIZE + 1);
        FilePage nextPage = new FilePage(firstPage);

        for (int i = 0; i < DEFAULT_EXTENT_SIZE; i++) {
            PageBuffer pb = bufferPool.pinNew(nextPage);
            try {
                // Set the next page pointer, which must point to the next free
                // extent for the last page in the extent
                if (i == DEFAULT_EXTENT_SIZE - 1)
                    controlPage.getFree(nextPage);
                else
                    nextPage.setPageId(nextPage.getPageId() + 1);
                DataPage.setNextPage(pb, nextPage);
                pb.flush();
            } finally {
                bufferPool.unPin(pb, BufferPool.Affinity.LIKED);
            }
        }

        controlPage.setFree(firstPage);
        controlPage.setExtents(controlPage.getExtents() + 1);

        controlPage.pageBuffer.flush();
    }

    /**
     * Pop an extent from the free list and return the first page of the extent,
     * if one exists, otherwise return null. Since this action touches several
     * pages and must be atomic, undo information is recorded in the control
     * page with the update.
     */
    private void beginGetFreeExtent(DataPage tail) throws BufferNotFound,
            InterruptedException {

        // Remember the start of the free list and the tail page in case we need
        // to recover
        FilePage page = new FilePage();
        controlPage.getFree(page);
        controlPage.setUndoFree(page);
        controlPage.setUndoPage(tail.pageBuffer.getPage());

        // Set the tail nextPage to the start of the first free extent
        tail.setNextPage(page);

        // Pin the last page of the free extent
        page.setPageId(page.getPageId() + DEFAULT_EXTENT_SIZE - 1);
        DataPage dataPage = new DataPage(bufferPool.pin(page,
                                                        BufferPool.Mode.EXCLUSIVE));
        try {
            // Set the free list to its nextPage and set its nextPage to null
            dataPage.getNextPage(page);
            controlPage.setFree(page);
            boolean needsFlush = !page.isNull();
            if (needsFlush) {
                page.setNull();
                dataPage.setNextPage(page);
            }

            // Flush the control page with undo info first
            controlPage.pageBuffer.flush();

            // Flush the last page of the extent with null nextPage pointer, if
            // necessary
            if (needsFlush)
                dataPage.pageBuffer.flush();

            // Finally flush the tail page pointing to the no-longer free extent
            tail.pageBuffer.flush();
        } finally {
            bufferPool.unPin(dataPage.pageBuffer, BufferPool.Affinity.HATED);
        }
    }

    /**
     * This commits a previous beginGetFreeExtent.
     */
    private void commitGetFreeExtent() {
        // Remove undo info from the control page
        controlPage.setUndoFree(new FilePage());
        controlPage.setUndoPage(new FilePage());

        // Flush the control page, which commits the getFreeExtent operation
        controlPage.pageBuffer.flush();
    }

    /**
     * Undo a previous beginGetFreeExtent.
     */
    private void undoGetFreeExtent() throws InterruptedException,
            BufferNotFound {
        // Set the tail nextPage to null, if necessary
        FilePage page = new FilePage();
        controlPage.getUndoPage(page);

        DataPage dataPage = new DataPage(bufferPool.pin(page,
                                                        BufferPool.Mode.EXCLUSIVE));
        try {
            dataPage.getNextPage(page);
            if (!page.isNull()) {
                page.setNull();
                dataPage.setNextPage(page);
                dataPage.pageBuffer.flush();
            }
        } finally {
            bufferPool.unPin(dataPage.pageBuffer, BufferPool.Affinity.HATED);
        }

        // Insert the previously freed extent back into the free list by:
        // 1) Setting the nextPage of the last page of the failed free extent to
        // the free list,
        // 2) and setting the free list back to the start of the failed free
        // extent

        controlPage.getUndoFree(page);

        page.setPageId(page.getPageId() + DEFAULT_EXTENT_SIZE - 1);
        dataPage = new DataPage(bufferPool.pin(page, BufferPool.Mode.EXCLUSIVE));
        try {
            // 1) Set the nextPage of the last page of the failed free extent to
            // the free list
            controlPage.getFree(page);
            dataPage.setNextPage(page);

            // 2) Set the free list back to the start of the failed free extent
            controlPage.getUndoFree(page);
            controlPage.setFree(page);

            // Flush the last page of the re-inserted extent
            dataPage.pageBuffer.flush();

            // Finally remove the undo info from control page and flush it,
            // committing the undo
            commitGetFreeExtent();
        } finally {
            bufferPool.unPin(dataPage.pageBuffer, BufferPool.Affinity.HATED);
        }
    }

    /**
     * Recover from a failure during file extension or garbage collection if
     * necessary.
     */
    private void recover() throws InterruptedException, BufferNotFound {
        FilePage page = new FilePage();
        controlPage.getUndoPage(page);
        if (!page.isNull())
            undoGetFreeExtent();

        controlPage.getRedoGCPage(page);
        if (!page.isNull())
            redoGarbageCollect();

        // Find the end of the log
        endOfLog = new UndoPointer();
        controlPage.getTail(endOfLog);
        UndoLog undoLog = new UndoLog();
        undoLog.setAddress(endOfLog);
        while ((endOfLog = _read(undoLog)) != null)
            undoLog.setAddress(endOfLog);
        endOfLog = new UndoPointer(undoLog.getAddress());
        controlPage.setTail(endOfLog);

        // Set the commitPoint, which defines the point prior to which all
        // records are known to be flushed to the file system.
        commitPoint = new FilePage(endOfLog.getPage());
    }

    private class LogIterator implements Iterator<UndoLog> {
        private UndoLog log;
        private UndoLog next;
        private UndoPointer nextPointer;

        LogIterator(UndoLog log) {
            this.log = log;
            try {
                nextPointer = read(log);
                if (nextPointer == null)
                    next = null;
                else
                    next = log;
            } catch (Exception e) {
                this.log = null;
            }
        }

        public boolean hasNext() {
            if (next != null)
                return true;
            next = getNext();
            return next != null;
        }

        public UndoLog next() {
            if (next == null && !hasNext())
                throw new NoSuchElementException();
            UndoLog s = next;
            next = null;
            return s;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        private UndoLog getNext() {
            if (log != null && nextPointer != null) {
                // Prepare to read the next sequential log entry
                log.setAddress(nextPointer);
                try {
                    nextPointer = _read(log);
                } catch (Exception e) {
                    nextPointer = null;
                }
            }
            if (nextPointer == null)
                next = null;
            else
                next = log;
            return next;
        }
    }
}
