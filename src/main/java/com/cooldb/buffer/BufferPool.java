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

package com.cooldb.buffer;

/**
 * BufferPool maintains a pool of PageBuffers that may be mapped onto physical
 * locations in the file system. A PageBuffer can be associated with a FilePage
 * through one of several 'pin' methods, which have corresponding 'unPin'
 * methods. A page may be pinned in one of two modes: SHARED and EXCLUSIVE.
 * <p>
 * After a page is unpinned, BufferPool maintains the association between the
 * PageBuffer and FilePage for as long as possible given a set of hints and a
 * least-recently-used replacement algorithm. The hints describe three levels of
 * affinity for the mapping: LOVED, LIKED, and HATED.
 * <p>
 * BufferPool supports a steal/no-force eviction policy. This allows the buffer
 * manager to make intelligent decisions about when and what to flush to disk in
 * order to minimize file system read/write operations on behalf of various
 * concurrent transactions. 'Steal' means that page updates may be flushed from
 * the buffer pool at arbitrary times due to the pin operations of various
 * transactions competing for the limited number of buffers (one transaction may
 * 'steal' the buffers used by another transaction), and 'no-force' means that
 * modified pages may remain in the buffer pool even after the relevant
 * transactions commit (the changes are not necessarily forced to disk at commit
 * time). Transaction atomicity and durability properties are guaranteed by
 * adherence to the Write-Ahead-Logging (WAL) protocol.
 * <p>
 * BufferPool supports the WAL protocol through the use of an optional
 * 'WriteAheadLogging' delegate. If this delegate is specified, the BufferPool
 * ensures that no modified PageBuffer is written to the file system before all
 * log records needed for recovery have been written to the file system. In
 * order for this to work, the BufferPool must be informed of the log sequence
 * number of the last log record written for the page when it is unpinned. If
 * the delegate is not specified, then the BufferPool writes modified pages to
 * the file system without logging.
 * <p>
 * BufferPool supports Multi-Version-Concurrency-Control (MVCC) through mvccPin,
 * which associates a PageBuffer with a specific version of the FilePage.
 * BufferPool simply provides a way of sharing the buffers used in MVCC. The
 * first pin of a versioned page returns a copy of the current version of the
 * page in the PageBuffer pinned in mode EXCLUSIVE so that the caller may
 * materialize the specific older version of the page in the supplied buffer.
 * Subsequent pins of the versioned page return that PageBuffer pinned in mode
 * SHARED until the buffer is replaced. These versions of the FilePage are never
 * written to disk.
 * <p>
 * The ensureCapacity method can be used to set the number of page buffers
 * allocated by BufferPool for pin operations. It will increase the number of
 * available buffers but it will ignore any attempt to reduce the number.
 * BufferPool automatically increases its capacity to the minimum required by
 * usage but can be limited to a maximum capacity.
 * <p>
 * The BufferPool contains an optional DatabaseWriter that can asynchronously
 * write modified page buffers to the file system whenever the number of dirty
 * page buffers exceeds some threshold, specified as a ratio of the number of
 * dirty page buffers to the buffer pool capacity. The DatabaseWriter sorts the
 * pages by their physical location in the file system so that writes are
 * physically clustered for efficiency. The DatabaseWriter can be started with
 * the 'start' method and stopped with the 'stop' method.
 */

public interface BufferPool {

    /**
     * Ensure that the BufferPool has allocated at least the number of page
     * buffers specified by capacity for pin operations.
     */
    void ensureCapacity(int capacity);

    /**
     * Ensure that the BufferPool does not allocate more than the number of page
     * buffers specified by maxCapacity for pin operations. A value of zero
     * indicates unlimited capacity and is the default.
     */
    void setMaximumCapacity(int maxCapacity);

    /**
     * Pin a specific file page in memory and associate it with a buffer, which
     * is returned. The Mode can be either SHARED or EXCLUSIVE.
     * <p>
     * If it is SHARED, then the same page and buffer may be pinned repeatedly
     * in SHARED mode any number of times until all pins have been removed by
     * corresponding calls to unPin. Any attempt to pin the page in EXCLUSIVE
     * mode while it is already pinned will block until the page is no longer
     * pinned in either mode. The returned PageBuffer in SHARED mode cannot be
     * modified.
     * <p>
     * If the mode is EXCLUSIVE, then any attempt to pin the page again in
     * either mode will block until the corresponding call to unPin is made.
     * <p>
     * In either mode, if the page is already in the buffer pool, then its
     * buffer will be returned immediately. However, if it is not already in the
     * pool, one of the other pages will be chosen for replacement and flushed
     * to file if necessary, and the requested page will be fetched from file
     * into the buffer vacated by the replaced page. The replacement strategy
     * favors stale pages over more recently pinned pages, HATED pages over
     * LIKED pages, and LIKED pages over LOVED pages (see unPin for more on
     * these Affinity types).
     * <p>
     * Pin will throw an InterruptedException if its thread is interrupted while
     * waiting on another thread that is holding the page exclusively.
     */
    PageBuffer pin(FilePage page, Mode mode) throws BufferNotFound,
            InterruptedException;

    /**
     * Try to pin the page in the given mode. Return the page buffer as in pin
     * if the request can be satisfied without blocking on a mode conflict;
     * otherwise return null rather than block.
     */
    PageBuffer tryPin(FilePage page, Mode mode) throws BufferNotFound;

    /**
     * Pin a version of the current page that is identified by the specified
     * transaction and version number. If this pin does not find the version
     * already resident in the buffer pool, a copy of the original page is made
     * and the returned PageBuffer is pinned with mode set to EXCLUSIVE. If on
     * the other hand the pin finds the version already resident in the buffer
     * pool, then the returned PageBuffer is pinned with mode SHARED.
     */
    PageBuffer pinVersion(FilePage page, long transId, long version)
            throws BufferNotFound, InterruptedException;

    /**
     * Pin the page as new, zero-filled and without reading it in from the file
     * system, in mode EXCLUSIVE.
     */
    PageBuffer pinNew(FilePage page) throws BufferNotFound,
            InterruptedException;

    /**
     * Pin the page without first reading it from the file system, and if the
     * page is not already in the buffer pool, fill it with zeros.
     * <p>
     * The page will be flushed to the file system when it is evicted from the
     * buffer pool only if the given transaction is still active at the time;
     * otherwise, if the transaction is committed at the time of eviction, then
     * the contents will be discarded without being written to the file system.
     */
    PageBuffer pinTemp(FilePage page, long transId) throws BufferNotFound,
            InterruptedException;

    /**
     * unPin releases a corresponding pin on a specific PageBuffer and indicates
     * an affinity for the page. The affinity is a hint to the buffer manager
     * that may be used when pages are selected for replacement. The buffer
     * manager will replace all HATED pages before any LIKED page, all LIKED
     * pages before any LOVED page. HATED is a weak affinity, LOVED is a strong
     * affinity. A weaker affinity may be changed to a stronger affinity, but a
     * stronger affinity cannot be changed to a weaker affinity unless the page
     * is replaced and subsequently pinned anew. A LOVED page can only be
     * replaced when all other buffered pages are also LOVED.
     * <p>
     * This form of unPin should only be used if page buffer was not modified
     * during the pin (see the forms of unPinDirty below).
     */
    void unPin(PageBuffer page, Affinity affinity);

    /**
     * unPin the page as 'dirty', or with modifications, and ensure that the
     * page is written to disk prior to buffer eviction. This form of unPin
     * should only be used if the page updates are not being logged.
     */
    void unPinDirty(PageBuffer page, Affinity affinity);

    /**
     * unPin the page as 'dirty' and enforce the Write-Ahead-Logging (WAL)
     * protocol. This form of unPinDirty guarantees that by the time the page is
     * written to disk, all log records upto and including the given page
     * log-sequence-number will already have been written safely to disk. Note
     * that even if logging is not enabled through the use of a
     * WriteAheadLogging delegate, this form of unpin must be used if the page
     * is modified in order for the page to be written to the file system, and
     * in this case any endLSN value will suffice.
     */
    void unPinDirty(PageBuffer page, Affinity affinity, long endLSN);

    /**
     * Write to the file system as many modified buffers as possible.
     * <p>
     * Any pages pinned in EXCLUSIVE mode at the time of the checkPoint will be
     * skipped. CheckPoint returns the list of remaining resident dirty pages
     * each with the minimum log-sequence-number needed to recover the page in
     * the event of a failure.
     * <p>
     * Use sparingly! This may result in one or more very expensive file syncs.
     */
    DPEntry[] checkPoint();

    /**
     * Start the DatabaseWriter thread.
     */
    void start();

    /**
     * Stop the DatabaseWriter thread.
     */
    void stop();

    /**
     * Set the ratio of dirty pages to buffer pool capacity that triggers the
     * DatabaseWriter to take a checkpoint. Ratio must be in the range 0 to 1.
     */
    void setDirtyPageRatio(double ratio);

    /**
     * Set the Write-Ahead-Logging (WAL) delegate.
     */
    void setWriteAheadLogging(WriteAheadLogging wal);

    /**
     * Mode types.
     */
    enum Mode {
        SHARED, EXCLUSIVE
    }

    /**
     * Affinity types.
     */
    enum Affinity {
        NONE, HATED, LIKED, LOVED
    }
}
