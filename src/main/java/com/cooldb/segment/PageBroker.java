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

import com.cooldb.buffer.*;
import com.cooldb.log.*;
import com.cooldb.transaction.RollbackException;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;

/**
 * PageBroker brokers access to the pages of a specific Segment on behalf of a
 * specific Transaction.
 * <p>
 * PageBroker provides pin/unPin methods as well as methods to attach various
 * types of log records to the operations conducted on the page while it is
 * pinned. The unPin method ensures that the logs are properly written to the
 * file system prior to releasing the page.
 * <p>
 * PageBroker is not thread-safe.
 */

public class PageBroker {

    final Segment segment;
    final TransactionManager transactionManager;
    // the following are only valid while a page is pinned
    private PageBuffer pageBuffer;
    private RedoLog redoLog;
    private UndoLog undoLog;
    private CLR clr;
    private Pin pinType = Pin.NO_PIN;
    private byte pageType;
    private long flushed;

    // Package-private constructor (see AbstractSegmentMethod.allocPageBroker)
    PageBroker(Segment segment, TransactionManager transactionManager) {
        this.segment = segment;
        this.transactionManager = transactionManager;
    }

    public FilePage getPage() {
        return pageBuffer.getPage();
    }

    /**
     * Pin the page for read operations only.
     */
    public PageBuffer readPin(FilePage page) throws BufferNotFound,
            InterruptedException {
        checkPin();
        pinType = Pin.READ_PIN;
        return (pageBuffer = transactionManager.pin(page,
                                                    BufferPool.Mode.SHARED));
    }

    /**
     * Pin a version of the current page that is identified by the specified
     * transaction and version number.
     */
    public PageBuffer versionPin(Transaction trans, FilePage page, long version)
            throws BufferNotFound, InterruptedException, RollbackException {
        checkPin();
        pinType = Pin.VERSION_PIN;
        return (pageBuffer = transactionManager
                .pinVersion(trans, page, version));
    }

    /**
     * Pin the page for write operations without associated logging.
     * <p>
     * If the flag isNew is true, then the page will be pinned without first
     * reading it from the file system, and the contents filled with zeros.
     * <p>
     * The PageBuffer will be flushed to the file system when it is unPinned.
     */
    public PageBuffer writePin(FilePage page, boolean isNew)
            throws BufferNotFound, InterruptedException {
        checkPin();
        pinType = Pin.WRITE_PIN;
        if (isNew)
            return (pageBuffer = transactionManager.pinNew(page));
        else
            return (pageBuffer = transactionManager.pin(page,
                                                        BufferPool.Mode.EXCLUSIVE));
    }

    /**
     * Pin the page for temporary write operations without associated logging.
     * <p>
     * The page will be pinned without first reading it from the file system,
     * and if the page is not already in the buffer pool cache, the contents
     * will be filled with zeros.
     * <p>
     * The PageBuffer will be flushed to the file system when it is evicted from
     * the buffer pool only if the transaction is still active at the time;
     * otherwise, if the transaction is committed at the time of eviction, then
     * the contents will be discarded without being written to the file system.
     */
    public PageBuffer tempPin(FilePage page, Transaction trans)
            throws BufferNotFound, InterruptedException {
        checkPin();
        pinType = Pin.TEMP_PIN;
        return (pageBuffer = transactionManager.pinTemp(page, trans));
    }

    /**
     * Pin the page for updates and prepare for log records to be attached.
     */
    public PageBuffer logPin(FilePage page, byte pageType)
            throws BufferNotFound, InterruptedException {
        checkPin();
        pinType = Pin.LOG_PIN;
        this.pageType = pageType;

        return (pageBuffer = transactionManager.pin(page,
                                                    BufferPool.Mode.EXCLUSIVE));
    }

    RedoLog getRedoLog() {
        if (redoLog == null) {
            redoLog = new RedoLog();
            redoLog.setSegmentType(segment.getSegmentType());
            redoLog.setSegmentId(segment.getSegmentId());
            redoLog.setPageType(pageType);
            redoLog.setPage(pageBuffer.getPage());
        }
        return redoLog;
    }

    UndoLog getUndoLog() {
        if (undoLog == null) {
            undoLog = new UndoLog();
            undoLog.setSegmentType(segment.getSegmentType());
            undoLog.setSegmentId(segment.getSegmentId());
            undoLog.setPageType(pageType);
            undoLog.setPage(pageBuffer.getPage());
        }
        return undoLog;
    }

    /**
     * Pin the page and prepare for the redoLog to be applied.
     */
    public PageBuffer redoPin(RedoLog redoLog) throws BufferNotFound,
            InterruptedException {
        checkPin();
        pinType = Pin.REDO_PIN;

        this.redoLog = redoLog;

        return (pageBuffer = transactionManager.pin(redoLog.getPage(),
                                                    BufferPool.Mode.EXCLUSIVE));
    }

    /**
     * Pin the page and prepare for the undoLog to be applied.
     */
    public PageBuffer undoPin(UndoLog undoLog) throws BufferNotFound,
            InterruptedException {
        checkPin();
        pinType = Pin.UNDO_PIN;

        clr = new CLR(undoLog);

        return (pageBuffer = transactionManager.pin(undoLog.getPage(),
                                                    BufferPool.Mode.EXCLUSIVE));
    }

    /**
     * Use the given pageBuffer.
     */
    public PageBuffer bufferPin(PageBuffer pageBuffer) {
        checkPin();
        pinType = Pin.BUFFER_PIN;
        return this.pageBuffer = pageBuffer;
    }

    /**
     * unPin the page without needing to write transaction logs to the file
     * system. Corresponds to readPin, redoPin, and pageUndoPin.
     */
    public void unPin(BufferPool.Affinity affinity)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        switch (pinType) {
            case READ_PIN:
            case VERSION_PIN:
            case TEMP_PIN:
                transactionManager.unPin(pageBuffer, affinity);
                break;
            case WRITE_PIN:
                pageBuffer.flush();
                transactionManager.unPin(pageBuffer, affinity);
                break;
            case REDO_PIN:
                transactionManager.unPinRedo(pageBuffer, affinity, redoLog
                        .getAddress());
                break;
            default:
                break;
        }

        flushed = 0;
        pageBuffer = null;
        redoLog = null;
        undoLog = null;
        clr = null;
        pinType = Pin.NO_PIN;
    }

    /**
     * unPin the page and write attached transaction log records to the file
     * system. Corresponds to logPin and undoPin.
     */
    public void unPin(Transaction trans, BufferPool.Affinity affinity)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        switch (pinType) {
            case LOG_PIN:
            case UNDO_PIN:
                flushLogs(trans);
                if (flushed > 0)
                    transactionManager.unPinDirty(pageBuffer, affinity, flushed);
                else
                    pinType = Pin.READ_PIN;
                break;
            default:
                break;
        }
        unPin(affinity);
    }

    /**
     * Flush all accrued logs. Ensures that any CLR is written last.
     */
    private void flushLogs(Transaction trans) throws LogExhaustedException,
            BufferNotFound, InterruptedException {
        if (undoLog != null && redoLog != null) {
            transactionManager.writeUndoRedo(trans, pageBuffer, undoLog,
                                             redoLog);
            flushed = redoLog.getAddress();
            undoLog = null;
            redoLog = null;
        } else if (redoLog != null) {
            transactionManager.writeRedo(trans, pageBuffer, redoLog);
            flushed = redoLog.getAddress();
            redoLog = null;
        }
        if (clr != null) {
            transactionManager.writeCLR(trans, pageBuffer, clr);
            flushed = clr.getAddress();
            clr = null;
        }
    }

    public LogData attachUndo(byte type, int size) {
        return getUndoLog().allocate(type, size);
    }

    public LogData attachLogicalUndo(byte type, int size) {
        UndoLog undoLog = getUndoLog();
        undoLog.setPage(FilePage.NULL);
        return undoLog.allocate(type, size);
    }

    public LogData attachRedo(byte type, int size) {
        return getRedoLog().allocate(type, size);
    }

    public LogData attachCLR(byte type, int size) {
        return clr.allocate(type, size);
    }

    public void attachUndo(byte type, DBObject obj) {
        LogData ld = getUndoLog().allocate(type, obj.storeSize());
        obj.writeTo(ld.getData());
    }

    public void attachRedo(byte type, DBObject obj) {
        LogData ld = getRedoLog().allocate(type, obj.storeSize());
        obj.writeTo(ld.getData());
    }

    public void attachCLR(byte type, DBObject obj) {
        LogData ld = clr.allocate(type, obj.storeSize());
        obj.writeTo(ld.getData());
    }

    public void attachCLR(LogData rld) {
        clr.add(rld);
    }

    public UndoPointer writeUndoRedo(Transaction trans)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        UndoPointer addr = null;

        if (undoLog != null && redoLog != null) {
            transactionManager.writeUndoRedo(trans, pageBuffer, undoLog,
                                             redoLog);
            addr = undoLog.getAddress();
            flushed = redoLog.getAddress();
            undoLog = null;
            redoLog = null;
        }

        return addr;
    }

    private void checkPin() {
        if (pinType != Pin.NO_PIN)
            throw new RuntimeException(
                    "Internal error: PageBroker already pinned.");
    }

    // pin types
    enum Pin {
        NO_PIN, READ_PIN, WRITE_PIN, LOG_PIN, REDO_PIN, UNDO_PIN, BUFFER_PIN, VERSION_PIN, TEMP_PIN
    }
}
