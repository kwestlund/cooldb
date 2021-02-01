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

import com.cooldb.buffer.BufferNotFound;

import java.util.Iterator;

/**
 * The LogManager manages reading and writing undo and redo log records.
 * <p>
 * LogManager is thread-safe, but without any of its methods being synchronized.
 * <p>
 * Synchronization is handled by the underlying redo and undo LogWriters. This
 * has important implications for deadlock avoidance, particularly with the
 * CheckpointWriter.
 */

public class LogManager implements WAL {

    private final RedoLogWriter redoLogWriter;
    private final UndoLogWriter undoLogWriter;
    private long committed;

    public LogManager(RedoLogWriter redoLogWriter, UndoLogWriter undoLogWriter) {
        this.redoLogWriter = redoLogWriter;
        this.undoLogWriter = undoLogWriter;

        // Make sure we never write a redo log record to address zero. Start at
        // 1.
        // This permits a distinction between a newly created page and the first
        // update to that page,
        // since the newly created pages are zero-filled and therefore have
        // pageLSN = 0
        redoLogWriter.setEndOfLog(1);
    }

    /**
     * Write an update to a specific page, described by both undo and redo log
     * records, to the log file(s).
     */
    public void writeUndoRedo(UndoLog undoLog, RedoLog redoLog)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        // Write Undo record first
        undoLogWriter.write(undoLog);

        redoLog.setUndoNxtLSN(undoLog.getAddress());

        // Now write the Redo record, pointing to the undo record
        redoLogWriter.write(redoLog);
    }

    /**
     * Write the redoLog record to the log file.
     */
    public void writeRedo(RedoLog redoLog) throws LogExhaustedException {
        redoLogWriter.write(redoLog);
    }

    /**
     * Iterate over the redo log entries from the given starting point
     */
    public Iterator<RedoLog> redoIterator(RedoLog logBuffer) {
        return redoLogWriter.iterator(logBuffer);
    }

    /**
     * Iterate over the undo log entries from the given starting point
     */
    public Iterator<UndoLog> undoIterator(UndoLog logBuffer) {
        return undoLogWriter.iterator(logBuffer);
    }

    /**
     * Random access to redo log records
     */
    public void readRedo(RedoLog redoLog) throws LogNotFoundException {
        redoLogWriter.read(redoLog);
    }

    /**
     * Random access to redo log records
     */
    public void readUndo(UndoLog undoLog) throws LogNotFoundException,
            BufferNotFound, InterruptedException {
        undoLogWriter.read(undoLog);
    }

    public long getEndOfLog() {
        return redoLogWriter.getEndOfLog();
    }

    public void setEndOfLog(long endOfLog) {
        redoLogWriter.setEndOfLog(endOfLog);
    }

    public long getStartOfLog() {
        return redoLogWriter.getDoNotOverwrite();
    }

    public void setStartOfLog(long lsn) {
        redoLogWriter.setDoNotOverwrite(lsn);
    }

    public int getRemaining() {
        return redoLogWriter.getRemaining();
    }

    public int getRemaining(long recLSN) {
        return redoLogWriter.getRemaining(recLSN);
    }

    // WriteAheadLogging

    public void setMinUndo(UndoPointer lsn) throws BufferNotFound,
            InterruptedException {
        undoLogWriter.setMinUndo(lsn);
    }

    public UndoPointer getEndOfUndoLog() {
        return undoLogWriter.getEndOfLog();
    }

    public long getRedoCapacity() {
        return redoLogWriter.getCapacity();
    }

    /**
     * Flush the logs upto and including the specified log record.
     */
    public synchronized long flushTo(long lsn) {
        try {
            // Quickly check whether the logs need to be flushed
            if (lsn > committed) {
                undoLogWriter.flush();
                redoLogWriter.flushTo(lsn);
                committed = lsn;
            }
            return committed;
        } catch (BufferNotFound bnf) {
            throw new RuntimeException(
                    "Internal Error.  BufferNotFound raised while flushing logs.");
        } catch (InterruptedException bnf) {
            throw new RuntimeException(
                    "Internal Error.  InterruptedException raised while flushing logs.");
        }
    }

    /**
     * Flush the logs upto and excluding the specified address, then move the
     * doNotOverwrite mark to the new firewall point.
     */
    public void moveFirewallTo(long lsn) {
        try {
            // Quickly check whether the logs need to be flushed
            undoLogWriter.flush();
            redoLogWriter.moveFirewallTo(lsn);
        } catch (BufferNotFound bnf) {
            throw new RuntimeException(
                    "Internal Error.  BufferNotFound raised while flushing logs.");
        } catch (InterruptedException bnf) {
            throw new RuntimeException(
                    "Internal Error.  InterruptedException raised while flushing logs.");
        }
    }

    /**
     * Force logs from OS buffers to disk.
     */
    public void force() {
        undoLogWriter.force();
        redoLogWriter.force();
    }
}
