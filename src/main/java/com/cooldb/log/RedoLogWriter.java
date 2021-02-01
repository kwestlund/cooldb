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

import com.cooldb.buffer.DBFile;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * RedoLogWriter writes redo log records to a circular file.
 * <p>
 * Writes are first buffered, then flushed at each commit or checkpoint. The
 * buffering is not so much for performance, but to provide control over exactly
 * when redo logs are written to the file system. In particular, it is important
 * that undo logs be written to the file system before their corresponding redo
 * logs. Since undo logs are buffered, redo logs must also be buffered.
 */

public class RedoLogWriter {

    // include 1-byte indicator, 2-byte record size + 1-byte end-of-file marker
    static final int OVERHEAD = 4;
    // Attributes
    DBFile dbf; // redo log file
    Stage stage; // write staging buffer
    ByteBuffer readStage = ByteBuffer.allocate(1024); // read staging buffer
    private long endOfLog = 1; // lsn marking end of redo log data
    private long doNotOverwrite; // lsn marking start of redo log data

    public RedoLogWriter(DBFile dbf) {
        this.dbf = dbf;
    }

    public void stop() {
        dbf = null;
        readStage = null;
        stage.buffer = null;
        stage = null;
    }

    public synchronized long getDoNotOverwrite() {
        return doNotOverwrite;
    }

    public synchronized void setDoNotOverwrite(long lsn) {
        // Make sure never to go back, only forwards
        doNotOverwrite = Math.max(lsn, doNotOverwrite);
    }

    public synchronized long getEndOfLog() {
        return endOfLog;
    }

    public synchronized void setEndOfLog(long lsn) {
        endOfLog = lsn;
    }

    // WriteAheadLogging

    public synchronized int getRemaining() {
        return getRemaining(doNotOverwrite);
    }

    public synchronized int getRemaining(long recLSN) {
        return (int) (dbf.capacity() - (endOfLog - recLSN));
    }

    public long getCapacity() {
        return dbf.capacity();
    }

    public synchronized void write(RedoLog rec) throws LogExhaustedException {
        int storeSize = rec.storeSize() + OVERHEAD;
        long freeSpace = dbf.capacity() - (endOfLog - doNotOverwrite);

        // Make sure we do not overwrite needed log entries
        if (freeSpace < storeSize)
            throw new LogExhaustedException("out of log space");

        // Write into the buffer. Make sure the buffer is large enough to hold
        // twice the size of the log record,
        // which accounts for the possibility of a file wrap skipping space at
        // the end of the file, which must
        // also be recorded in the staging area. Grow the staging buffer in
        // chunks of 1/8 the total redo space.

        if (stage == null)
            stage = new Stage();
        stage.ensureCapacity(storeSize * 2);

        // Determine the log record address, which is its position in the file
        // mod the file size.
        // If the record will fit at the end of the file, then its address is
        // the current end-of-file.
        // However, if the log record would wrap around the end of the file,
        // then its address is the
        // current end-of-file plus the remaining space at the end of the file
        // that must be skipped.

        long remaining = dbf.capacity() - (endOfLog % dbf.capacity());

        // Wrap around the log if necessary
        if (storeSize > remaining) {

            // Make sure BEFORE we rewind the log that we will have enough space
            // for the
            // log record after skipping the remaining space at the end of the
            // log
            freeSpace -= remaining;
            if (freeSpace < storeSize)
                throw new LogExhaustedException("out of log space");

            stage.wrap(remaining);

            endOfLog += remaining;
        }

        // Now write the log record
        stage.buffer.put(Log.OK);
        stage.buffer.putShort((short) (storeSize - OVERHEAD));
        rec.writeTo(stage.buffer);

        // Set the log record's address to the current endOfLog
        rec.setAddress(endOfLog);

        // Advance the end-of-log by the amount of log data written, excluding
        // the EOL marker
        endOfLog += storeSize - 1;
    }

    public synchronized void read(RedoLog rec) throws LogNotFoundException {
        // Make sure the log entry still exists
        if (endOfLog > 0 && rec.getAddress() < endOfLog - dbf.capacity())
            throw new LogNotFoundException("log record not found");

        if (!readNext(rec)) {
            // Edge case: indicator of first record may be set to EOL after a
            // wrap
            if (rec.getAddress() != endOfLog - dbf.capacity())
                throw new LogNotFoundException(
                        "log record not found, perhaps due to incomplete write");
        }
    }

    public synchronized boolean readNext(RedoLog rec) {
        // Position to log record address
        long start = rec.getAddress() % dbf.capacity();

        // Read the indicator byte
        byte indicator = dbf.read(readStage, start, 1).get();

        // Rewind if we have an explicit WRAP indicator
        switch (indicator) {
            case Log.WRAP:
                rec.setAddress(rec.getAddress() + (dbf.capacity() - start));
                return readNext(rec);
            case Log.OK:
                break;
            case Log.EOL:
            case Log.BAD:
            default:
                return false;
        }

        // Read the record size
        int size = dbf.read(readStage, start + 1, 2).getShort();

        ensureReadCapacity(size);

        // Read the log record
        rec.readFrom(dbf.read(readStage, start + 3, size));

        return true;
    }

    /**
     * Flush all log records to the file system upto and including the record
     * identified by the given log-sequence-number.
     */
    public synchronized void flushTo(long lsn) {
        if (stage != null)
            stage.flushTo(lsn, false);
    }

    /**
     * Force logs from OS buffers to disk.
     */
    public void force() {
        dbf.force();
    }

    /**
     * Flush the logs upto and excluding the specified address, then move the
     * doNotOverwrite mark to the new firewall point.
     */
    public synchronized void moveFirewallTo(long lsn) {
        if (stage != null)
            stage.flushTo(lsn, true);
        setDoNotOverwrite(lsn);
    }

    // Iterate over the log entries from the given starting point
    public Iterator<RedoLog> iterator(RedoLog logBuffer) {
        return new LogIterator(logBuffer);
    }

    private void ensureReadCapacity(int size) {
        if (readStage == null || readStage.capacity() < size)
            readStage = ByteBuffer.allocate(size);
    }

    /**
     * Manage the staging area to the file.
     */
    class Stage {
        ByteBuffer buffer = ByteBuffer.allocate((int) (dbf.capacity() / 8));
        long baseLSN = endOfLog;
        int eofMark; // this maps to the end-of-file + 1, or the start of

        void ensureCapacity(int size) {
            if (buffer.remaining() < size) {
                ByteBuffer bb = ByteBuffer
                        .allocate((int) ((((int) Math.floor((buffer.position() + size) / (dbf.capacity() * 1.0 / 8))) + 1) * (dbf.capacity() / 8)));
                buffer.flip();
                bb.put(buffer);
                buffer = bb;
            }
        }

        void wrap(long remaining) {
            eofMark = (int) (endOfLog - baseLSN + remaining);

            // Write a WRAP indicator so scanning will work to wrap around the
            // end of the file
            buffer.put(Log.WRAP);

            // Set the stage buffer position to map to the physical end-of-file
            // + 1, or start of file
            buffer.position(eofMark);
        }

        void flushTo(long lsn, boolean exclusive) {
            int commitMark = (int) (lsn - baseLSN);
            if (commitMark < 0 || (commitMark == 0 && exclusive))
                return;

            // flip the buffer to prepare for writing
            buffer.flip();

            // save the limit for later compact
            int limit = buffer.limit();

            // Read the log at address lsn to determine the limit of what to
            // flush, unless exclusive
            if (!exclusive) {
                buffer.position(commitMark + 1);
                commitMark += 3 + buffer.getShort();
            }

            // If this write is going to wrap around the end of the file, then
            // write the second
            // half first so that we do not end up with the first half without
            // the second half if there is a failure in between the two writes
            long position = baseLSN % dbf.capacity();

            if (eofMark > 0 && commitMark > eofMark) {
                // write the beginning of the file, which is actually the end of
                // the wrapped bytes
                buffer.position(eofMark);

                // add one for the eol marker (see below); note, it is assumed
                // that enough space
                // exists in the buffer for this
                buffer.limit(commitMark + 1);

                // avoid a separate write for the eol marker by overwriting
                // the byte position in the buffer corresponding to that eol
                // position
                // then replacing it after the file write
                byte saved = buffer.get(commitMark);
                buffer.put(commitMark, Log.EOL);
                dbf.write(buffer, 0);
                buffer.put(commitMark, saved);

                // now write the end of the file, which is actually the start of
                // the wrapped bytes
                buffer.limit(eofMark);
                buffer.position(0);
                dbf.write(buffer, position);
            } else {
                // write the buffer upto the commitMark to the position in the
                // file
                buffer.position(0);
                buffer.limit(commitMark + 1);
                byte saved = buffer.get(commitMark);
                buffer.put(commitMark, Log.EOL);
                dbf.write(buffer, position);
                buffer.put(commitMark, saved);
            }

            // Compact the staging buffer
            buffer.limit(limit);
            buffer.position(commitMark);
            buffer.compact();

            baseLSN += commitMark;
            eofMark = Math.max(eofMark - commitMark, 0);
        }
        // file
    }

    private class LogIterator implements Iterator<RedoLog> {
        private RedoLog rec;
        private RedoLog next;

        LogIterator(RedoLog rec) {
            this.rec = rec;
            try {
                read(rec);
                next = rec;
            } catch (LogNotFoundException e) {
                this.rec = null;
            }
        }

        public boolean hasNext() {
            if (next != null)
                return true;
            next = getNext();
            return next != null;
        }

        public RedoLog next() {
            if (next == null && !hasNext())
                throw new NoSuchElementException();
            RedoLog s = next;
            next = null;
            return s;
        }

        public void remove() {
            throw new UnsupportedOperationException();
        }

        private RedoLog getNext() {
            if (rec != null) {
                // Prepare to read the next sequential log entry
                rec.setAddress(rec.getAddress() + rec.storeSize() + OVERHEAD
                                       - 1);
                if (!readNext(rec))
                    rec = null;
            }
            return rec;
        }
    }
}
