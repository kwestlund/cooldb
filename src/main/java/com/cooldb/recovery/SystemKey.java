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

package com.cooldb.recovery;

import com.cooldb.buffer.DBFile;
import com.cooldb.log.UndoPointer;

import java.nio.ByteBuffer;

/**
 * The SystemKey records information required to start the database server.
 */

public class SystemKey {
    /**
     * The version of the running database instance.
     */
    public static final int VERSION = 4;
    private final DBFile dbf;
    private ByteBuffer buffer;
    // Stored attributes
    private long master; // Address of last checkpoint log record; recovery
    // start point
    private long nextTransId; // Next transaction identifier at shutdown
    private UndoPointer commitLSN = new UndoPointer(); // Oldest possible undo
    // log record by active
    // transactions
    private int version; // Version of the database
    private int sysFileSize; // Size of the system file in pages
    private String[] segmentMethods = new String[Byte.MAX_VALUE];

    public SystemKey(DBFile dbf) {
        this.dbf = dbf;
        buffer = ByteBuffer.allocateDirect(dbf.getPageSize());
        dbf.fetch(0, buffer);
        buffer.rewind();
        readFrom(buffer);
    }

    public void stop() {
        this.buffer = null;
    }

    /**
     * Initialize the SystemKey at time of database creation.
     */
    public void create(int sysFileSize) {
        this.sysFileSize = sysFileSize;

        master = 0;
        nextTransId = 0;
        commitLSN = new UndoPointer();
        version = VERSION;
        segmentMethods = new String[Byte.MAX_VALUE];

        flush();
    }

    /**
     * Force out the system key data to disk.
     */
    public synchronized void flush() {
        buffer.rewind();
        writeTo(buffer);
        dbf.flush(0, buffer, false);
        // force();
    }

    /**
     * Force logs from OS buffers to disk.
     */
    public void force() {
        dbf.force();
    }

    /**
     * Get the address of the last begin-checkpoint log record written
     */
    public synchronized long getMaster() {
        return master;
    }

    /**
     * Set the address of the last begin-checkpoint log record written
     */
    public synchronized void setMaster(long master) {
        this.master = master;
    }

    /**
     * Get the next transaction identifier at the time of the last checkpoint
     */
    public synchronized long getNextTransId() {
        return nextTransId;
    }

    /**
     * Set the next transaction identifier at the time of the last checkpoint
     */
    public synchronized void setNextTransId(long nextTransId) {
        this.nextTransId = nextTransId;
    }

    /**
     * Get the address of the oldest undo log record written by active
     * transactions at the time of the last checkpoint
     */
    public synchronized UndoPointer getCommitLSN() {
        return commitLSN;
    }

    /**
     * Set the address of the oldest undo log record written by active
     * transactions at the time of the last checkpoint
     */
    public synchronized void setCommitLSN(UndoPointer commitLSN) {
        this.commitLSN = new UndoPointer(commitLSN);
    }

    /**
     * Get the version of the database represented by this system key. This must
     * not differ from the VERSION of the running instance.
     */
    public synchronized int getVersion() {
        return version;
    }

    /**
     * Get the file size of the system file in number of pages.
     */
    public synchronized int getSysFileSize() {
        return sysFileSize;
    }

    /**
     * Set the file size of the system file in number of pages.
     */
    public synchronized void setSysFileSize(int sysFileSize) {
        this.sysFileSize = sysFileSize;
    }

    /**
     * Maintain mappings btwn segment code and class (max = 127)
     */
    public synchronized byte registerSegmentMethod(String className) {
        int i;
        for (i = 0; i < segmentMethods.length; i++) {
            if (segmentMethods[i] != null
                    && segmentMethods[i].equals(className))
                return (byte) i;
        }
        //noinspection StatementWithEmptyBody
        for (i = 0; i < segmentMethods.length && segmentMethods[i] != null; i++)
            ;
        if (i == segmentMethods.length)
            throw new RuntimeException("Too many SegmentMethod types exist!");

        segmentMethods[i] = className;
        flush();

        return (byte) i;
    }

    /**
     * Lookup the class associated with a segment code
     */
    public synchronized String getSegmentMethod(byte segmentType) {
        return segmentMethods[segmentType];
    }

    private void writeTo(ByteBuffer bb) {
        bb.putLong(master);
        bb.putLong(nextTransId);
        commitLSN.writeTo(bb);
        bb.putInt(VERSION);
        bb.putInt(sysFileSize);
        int size = 0;
        for (String segmentMethod : segmentMethods) {
            if (segmentMethod != null)
                ++size;
        }
        bb.put((byte) size);
        for (int i = 0; i < segmentMethods.length; i++) {
            if (segmentMethods[i] != null) {
                byte[] bytes = segmentMethods[i].getBytes();
                bb.put((byte) i);
                bb.putShort((short) bytes.length);
                bb.put(bytes);
            }
        }
    }

    private void readFrom(ByteBuffer bb) {
        master = bb.getLong();
        nextTransId = bb.getLong();
        commitLSN.readFrom(bb);
        version = bb.getInt();
        sysFileSize = bb.getInt();
        int size = bb.get();
        for (int i = 0; i < size; i++) {
            byte segmentType = bb.get();
            short length = bb.getShort();
            byte[] bytes = new byte[length];
            bb.get(bytes);
            segmentMethods[segmentType] = new String(bytes);
        }
    }
}
