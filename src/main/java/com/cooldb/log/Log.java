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

import com.cooldb.buffer.FilePage;

import java.nio.ByteBuffer;

public abstract class Log {

    // Log Types
    public final static byte UPDATE = 0;
    public final static byte CLR = 1;
    public final static byte BEGIN_CHECKPOINT = 2;
    public final static byte END_CHECKPOINT = 3;
    public final static byte COMMIT = 4;

    // Log integrity codes
    public final static byte BAD = 0; // Bad read
    public final static byte EOL = -1; // Logical end-of-log indicator
    public final static byte WRAP = -2; // Physical end-of-log indicator
    public final static byte OK = -3; // Integrity indicator
    private static final int OVERHEAD = FilePage.sizeOf() * 2
            + UndoPointer.sizeOf() + 15;
    final FilePage page; // Page to which updates were applied
    final UndoPointer undoNxtLSN; // Address of next undo record
    // Stored control parameters
    byte type; // Log record type
    long transID; // Transaction ID
    FilePage segmentId; // Segment identifier
    byte segmentType; // Segment type
    byte pageType; // Page type
    LogData logDataList; // List of log data attachments (entries)
    int logDataSize; // Total store size of all attached logData entries

    public Log() {
        segmentId = new FilePage();
        page = new FilePage();
        undoNxtLSN = new UndoPointer();
    }

    // Copy constructor
    public Log(Log log) {
        this();

        type = log.type;
        transID = log.transID;
        segmentId.assign(log.segmentId);
        page.assign(log.page);
        segmentType = log.segmentType;
        pageType = log.pageType;
        undoNxtLSN.assign(log.undoNxtLSN);

        if (log.logDataList != null)
            logDataList = log.logDataList.copy();

        logDataSize = log.logDataSize;
    }

    public boolean isUpdate() {
        return type == UPDATE;
    }

    public boolean isCLR() {
        return type == CLR;
    }

    public boolean isBeginCheckpoint() {
        return type == BEGIN_CHECKPOINT;
    }

    public boolean isEndCheckpoint() {
        return type == END_CHECKPOINT;
    }

    public boolean isCommit() {
        return type == COMMIT;
    }

    public long getTransID() {
        return transID;
    }

    public void setTransID(long transID) {
        this.transID = transID;
    }

    public FilePage getSegmentId() {
        return segmentId;
    }

    public void setSegmentId(FilePage segmentId) {
        this.segmentId = segmentId;
    }

    public FilePage getPage() {
        return page;
    }

    public void setPage(FilePage page) {
        this.page.assign(page);
    }

    public byte getType() {
        return type;
    }

    public void setType(byte type) {
        this.type = type;
    }

    public byte getSegmentType() {
        return segmentType;
    }

    public void setSegmentType(byte segmentType) {
        this.segmentType = segmentType;
    }

    public byte getPageType() {
        return pageType;
    }

    public void setPageType(byte pageType) {
        this.pageType = pageType;
    }

    public UndoPointer getUndoNxtLSN() {
        return undoNxtLSN;
    }

    public void setUndoNxtLSN(UndoPointer undoNxtLSN) {
        this.undoNxtLSN.assign(undoNxtLSN);
    }

    // LogData
    public LogData allocate(byte flag, int size) {
        LogData entry = new LogData(flag, size);

        // Attach new entry to the list of entries in this log
        attach(entry);

        return entry;
    }

    public void add(LogData entry) {
        attach(entry.copy());
    }

    public LogData getData() {
        return logDataList;
    }

    public void freeData() {
        logDataSize = 0;
        logDataList = null;
    }

    // Storable
    public void writeTo(ByteBuffer bb) {
        bb.put(type);
        bb.putLong(transID);
        segmentId.writeTo(bb);
        page.writeTo(bb);
        bb.put(segmentType);
        bb.put(pageType);
        undoNxtLSN.writeTo(bb);

        int size = logDataList == null ? 0 : logDataList.size();
        bb.putInt(size);

        // Write entries to contiguous data space
        LogData entry = logDataList;
        while (entry != null) {
            entry.writeTo(bb);
            entry = entry.next();
        }
    }

    public void readFrom(ByteBuffer bb) {
        type = bb.get();
        transID = bb.getLong();
        segmentId.readFrom(bb);
        page.readFrom(bb);
        segmentType = bb.get();
        pageType = bb.get();
        undoNxtLSN.readFrom(bb);

        freeData();

        int size = bb.getInt();
        if (size > 0) {
            // Read entries from contiguous data space
            LogData entry;
            for (int i = 0; i < size; i++) {
                entry = new LogData();
                entry.readFrom(bb);
                attach(entry);
            }
        }
    }

    public int storeSize() {
        return OVERHEAD + logDataSize;
    }

    private void attach(LogData entry) {
        if (logDataList == null)
            logDataList = entry;
        else
            logDataList.add(entry);

        // Maintain the total storeSize of all log entries
        logDataSize += entry.storeSize();
    }
}
