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
import com.cooldb.buffer.PageBuffer;

/**
 * DataPage manages the space within a PageBuffer in support of the
 * ExtendableLogWriter. The DataPage stores the log records as well as
 * attributes needed by the ExtendableLogWriter to manage the entire log space.
 */

public class DataPage {
    // Position of the nextPage attribute on the page
    static final int NEXT_PAGE = 0;
    // LSN of last log written to this page
    static final int LAST_LSN = NEXT_PAGE + FilePage.sizeOf();
    // Start of log data region
    static final int LOG_DATA = LAST_LSN + 8;
    // Amount of space available to storing log data records
    static int DATA_SPACE;
    final PageBuffer pageBuffer;

    public DataPage(PageBuffer pageBuffer) {
        this.pageBuffer = pageBuffer;
        DATA_SPACE = pageBuffer.capacity() - LOG_DATA;
    }

    /**
     * Sets the size of a log record entry.
     */
    public static void putRecordSize(PageBuffer pageBuffer, short offset,
                                     int size) {
        pageBuffer.putInt(offset, size);
    }

    /**
     * Gets the size of a log record entry.
     */
    public static int getRecordSize(PageBuffer pageBuffer, short offset) {
        return pageBuffer.getInt(offset);
    }

    /**
     * Writes the nextPage identifier into the pageBuffer.
     */
    public static void setNextPage(PageBuffer pageBuffer, FilePage nextPage) {
        pageBuffer.put(NEXT_PAGE, nextPage);
    }

    // static versions of the above

    /**
     * Reads the nextPage identifier from the pageBuffer.
     */
    public static void getNextPage(PageBuffer pageBuffer, FilePage nextPage) {
        pageBuffer.get(NEXT_PAGE, nextPage);
    }

    /**
     * Get the LSN of the last log record written to this page.
     */
    public static long getLastLSN(PageBuffer pageBuffer) {
        return pageBuffer.getLong(LAST_LSN);
    }

    /**
     * Set the LSN of the last log record written to this page.
     */
    public static void setLastLSN(PageBuffer pageBuffer, long lsn) {
        pageBuffer.putLong(LAST_LSN, lsn);
    }

    /**
     * Writes 'len' bytes into the log data space at the given offset from the
     * array of bytes starting at position 'start'.
     */
    public static void write(PageBuffer pageBuffer, short offset, byte[] log,
                             int start, int len) {
        pageBuffer.put(offset, log, start, len);
    }

    /**
     * Reads 'len' bytes from the log data space at the given offset into the
     * array of bytes starting at position 'start'.
     */
    public static void read(PageBuffer pageBuffer, short offset, byte[] log,
                            int start, int len) {
        pageBuffer.get(offset, log, start, len);
    }

    /**
     * Writes 'len' bytes into the log data space at the given offset from the
     * array of bytes starting at position 'start'.
     */
    public void write(short offset, byte[] log, int start, int len) {
        write(pageBuffer, offset, log, start, len);
    }

    /**
     * Reads 'len' bytes from the log data space at the given offset into the
     * array of bytes starting at position 'start'.
     */
    public void read(short offset, byte[] log, int start, int len) {
        read(pageBuffer, offset, log, start, len);
    }

    /**
     * Sets the size of a log record entry.
     */
    public void putRecordSize(short offset, int size) {
        putRecordSize(pageBuffer, offset, size);
    }

    /**
     * Gets the size of a log record entry.
     */
    public int getRecordSize(short offset) {
        return getRecordSize(pageBuffer, offset);
    }

    /**
     * Writes the nextPage, which points to the next page in a list of pages.
     * <p>
     * There are two such lists: the list of active pages, and the list of
     * garbage-collected, inactive pages.
     * <p>
     * The nextPage is also used to link extents. <br>
     * In the last page of each list, the nextPage is null.
     */
    public void setNextPage(FilePage nextPage) {
        setNextPage(pageBuffer, nextPage);
    }

    /**
     * Reads the nextPage identifier.
     */
    public void getNextPage(FilePage nextPage) {
        getNextPage(pageBuffer, nextPage);
    }

    /**
     * Get the LSN of the last log record written to this page.
     */
    public long getLastLSN() {
        return getLastLSN(pageBuffer);
    }

    /**
     * Set the LSN of the last log record written to this page.
     */
    public void setLastLSN(long lsn) {
        setLastLSN(pageBuffer, lsn);
    }
}
