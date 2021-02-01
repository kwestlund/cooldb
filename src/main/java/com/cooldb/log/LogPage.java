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


import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Printable;

import java.io.PrintStream;

/**
 * LogPage manages the space within a PageBuffer so that all pages under its
 * care are guaranteed to contain the log-sequence-number of the most recent
 * redo update to the page.
 */

public abstract class LogPage implements Printable {

    private static final int PAGE_LSN = 0;
    private static final int PAGE_UNDO_NXT_LSN = PAGE_LSN + 8;
    private static final int PAGE_FIRST_LSN = PAGE_UNDO_NXT_LSN
            + UndoPointer.sizeOf();
    public static final int BASE = PAGE_FIRST_LSN + 8;
    protected PageBuffer pageBuffer;

    public LogPage(PageBuffer pageBuffer) {
        this.pageBuffer = pageBuffer;
    }

    protected LogPage() {
    }

    /**
     * Sets the log-sequence-number of the last update to this page.
     */
    public static void setPageLSN(PageBuffer pageBuffer, long pageLSN) {
        pageBuffer.putLong(PAGE_LSN, pageLSN);
    }

    /**
     * Returns the log-sequence-number of the last update to this page.
     */
    public static long getPageLSN(PageBuffer pageBuffer) {
        return pageBuffer.getLong(PAGE_LSN);
    }

    /**
     * Sets the log-sequence-number of the last update to this page recorded in
     * the undo log.
     */
    public static void setPageUndoNxtLSN(PageBuffer pageBuffer,
                                         UndoPointer pageUndoNxtLSN) {
        pageBuffer.put(PAGE_UNDO_NXT_LSN, pageUndoNxtLSN);
    }

    /**
     * Returns the log-sequence-number of the last update to this page recorded
     * in the undo log.
     */
    public static void getPageUndoNxtLSN(PageBuffer pageBuffer,
                                         UndoPointer pageUndoNxtLSN) {
        pageBuffer.get(PAGE_UNDO_NXT_LSN, pageUndoNxtLSN);
    }

    /**
     * Sets the log-sequence-number of the first update to this page recorded in
     * the undo log.
     */
    public static void setPageFirstLSN(PageBuffer pageBuffer, long pageFirstLSN) {
        pageBuffer.putLong(PAGE_FIRST_LSN, pageFirstLSN);
    }

    /**
     * Returns the log-sequence-number of the first update to this page recorded
     * in the undo log.
     */
    public static long getPageFirstLSN(PageBuffer pageBuffer) {
        return pageBuffer.getLong(PAGE_FIRST_LSN);
    }

    /**
     * Set the underlying PageBuffer.
     */
    public void setPageBuffer(PageBuffer pageBuffer) {
        this.pageBuffer = pageBuffer;
    }

    /**
     * Returns the log-sequence-number of the last update to this page recorded
     * in the redo log.
     */
    public long getPageLSN() {
        return getPageLSN(pageBuffer);
    }

    /**
     * Sets the log-sequence-number of the last update to this page recorded in
     * the redo log.
     */
    public void setPageLSN(long pageLSN) {
        setPageLSN(pageBuffer, pageLSN);
    }

    /**
     * Sets the log-sequence-number of the last update to this page recorded in
     * the undo log.
     */
    public void setPageUndoNxtLSN(UndoPointer pageUndoNxtLSN) {
        setPageUndoNxtLSN(pageBuffer, pageUndoNxtLSN);
    }

    /**
     * Returns the log-sequence-number of the last update to this page recorded
     * in the undo log.
     */
    public void getPageUndoNxtLSN(UndoPointer pageUndoNxtLSN) {
        getPageUndoNxtLSN(pageBuffer, pageUndoNxtLSN);
    }

    /**
     * Returns the log-sequence-number of the first update to this page recorded
     * in the undo log.
     */
    public long getPageFirstLSN() {
        return getPageFirstLSN(pageBuffer);
    }

    /**
     * Sets the log-sequence-number of the first update to this page recorded in
     * the undo log.
     */
    public void setPageFirstLSN(long pageFirstLSN) {
        setPageFirstLSN(pageBuffer, pageFirstLSN);
    }

    public int getBase() {
        return LogPage.BASE;
    }

    public void print(PrintStream out, String linePrefix) {
        out.print(linePrefix + getClass().getSimpleName() + "(");
        out.print("PAGE_LSN:" + getPageLSN() + "/");
        UndoPointer pageUndoNxtLSN = new UndoPointer();
        getPageUndoNxtLSN(pageUndoNxtLSN);
        out.print("PAGE_UNDO_NXT_LSN:" + pageUndoNxtLSN + "/");
        out.print("PAGE_FIRST_LSN:" + getPageFirstLSN());
        out.println(")");
    }
}
