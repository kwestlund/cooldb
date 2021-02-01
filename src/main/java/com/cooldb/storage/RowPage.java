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

package com.cooldb.storage;

import com.cooldb.buffer.FilePage;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.LogPage;

import java.io.PrintStream;

/**
 * RowPage extends LogPage to add support for record management within the page
 * buffer.
 */

public abstract class RowPage extends LogPage {

    // Next page in segment
    private static final int NEXT_PAGE = LogPage.BASE;
    // Previous page in segment
    private static final int PREV_PAGE = NEXT_PAGE + FilePage.sizeOf();
    // Next free page in segment
    private static final int NEXT_FREE_PAGE = PREV_PAGE + FilePage.sizeOf();
    // Count of rows marked for deletion (holes within the row data space),
    private static final int DELETE_COUNT = NEXT_FREE_PAGE + FilePage.sizeOf();
    // End of RowPage data (start of row data)
    static final int BASE = DELETE_COUNT + 2;

    public RowPage(PageBuffer pageBuffer) {
        super(pageBuffer);
    }

    protected RowPage() {
        super();
    }

    /**
     * Reads the identifier of the next page in the segment
     */
    public void getNextPage(FilePage filePage) {
        pageBuffer.get(NEXT_PAGE, filePage);
    }

    /**
     * Writes the identifier of the next page in the segment
     */
    public void setNextPage(FilePage filePage) {
        pageBuffer.put(NEXT_PAGE, filePage);
    }

    /**
     * Reads the identifier of the previous page in the segment
     */
    public void getPrevPage(FilePage filePage) {
        pageBuffer.get(PREV_PAGE, filePage);
    }

    /**
     * Writes the identifier of the previous page in the segment
     */
    public void setPrevPage(FilePage filePage) {
        pageBuffer.put(PREV_PAGE, filePage);
    }

    // Offsets of all fields stored on a RowPage:

    /**
     * Reads the identifier of the next free page in the segment
     */
    public void getNextFreePage(FilePage filePage) {
        pageBuffer.get(NEXT_FREE_PAGE, filePage);
    }

    /**
     * Writes the identifier of the next free page in the segment
     */
    public void setNextFreePage(FilePage filePage) {
        pageBuffer.put(NEXT_FREE_PAGE, filePage);
    }

    /**
     * Returns the count of rows marked for deletion (holes within the row data
     * space),
     */
    public short getDeleteCount() {
        return pageBuffer.getShort(DELETE_COUNT);
    }

    /**
     * Sets the count of rows marked for deletion (holes within the row data
     * space),
     */
    public void setDeleteCount(short deleteCount) {
        pageBuffer.putShort(DELETE_COUNT, deleteCount);
    }

    public void print(PrintStream out, String linePrefix) {
        super.print(out, linePrefix);
        out.print(linePrefix + getClass().getSimpleName() + "(");
        FilePage filePage = new FilePage();
        getNextPage(filePage);
        out.print("NEXT_PAGE:" + filePage + "/");
        getPrevPage(filePage);
        out.print("PREV_PAGE:" + filePage + "/");
        getNextFreePage(filePage);
        out.print("NEXT_FREE_PAGE:" + filePage + "/");
        out.print("DELETE_COUNT:" + getDeleteCount());
        out.println(")");
    }
}
