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
 * ControlPage manages the space within a PageBuffer in support of the
 * ExtendableLogWriter. The ControlPage stores attributes required by
 * ExtendableLogWriter to persist its operations, such as log space allocation.
 */

public class ControlPage {
    // first and oldest active log record
    private static final int MIN_UNDO = 0;
    // id of the first live log record written (startOfLog)
    private static final int HEAD = MIN_UNDO + UndoPointer.sizeOf();
    // id of next log record to be stored (endOfLog)
    private static final int TAIL = HEAD + FilePage.sizeOf();
    // start of the chain of garbage-collected extents
    private static final int FREE = TAIL + UndoPointer.sizeOf();
    // size in pages of each extent
    private static final int EXTENT_SIZE = FREE + FilePage.sizeOf();
    // total number of extents allocated
    private static final int EXTENTS = EXTENT_SIZE + 2;
    // file extension undo info
    private static final int UNDO_FREE = EXTENTS + 4;
    private static final int UNDO_PAGE = UNDO_FREE + FilePage.sizeOf();
    // file garbage collection redo info
    private static final int REDO_GC_PAGE = UNDO_PAGE + FilePage.sizeOf();
    private static final int REDO_GC_NEXT = REDO_GC_PAGE + FilePage.sizeOf();
    // unused space on the page, available for future use
    @SuppressWarnings("unused")
    private static final int UNUSED = REDO_GC_NEXT + FilePage.sizeOf();
    final PageBuffer pageBuffer;

    public ControlPage(PageBuffer pageBuffer) {
        this.pageBuffer = pageBuffer;
    }

    /**
     * Writes the minUndo identifier.
     */
    public static void setMinUndo(PageBuffer pageBuffer, UndoPointer minUndo) {
        pageBuffer.put(MIN_UNDO, minUndo);
    }

    /**
     * Reads the minUndo identifier.
     */
    public static void getMinUndo(PageBuffer pageBuffer, UndoPointer minUndo) {
        pageBuffer.get(MIN_UNDO, minUndo);
    }

    /**
     * Writes the head identifier.
     */
    public static void setHead(PageBuffer pageBuffer, FilePage head) {
        pageBuffer.put(HEAD, head);
    }

    /**
     * Reads the head identifier.
     */
    public static void getHead(PageBuffer pageBuffer, FilePage head) {
        pageBuffer.get(HEAD, head);
    }

    /**
     * Writes the tail identifier. This is the end-of-log.
     */
    public static void setTail(PageBuffer pageBuffer, UndoPointer tail) {
        pageBuffer.put(TAIL, tail);
    }

    /**
     * Reads the tail identifier.
     */
    public static void getTail(PageBuffer pageBuffer, UndoPointer tail) {
        pageBuffer.get(TAIL, tail);
    }

    /**
     * Writes the free identifier.
     */
    public static void setFree(PageBuffer pageBuffer, FilePage free) {
        pageBuffer.put(FREE, free);
    }

    /**
     * Reads the free identifier.
     */
    public static void getFree(PageBuffer pageBuffer, FilePage free) {
        pageBuffer.get(FREE, free);
    }

    // static versions of the above

    /**
     * Sets the fixed extent size.
     */
    public static void setExtentSize(PageBuffer pageBuffer, short extentSize) {
        pageBuffer.putShort(EXTENT_SIZE, extentSize);
    }

    /**
     * Gets the fixed extent size.
     */
    public static short getExtentSize(PageBuffer pageBuffer) {
        return pageBuffer.getShort(EXTENT_SIZE);
    }

    /**
     * Sets the total number of allocated extents
     */
    public static void setExtents(PageBuffer pageBuffer, int extents) {
        pageBuffer.putInt(EXTENTS, extents);
    }

    /**
     * Gets the total number of allocated extents
     */
    public static int getExtents(PageBuffer pageBuffer) {
        return pageBuffer.getInt(EXTENTS);
    }

    /**
     * Writes the undoFree identifier into the pageBuffer.
     */
    public static void setUndoFree(PageBuffer pageBuffer, FilePage undoFree) {
        pageBuffer.put(UNDO_FREE, undoFree);
    }

    /**
     * Reads the undoFree identifier from the pageBuffer.
     */
    public static void getUndoFree(PageBuffer pageBuffer, FilePage undoFree) {
        pageBuffer.get(UNDO_FREE, undoFree);
    }

    /**
     * Writes the undoPage identifier into the pageBuffer.
     */
    public static void setUndoPage(PageBuffer pageBuffer, FilePage undoPage) {
        pageBuffer.put(UNDO_PAGE, undoPage);
    }

    /**
     * Reads the undoPage identifier from the pageBuffer.
     */
    public static void getUndoPage(PageBuffer pageBuffer, FilePage undoPage) {
        pageBuffer.get(UNDO_PAGE, undoPage);
    }

    /**
     * Writes the redoGCPage identifier into the pageBuffer.
     */
    public static void setRedoGCPage(PageBuffer pageBuffer, FilePage redoGCPage) {
        pageBuffer.put(REDO_GC_PAGE, redoGCPage);
    }

    /**
     * Reads the redoGCPage identifier from the pageBuffer.
     */
    public static void getRedoGCPage(PageBuffer pageBuffer, FilePage redoGCPage) {
        pageBuffer.get(REDO_GC_PAGE, redoGCPage);
    }

    /**
     * Writes the redoGCNext identifier into the pageBuffer.
     */
    public static void setRedoGCNext(PageBuffer pageBuffer, FilePage redoGCNext) {
        pageBuffer.put(REDO_GC_NEXT, redoGCNext);
    }

    /**
     * Reads the redoGCNext identifier from the pageBuffer.
     */
    public static void getRedoGCNext(PageBuffer pageBuffer, FilePage redoGCNext) {
        pageBuffer.get(REDO_GC_NEXT, redoGCNext);
    }

    /**
     * Writes the minUndo identifier.
     */
    public void setMinUndo(UndoPointer minUndo) {
        setMinUndo(pageBuffer, minUndo);
    }

    /**
     * Reads the minUndo identifier.
     */
    public void getMinUndo(UndoPointer minUndo) {
        getMinUndo(pageBuffer, minUndo);
    }

    /**
     * Writes the head identifier.
     */
    public void setHead(FilePage head) {
        setHead(pageBuffer, head);
    }

    /**
     * Reads the head identifier.
     */
    public void getHead(FilePage head) {
        getHead(pageBuffer, head);
    }

    /**
     * Writes the tail identifier. This is the end-of-log.
     */
    public void setTail(UndoPointer tail) {
        setTail(pageBuffer, tail);
    }

    /**
     * Reads the tail identifier.
     */
    public void getTail(UndoPointer tail) {
        getTail(pageBuffer, tail);
    }

    /**
     * Writes the free identifier.
     */
    public void setFree(FilePage free) {
        setFree(pageBuffer, free);
    }

    /**
     * Reads the free identifier.
     */
    public void getFree(FilePage free) {
        getFree(pageBuffer, free);
    }

    /**
     * Gets the fixed extent size.
     */
    public short getExtentSize() {
        return getExtentSize(pageBuffer);
    }

    /**
     * Sets the fixed extent size.
     */
    public void setExtentSize(short extentSize) {
        setExtentSize(pageBuffer, extentSize);
    }

    /**
     * Gets the total number of allocated extents
     */
    public int getExtents() {
        return getExtents(pageBuffer);
    }

    /**
     * Sets the total number of allocated extents
     */
    public void setExtents(int extents) {
        setExtents(pageBuffer, extents);
    }

    /**
     * Writes the undoFree page identifier.
     */
    public void setUndoFree(FilePage undoFree) {
        setUndoFree(pageBuffer, undoFree);
    }

    /**
     * Reads the undoFree page identifier.
     */
    public void getUndoFree(FilePage undoFree) {
        getUndoFree(pageBuffer, undoFree);
    }

    /**
     * Writes the undoPage page identifier.
     */
    public void setUndoPage(FilePage undoPage) {
        setUndoPage(pageBuffer, undoPage);
    }

    /**
     * Reads the undoPage page identifier.
     */
    public void getUndoPage(FilePage undoPage) {
        getUndoPage(pageBuffer, undoPage);
    }

    /**
     * Writes the redoGCPage identifier.
     */
    public void setRedoGCPage(FilePage redoGCPage) {
        setRedoGCPage(pageBuffer, redoGCPage);
    }

    /**
     * Reads the redoGCPage identifier.
     */
    public void getRedoGCPage(FilePage redoGCPage) {
        getRedoGCPage(pageBuffer, redoGCPage);
    }

    /**
     * Writes the redoGCNext identifier.
     */
    public void setRedoGCNext(FilePage redoGCNext) {
        setRedoGCNext(pageBuffer, redoGCNext);
    }

    /**
     * Reads the redoGCNext identifier.
     */
    public void getRedoGCNext(FilePage redoGCNext) {
        getRedoGCNext(pageBuffer, redoGCNext);
    }
}
