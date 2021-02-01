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

import com.cooldb.buffer.PageBuffer;

/**
 * RowHeader represents the physical layout of all database records stored in
 * Datasets. Each row has a flag and either a transaction identifier or a rowid
 * linking it to another row piece, followed by the column data.
 * <p>
 * Row Header (10 bytes)
 * <ul>
 * <li>Flags (1 byte): isDeleted, isLinked, isLocked, isReplace
 * <li>Lock holder or rowid link(8 bytes)
 * </ul>
 */

public class RowHeader {
    // Flag types
    private final static byte DELETED = (byte) 1;
    private final static byte LOCKED = (byte) 2;
    private final static byte LINKED = (byte) 4;
    private final static byte REPLACE = (byte) 8;
    // Field Offsets
    private final static int FLAG = 0;
    private final static int LOCK = FLAG + 1;
    private final static int LINK = LOCK;
    private final static int ROWDATA = LOCK + 8;
    private final static int OVERHEAD = ROWDATA;

    /**
     * Create a new row header for a newly inserted row.
     */
    public static void create(PageBuffer pageBuffer, int location) {
        pageBuffer.put(FLAG + location, (byte) 0);
    }

    /**
     * Return true if this row is deleted.
     */
    public static boolean isDeleted(PageBuffer pageBuffer, int location) {
        return (pageBuffer.get(FLAG + location) & DELETED) > 0;
    }

    public static boolean isDeleted(byte flags) {
        return (flags & DELETED) > 0;
    }

    public static void setDeleted(PageBuffer pageBuffer, int location, boolean b) {
        if (b)
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) | DELETED));
        else
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) & ~DELETED));
    }

    /**
     * Return true if this row is locked exclusively. If the result is true,
     * then the lock-holder-index field will point to the transaction entry
     * holding the lock.
     */
    public static boolean isLocked(PageBuffer pageBuffer, int location) {
        return (pageBuffer.get(FLAG + location) & LOCKED) > 0;
    }

    public static boolean isLocked(byte flags) {
        return (flags & LOCKED) > 0;
    }

    public static void setLocked(PageBuffer pageBuffer, int location, boolean b) {
        if (b)
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) | LOCKED));
        else
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) & ~LOCKED));
    }

    /**
     * Return true if this row is linked and continues onto another page.
     */
    public static boolean isLinked(PageBuffer pageBuffer, int location) {
        return (pageBuffer.get(FLAG + location) & LINKED) > 0;
    }

    public static boolean isLinked(byte flags) {
        return (flags & LINKED) > 0;
    }

    public static void setLinked(PageBuffer pageBuffer, int location, boolean b) {
        if (b)
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) | LINKED));
        else
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) & ~LINKED));
    }

    /**
     * Return true if this row replaces another row that is not universally
     * committed.
     */
    public static boolean isReplace(PageBuffer pageBuffer, int location) {
        return (pageBuffer.get(FLAG + location) & REPLACE) > 0;
    }

    public static boolean isReplace(byte flags) {
        return (flags & REPLACE) > 0;
    }

    public static void setReplace(PageBuffer pageBuffer, int location, boolean b) {
        if (b)
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) | REPLACE));
        else
            pageBuffer.put(FLAG + location, (byte) (pageBuffer.get(FLAG
                                                                           + location) & ~REPLACE));
    }

    public static byte setReplace(byte flags, boolean b) {
        if (b)
            return (byte) (flags | REPLACE);
        else
            return (byte) (flags & ~REPLACE);
    }

    /**
     * Get the lock-holder-index field that points to the transaction entry that
     * holds an exclusive lock on this row.
     */
    public static long getLockHolder(PageBuffer pageBuffer, int location) {
        return pageBuffer.getLong(LOCK + location);
    }

    /**
     * Set the lock-holder-index field that points to the transaction entry that
     * holds an exclusive lock on this row.
     */
    public static void setLockHolder(PageBuffer pageBuffer, int location,
                                     long lockHolder) {
        pageBuffer.putLong(LOCK + location, lockHolder);
    }

    /**
     * Get the Rowid that links this row piece with another row piece.
     */
    public static void getLink(PageBuffer pageBuffer, int location, Rowid link) {
        pageBuffer.get(LINK + location, link);
    }

    /**
     * Set the Rowid that links this row piece with another row piece.
     */
    public static void setLink(PageBuffer pageBuffer, int location, Rowid link) {
        pageBuffer.put(LINK + location, link);
    }

    public static int getOverhead() {
        return OVERHEAD;
    }

    public static String describeFlags(byte flags) {
        String s = isDeleted(flags) ? "[DELETED" : null;
        if (isLocked(flags))
            s = s == null ? "[LOCKED" : s + "|LOCKED";
        if (isLinked(flags))
            s = s == null ? "[LINKED" : s + "|LINKED";
        if (isReplace(flags))
            s = s == null ? "[REPLACE" : s + "|REPLACE";
        return s == null ? "[]" : s + "]";
    }
}
