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
 * LockEntry represents the physical layout of all transaction lock entries
 * stored in dataset pages. Each page has fixed header information followed by a
 * variable length table of these LockEntry's, which are followed by the
 * DirectoryArea.
 */

public class LockEntry {

    // OVERHEAD: 8 for the holder plus 1 for the successor index
    private final static int OVERHEAD = 8 + 1;
    // Field Offsets
    private final static int HOLDER = 0;
    private final static int SUCCESSOR = HOLDER + 8;

    /**
     * Gets the Transaction id of the holder of this LockEntry.
     */
    public static long getHolder(PageBuffer pageBuffer, int offset) {
        return pageBuffer.getLong(HOLDER + offset);
    }

    /**
     * Sets the Transaction id of the holder of this LockEntry.
     */
    public static void setHolder(PageBuffer pageBuffer, int offset, long holder) {
        pageBuffer.putLong(HOLDER + offset, holder);
    }

    /**
     * Gets the LockEntry index of the transaction that replaced this one but
     * that may not yet have committed.
     */
    public static int getSuccessor(PageBuffer pageBuffer, int offset) {
        return pageBuffer.get(SUCCESSOR + offset) & 0xff;
    }

    /**
     * Sets the LockEntry index of the transaction that replaced this one but
     * that may not yet have committed.
     */
    public static void setSuccessor(PageBuffer pageBuffer, int offset,
                                    int successor) {
        pageBuffer.put(SUCCESSOR + offset, (byte) successor);
    }

    /**
     * Fixed store size of each LockEntry.
     */
    public static int sizeOf() {
        return OVERHEAD;
    }
}
