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

package com.cooldb.buffer;

import java.nio.ByteBuffer;

/*
 * FileManager manages a set of FileChannels, adding page-level fetch/flush operations.
 * The implementation must be thread-safe and rely on the concurrency provided by the
 * underlying FileChannel implementation.
 */

public interface FileManager {
    /*
     * This is the default page size.
     */
    int DEFAULT_PAGE_SIZE = 8192 * 2;

    /*
     * Add a file identified by the given fileId and accessible via the given
     * DBFile to the set of files managed by this FileManager.
     */
    void addFile(short fileId, DBFile dbf);

    /*
     * Read the specified page from disk into the given buffer.
     */
    void fetch(FilePage page, ByteBuffer buffer);

    /*
     * Write the specified page from the given buffer to disk, and if force is
     * true then force a file sync (a sync is very expensive!).
     */
    void flush(FilePage page, ByteBuffer buffer, boolean force);

    /*
     * Force a sync of the specified file.
     */
    void force(short fileId);

    /*
     * Extend the size of the specified file by the given number of pages.
     */
    void extend(short fileId, int pages);

    /*
     * The size of each page in bytes. This is the same for all files and cannot
     * be changed during the life of the database.
     */
    int getPageSize();

    /*
     * Remove all files from the list.
     */
    void clear();
}
