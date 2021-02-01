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
import java.util.Arrays;

public class FileManagerImpl implements FileManager {
    private final int pageSize;
    private DBFile[] files = new DBFile[0];

    public FileManagerImpl() {
        this.pageSize = DEFAULT_PAGE_SIZE;
    }

    public FileManagerImpl(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void addFile(short fileId, DBFile dbf) {
        if (files.length <= fileId) {
            DBFile[] newFiles = new DBFile[fileId + 1];
            System.arraycopy(files, 0, newFiles, 0, files.length);
            files = newFiles;
        }
        files[fileId] = dbf;
    }

    public void fetch(FilePage page, ByteBuffer buffer) {
        files[page.getFileId()].fetch(page.getPageId(), buffer);
    }

    public void flush(FilePage page, ByteBuffer buffer, boolean force) {
        files[page.getFileId()].flush(page.getPageId(), buffer, force);
    }

    public void force(short fileId) {
        files[fileId].force();
    }

    public void extend(short fileId, int pages) {
        files[fileId].extend(pages);
    }

    public void clear() {
        Arrays.fill(files, null);
    }
}
