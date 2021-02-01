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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

/*
 * DBFile wraps a FileChannel adding page-level fetch/flush operations.
 * This class is thread-safe and relies on the concurrency provided by
 * the underlying FileChannel implementation.
 */

public class DBFile {
    private final ByteBuffer zeroBuffer;
    private final int pageSize;
    private RandomAccessFile raf;
    private FileChannel channel;
    private FileLock fileLock;

    public DBFile(RandomAccessFile raf) {
        this(raf, FileManager.DEFAULT_PAGE_SIZE);
    }

    public DBFile(RandomAccessFile raf, int pageSize) {
        this.raf = raf;
        this.channel = raf.getChannel();
        this.pageSize = pageSize;
        zeroBuffer = ByteBuffer.allocateDirect(pageSize);
        zeroBuffer.put(new byte[pageSize]);
    }

    public void close() {
        try {
            if (raf != null) {
                // channel.force(false);
                if (fileLock != null)
                    fileLock.release();
                channel.close();
                raf.close();
                raf = null;
                channel = null;
                fileLock = null;
            }
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public boolean tryLock() {
        try {
            if (fileLock == null && channel != null)
                fileLock = channel.tryLock();
            return fileLock != null;
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public int getPageSize() {
        return pageSize;
    }

    public void fetch(int pid, ByteBuffer buffer) {
        buffer.position(0).limit(pageSize);
        try {
            int n;
            long position = (long) pid * (long) pageSize;

            if ((n = channel.read(buffer, position)) != pageSize)
                throw new IOError("Partial read of " + n + " bytes");
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public void flush(int pid, ByteBuffer buffer, boolean force) {
        buffer.position(0).limit(pageSize);
        try {
            int n;
            long position = (long) pid * (long) pageSize;

            if ((n = channel.write(buffer, position)) != pageSize)
                throw new IOError("Partial write of " + n + " bytes");

            // Force file sync if requested
            if (force)
                channel.force(false);
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public void write(ByteBuffer bb, long position) {
        try {
            channel.write(bb, position);
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public ByteBuffer read(ByteBuffer bb, long position, int len) {
        try {
            bb.limit(len);
            bb.rewind();
            channel.read(bb, position);
            bb.rewind();
            return bb;
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public void force() {
        try {
            channel.force(false);
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public void extend(int pages) {
        // extend the size of the file by n-pages
        int start = size();
        int end = start + pages;
        for (int i = start; i < end; i++)
            flush(i, zeroBuffer, false);
        // force();
    }

    public void truncate(long size) {
        try {
            channel.truncate(size);
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public int size() {
        try {
            return (int) (channel.size() / pageSize);
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public long capacity() {
        try {
            return channel.size();
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    public MappedByteBuffer map() {
        try {
            return channel.map(FileChannel.MapMode.READ_WRITE, 0, channel
                    .size());
        } catch (IOException e) {
            throw new IOError(e.toString());
        }
    }

    // This is very serious
    static class IOError extends Error {
        IOError(String msg) {
            super(msg);
        }
    }
}
