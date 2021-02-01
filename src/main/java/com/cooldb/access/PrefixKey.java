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

package com.cooldb.access;

import com.cooldb.buffer.DBObject;
import com.cooldb.util.ByteArrayUtils;

import java.nio.ByteBuffer;

/**
 * <code>PrefixKey</code> represents the distinctive component of a search
 * key.
 * <p>
 * Methods are provided that facilitate front and rear key compression.
 */

public class PrefixKey implements DBObject {
    // Entry types
    private static final int MIN = -1;
    private static final int MAX = 1;
    private byte[] bytes;
    private int length;
    private int type;

    public PrefixKey(PrefixKey key) {
        bytes = key.bytes.clone();
        length = key.length;
        type = key.type;
    }

    public void setMinValue() {
        type = MIN;
        length = 0;
    }

    /*
     * public PrefixKey append(Rowid rowid) { ensureCapacity(8); FilePage page =
     * rowid.getPage(); ByteArrayUtils.putUShort(bytes, page.getFileId(),
     * length); length += 2; ByteArrayUtils.putUInt(bytes, page.getPageId(),
     * length); length += 4; ByteArrayUtils.putUShort(bytes, rowid.getIndex(),
     * length); length += 2; return this; }
     */

    public void setMaxValue() {
        type = MAX;
        length = 0;
    }

    public void assign(DBObject o) {
        PrefixKey key = (PrefixKey) o;
        if (key.length > 0) {
            if (bytes == null || key.length > bytes.length)
                bytes = key.bytes.clone();
            else
                System.arraycopy(key.bytes, 0, bytes, 0, key.length);
        }
        length = key.length;
        type = key.type;
    }

    public void reset() {
        length = 0;
        type = 0;
    }

    public PrefixKey append(int value) {
        ensureCapacity(5);
        ByteArrayUtils.putInt(bytes, value, length);
        length += 5;
        return this;
    }

    // DBObject implementation
    public DBObject copy() {
        return new PrefixKey(this);
    }

    public void writeTo(ByteBuffer bb) {
        bb.put((byte) length);
        if (length > 0)
            bb.put(bytes, 0, length);
        else
            bb.put((byte) type);
    }

    public void readFrom(ByteBuffer bb) {
        length = bb.get() & 0xff;
        if (length > 0) {
            if (bytes == null || length > bytes.length)
                bytes = new byte[length];
            bb.get(bytes, 0, length);
            type = 0;
        } else
            type = bb.get();
    }

    public int storeSize() {
        return length > 0 ? length + 1 : 2;
    }

    public int compareTo(Object obj) {
        PrefixKey o = (PrefixKey) obj;

        int diff = type - o.type;
        if (diff != 0)
            return diff;

        int l = Math.min(length, o.length);
        for (int i = 0; i < l; i++) {
            diff = (bytes[i] & 0xff) - (o.bytes[i] & 0xff);
            if (diff != 0)
                return diff;
        }
        return length - o.length;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof PrefixKey && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        int hash = 5381;
        for (int i = 0; i < length; i++)
            hash = ((hash << 5) + hash) + bytes[i];
        return hash;
    }

    int getLength() {
        return length;
    }

    byte[] getBytes() {
        return bytes;
    }

    public void setBytes(byte[] bytes) {
        if (this.bytes == null || bytes.length > this.bytes.length)
            this.bytes = bytes.clone();
        else
            System.arraycopy(bytes, 0, this.bytes, 0, bytes.length);
        length = bytes.length;
        type = 0;
    }

    /**
     * Remove the first <code>prefix</code> bytes from the key.
     */
    void compressFront(int prefix) {
        length -= prefix;
        if (length > 0)
            System.arraycopy(bytes, prefix, bytes, 0, length);
    }

    /**
     * Remove all but the first <code>prefix</code> bytes from the key.
     */
    void compressRear(int prefix) {
        length = prefix;
    }

    /**
     * Return the longest common prefix of this key and an upper key represented
     * as the number of bytes of this key that must appear in all strings for
     * which "this <= s < upper" is true.
     */
    int prefix(PrefixKey upper) {
        if (type != 0 || upper.type != 0)
            return 0;

        int diff;
        int p = 0;
        while (p < length) {
            diff = upper.bytes[p] - bytes[p];
            if (diff == 0)
                ++p;
            else if (diff == 1)
                return p + 1;
            else
                return p;
        }
        return p;
    }

    /**
     * Return the shortest separator between this and the upper key represented
     * as the number of bytes of the <i>upper</i> key that is sufficient and
     * necessary to guarantee that for any string for which "this < s <= sep" is
     * true then "this < s <= upper" must also be true.
     */
    int separator(PrefixKey upper) {
        if (type != 0 || upper.type != 0)
            return 0;

        int diff;
        int p = 0;
        while (p < length) {
            diff = upper.bytes[p] - bytes[p];
            if (diff == 0)
                ++p;
            else
                return p + 1;
        }
        if (p < upper.length)
            return p + 1;

        // allow for the possibility that this == upper
        return p;
    }

    /**
     * Ensure room for l additional bytes.
     */
    private void ensureCapacity(int l) {
        if (bytes == null)
            bytes = new byte[l];
        else if (l + length > bytes.length) {
            byte[] newBytes = new byte[l + length];
            if (length > 0)
                System.arraycopy(bytes, 0, newBytes, 0, length);
            bytes = newBytes;
        }
    }
}
