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
 * PageBuffer wraps a ByteBuffer and associates is with a specific, pinned FilePage.
 *<p>
 * If an attempt to modify the PageBuffer via one of the 'put' methods is made
 * when the PageBuffer is pinned in read-only mode, a ReadOnlyBufferException occurs.
 */

public class PageBuffer {

    private BufferPoolImpl.Slot slot;
    private ByteBuffer bb;
    private boolean isDirty;

    // Package private constructor and methods
    PageBuffer(BufferPoolImpl.Slot slot, ByteBuffer bb) {
        this.slot = slot;
        this.bb = bb;
        isDirty = slot.isDirty();
    }

    // Support for DBObject types
    public PageBuffer put(DBObject obj) {
        validate();
        obj.writeTo(bb);
        isDirty = true;
        return this;
    }

    // Wraps ByteBuffer

    // Puts

    public PageBuffer put(int index, DBObject obj) {
        validate();
        bb.position(index);
        obj.writeTo(bb);
        isDirty = true;
        return this;
    }

    public void get(DBObject obj) {
        validate();
        obj.readFrom(bb);
    }

    public void get(int index, DBObject obj) {
        validate();
        bb.position(index);
        obj.readFrom(bb);
    }

    public void memmove(int to, int from, int len) {
        if (from == to)
            return;

        validate();
        if (bb.hasArray()) {
            byte[] bytes = bb.array();
            System.arraycopy(bytes, from, bytes, to, len);
        } else {
            bb.clear();
            ByteBuffer src = bb.slice();
            src.position(from);
            src.limit(from + len);
            position(to);
            put(src);
        }
        isDirty = true;
    }

    public PageBuffer put(byte b) {
        validate();
        bb.put(b);
        isDirty = true;
        return this;
    }

    public PageBuffer put(int index, byte b) {
        validate();
        bb.put(index, b);
        isDirty = true;
        return this;
    }

    public PageBuffer put(byte[] src, int offset, int length) {
        validate();
        bb.put(src, offset, length);
        isDirty = true;
        return this;
    }

    public PageBuffer put(int index, byte[] src, int offset, int length) {
        validate();
        bb.position(index);
        bb.put(src, offset, length);
        isDirty = true;
        return this;
    }

    public PageBuffer put(ByteBuffer src) {
        validate();
        bb.put(src);
        isDirty = true;
        return this;
    }

    public PageBuffer put(int index, ByteBuffer src) {
        validate();
        bb.position(index);
        bb.put(src);
        isDirty = true;
        return this;
    }

    public PageBuffer put(int index, ByteBuffer src, int offset, int length) {
        validate();
        bb.position(index);
        src.position(offset);
        src.limit(offset + length);
        bb.put(src);
        src.clear();
        isDirty = true;
        return this;
    }

    public PageBuffer put(PageBuffer src) {
        return put(src.getByteBuffer());
    }

    public PageBuffer put(int index, PageBuffer src) {
        return put(index, src.getByteBuffer());
    }

    public PageBuffer put(int index, PageBuffer src, int offset, int length) {
        return put(index, src.getByteBuffer(), offset, length);
    }

    public PageBuffer putChar(char value) {
        validate();
        bb.putChar(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putChar(int index, char value) {
        validate();
        bb.position(index);
        bb.putChar(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putDouble(double value) {
        validate();
        bb.putDouble(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putDouble(int index, double value) {
        validate();
        bb.position(index);
        bb.putDouble(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putFloat(float value) {
        validate();
        bb.putFloat(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putFloat(int index, float value) {
        validate();
        bb.position(index);
        bb.putFloat(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putInt(int index, int value) {
        validate();
        bb.position(index);
        bb.putInt(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putInt(int value) {
        validate();
        bb.putInt(value);
        isDirty = true;
        return this;
    }

    // Gets

    public PageBuffer putLong(int index, long value) {
        validate();
        bb.position(index);
        bb.putLong(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putLong(long value) {
        validate();
        bb.putLong(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putShort(int index, short value) {
        validate();
        bb.position(index);
        bb.putShort(value);
        isDirty = true;
        return this;
    }

    public PageBuffer putShort(short value) {
        validate();
        bb.putShort(value);
        isDirty = true;
        return this;
    }

    public byte get() {
        validate();
        return bb.get();
    }

    public PageBuffer get(byte[] dst, int offset, int length) {
        validate();
        bb.get(dst, offset, length);
        return this;
    }

    public PageBuffer get(int index, byte[] dst, int offset, int length) {
        validate();
        bb.position(index);
        bb.get(dst, offset, length);
        return this;
    }

    public PageBuffer get(byte[] dst) {
        validate();
        bb.get(dst);
        return this;
    }

    public byte get(int index) {
        validate();
        return bb.get(index);
    }

    public char getChar() {
        validate();
        return bb.getChar();
    }

    public char getChar(int index) {
        validate();
        return bb.getChar(index);
    }

    public double getDouble() {
        validate();
        return bb.getDouble();
    }

    public double getDouble(int index) {
        validate();
        return bb.getDouble(index);
    }

    public float getFloat() {
        validate();
        return bb.getFloat();
    }

    public float getFloat(int index) {
        validate();
        return bb.getFloat(index);
    }

    public int getInt() {
        validate();
        return bb.getInt();
    }

    public int getInt(int index) {
        validate();
        return bb.getInt(index);
    }

    // Wraps java.nio.Buffer

    public long getLong() {
        validate();
        return bb.getLong();
    }

    public long getLong(int index) {
        validate();
        return bb.getLong(index);
    }

    public short getShort() {
        validate();
        return bb.getShort();
    }

    public short getShort(int index) {
        validate();
        return bb.getShort(index);
    }

    public int capacity() {
        validate();
        return bb.capacity();
    }

    public int position() {
        validate();
        return bb.position();
    }

    public PageBuffer position(int newPosition) {
        validate();
        bb.position(newPosition);
        return this;
    }

    public int limit() {
        validate();
        return bb.limit();
    }

    public PageBuffer limit(int newLimit) {
        validate();
        bb.limit(newLimit);
        return this;
    }

    public PageBuffer mark() {
        validate();
        bb.mark();
        return this;
    }

    public PageBuffer reset() {
        validate();
        bb.reset();
        return this;
    }

    public PageBuffer clear() {
        validate();
        bb.clear();
        return this;
    }

    public PageBuffer flip() {
        validate();
        bb.flip();
        return this;
    }

    public PageBuffer rewind() {
        validate();
        bb.rewind();
        return this;
    }

    public int remaining() {
        validate();
        return bb.remaining();
    }

    public boolean hasRemaining() {
        validate();
        return bb.hasRemaining();
    }

    public boolean isReadOnly() {
        validate();
        return bb.isReadOnly();
    }

    /**
     * Returns the FilePage identifier of the page currently pinned in this
     * PageBuffer.
     */
    public FilePage getPage() {
        validate();
        return slot.getPage();
    }

    /**
     * Flush the page to the file system while it is pinned.
     */
    public void flush() {
        validate();
        if (isDirty) {
            slot.flush();
            isDirty = false;
        }
    }

    BufferPoolImpl.Slot getSlot() {
        validate();
        return slot;
    }

    ByteBuffer getByteBuffer() {
        validate();
        return bb;
    }

    void invalidate() {
        slot = null;
        bb = null;
    }

    private void validate() {
        if (bb == null)
            throw new RuntimeException(
                    "Attempted use of PageBuffer after it was unPinned.");
    }
}
