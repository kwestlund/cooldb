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

package com.cooldb.core;

import com.cooldb.api.TypeException;
import com.cooldb.api.Varchar;
import com.cooldb.buffer.DBObject;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

public class VarcharColumn extends AbstractColumn implements Varchar {
    /**
     * String Encoding Parameter
     */
    private static final int N = 5;
    private byte[] bytes;
    private int length;
    private int maxLength;

    /**
     * Creates this <code>Varchar</code> column with default maximum character
     * length equal to Byte.MAX_VALUE (255). The value of the column is
     * initially set to null.
     */
    public VarcharColumn() {
        super();
        this.maxLength = Byte.MAX_VALUE;
    }

    /**
     * Creates this <code>Varchar</code> column with the specified maximum
     * character length. The value of the column is initially set to null.
     *
     * @param maxLength the maximum character length of the this Varchar
     */
    public VarcharColumn(int maxLength) {
        super();
        this.maxLength = maxLength;
    }

    /**
     * Creates this <code>Varchar</code> column as a copy of the specified
     * <code>Varchar</code> column and its value.
     *
     * @param vc the column to be copied into this one
     */
    public VarcharColumn(VarcharColumn vc) {
        super(vc);
        if (!vc.isNull())
            bytes = vc.bytes.clone();
        length = vc.length;
        maxLength = vc.maxLength;
    }

    /**
     * Creates this <code>Varchar</code> column with default maximum character
     * length equal to Byte.MAX_VALUE (255) and initial value set to the
     * specified <code>String</code>.
     *
     * @param s the string used to initialize the value of this
     *          <code>Varchar</code> instance.
     * @throws TypeException if the string is too long
     */
    public VarcharColumn(String s) throws TypeException {
        super();
        this.maxLength = Byte.MAX_VALUE;
        setString(s);
    }

    @Override
    public void assign(DBObject o) {
        super.assign(o);
        VarcharColumn vc = (VarcharColumn) o;
        if (!vc.isNull())
            bytes = vc.bytes.clone();
        length = vc.length;
        maxLength = vc.maxLength;
    }

    @Override
    public int getLength() {
        return length;
    }

    @Override
    public int getMaxLength() {
        return maxLength;
    }

    @Override
    public String getString() {
        if (isNull())
            return "";
        return new String(bytes, 0, length, StandardCharsets.UTF_8);
    }

    @Override
    public void setString(String string) throws TypeException {
        if (string == null) {
            setNull(true);
            return;
        }
        byte[] tmp = string.getBytes(StandardCharsets.UTF_8);
        if (tmp.length > maxLength)
            throw new TypeException("String too long.");

        bytes = tmp;
        length = tmp.length;

        setNull(false);
    }

    @Override
    public byte[] getBytes() {
        if (isNull())
            return new byte[0];
        byte[] r = new byte[length];
        System.arraycopy(bytes, 0, r, 0, length);
        return r;
    }

    @Override
    public void setBytes(byte[] tmp) throws TypeException {
        setBytes(tmp, 0, tmp.length);
    }

    /**
     * Gets the internal byte array used as backing store for this column.
     *
     * @return the internal byte array used as backing store for this column.
     */
    public byte[] getPrivateBytes() {
        return bytes;
    }

    @Override
    public void setBytes(byte[] tmp, int start, int length) throws TypeException {
        if (tmp == null) {
            setNull(true);
            return;
        }
        if (length > maxLength)
            throw new TypeException("Byte array too long.");

        this.length = length;

        if (bytes == null || length > bytes.length)
            bytes = new byte[length];

        System.arraycopy(tmp, start, bytes, 0, length);

        setNull(false);
    }

    @Override
    public void setNull(boolean isNull) {
        super.setNull(isNull);
        if (isNull)
            length = 0;
    }

    // DBObject implementation
    @Override
    public DBObject copy() {
        return new VarcharColumn(this);
    }

    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);

        if (!isNull()) {
            if (maxLength > 255)
                bb.putInt(length);
            else
                bb.put((byte) length);
            bb.put(bytes, 0, length);
        }
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);

        if (!isNull()) {
            if (maxLength > 255)
                length = bb.getInt();
            else
                length = bb.get() & 0xff;

            if (bytes == null || length > bytes.length)
                bytes = new byte[length];

            bb.get(bytes, 0, length);
        }
    }

    @Override
    public int storeSize() {
        int size = super.storeSize();
        if (isNull())
            return size;

        return size + length + (maxLength > 255 ? 4 : 1);
    }

    @Override
    protected int _compareTo(Object obj) {
        VarcharColumn o = (VarcharColumn) obj;
        int l = Math.min(length, o.length);
        for (int i = 0; i < l; i++) {
            int diff = bytes[i] - o.bytes[i];
            if (diff != 0)
                return diff;
        }
        return length - o.length;
    }

    @Override
    public int hashCode() {
        if (isNull())
            return super.hashCode();

        int hash = 5381;

        if (!isNull()) {
            for (int i = 0; i < length; i++)
                hash = ((hash << 5) + hash) + bytes[i];
        }

        return hash;
    }

    @Override
    public int encode(byte[] dst, int to) {
        int next = to;

        // encode the null indicator
        dst[next++] = (byte) (nullIndicator + 1);

        if (nullIndicator == 0) {
            // encode the string value
            for (int n = 0; n < length; n++) {
                // insert a continuation control character every N bytes
                if (n > 0 && n % N == 0)
                    dst[next++] = (byte) 0xff;
                dst[next++] = bytes[n];
            }
            // make sure the encoding length is a multiple of N
            int r = length % N;
            if (r > 0 || length == 0) {
                int pad = N - r;
                for (int n = 0; n < pad; n++) {
                    // pad final segment with zeros
                    dst[next++] = (byte) 0;
                }
            } else if (r == 0 && length > 0) {
                r = N;
            }
            // insert the terminating length control character
            dst[next++] = (byte) r;
        }

        // complement the encoding if descending sort direction
        if (sortDirection == -1) {
            for (int n = to; n < next; n++)
                dst[n] = (byte) ~dst[n];
        }

        return next;
    }

    @Override
    public int decode(byte[] src, int from) {
        boolean desc = sortDirection == -1;

        // decode the null indicator
        int next = from;
        byte b = src[next++];
        if (desc)
            b = (byte) ~b;
        nullIndicator = (byte) (b - 1);
        if (nullIndicator != 0)
            return next;

        // determine length
        length = 0;
        int i = next;
        int l;
        do {
            // grab the next control character
            i += N;
            b = src[i++];
            if (desc)
                b = (byte) ~b;
            l = b & 0xff;

            // determine continuation status and length from control character
            length += (l == 0xff ? N : l);
        } while (l == 0xff);

        // allocate space for the string if necessary
        if (bytes == null || length > bytes.length)
            bytes = new byte[length];

        // decode the string
        for (l = 0; l < length; l++) {
            if (l > 0 && l % N == 0)
                ++next;
            bytes[l] = src[next++];
        }

        // skip the padding bytes
        int r = length % N;
        if (r > 0 || length == 0) {
            next += N - r;
        }

        // complement the string if descending sort direction
        if (desc) {
            for (l = 0; l < length; l++)
                bytes[l] = (byte) ~bytes[l];
        }

        return next + 1;
    }

    @Override
    public int encodingSize() {
        if (nullIndicator != 0)
            return 1;

        int size = length;
        int r = length % N;
        if (r > 0 || length == 0)
            size += N - r;
        return size + size / N + 1;
    }

    @Override
    public byte getByte() {
        return 0;
    }

    @Override
    public void setByte(byte val) {
    }

    @Override
    public double getDouble() {
        return 0;
    }

    @Override
    public void setDouble(double val) {
    }

    @Override
    public int getInt() throws TypeException {
        return 0;
    }

    @Override
    public void setInt(int val) throws TypeException {
    }

    @Override
    public long getLong() throws TypeException {
        return 0;
    }

    @Override
    public void setLong(long val) {
    }

    @Override
    public short getShort() throws TypeException {
        return 0;
    }

    @Override
    public void setShort(short val) {
    }

    @Override
    protected void _toString(StringBuffer sbuf) {
        sbuf.append(new String(bytes, 0, length, StandardCharsets.UTF_8));
    }
}
