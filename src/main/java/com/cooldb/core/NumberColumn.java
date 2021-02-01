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
import com.cooldb.buffer.DBObject;
import com.cooldb.util.ByteArrayUtils;

import java.nio.ByteBuffer;

class NumberColumn extends AbstractColumn implements DBObject {
    private char type;
    private long li;
    private double fp;

    NumberColumn(char type) {
        super();
        this.type = type;
    }

    NumberColumn(NumberColumn nc) {
        super(nc);
        type = nc.type;
        li = nc.li;
        fp = nc.fp;
    }

    @Override
    public String getString() {
        if (isNull())
            return "0";

        if (type == 'd')
            return Double.toString(fp);
        else
            return Long.toString(li);
    }

    @Override
    public void setString(String string) throws TypeException {
        if (string == null) {
            setNull(true);
            return;
        }
        try {
            switch (type) {
                case 'b':
                    li = Byte.parseByte(string);
                    break;
                case 'i':
                    li = Short.parseShort(string);
                    break;
                case 'I':
                    li = Integer.parseInt(string);
                    break;
                case 'l':
                    li = Long.parseLong(string);
                    break;
                case 'd':
                    fp = Double.parseDouble(string);
                    break;
            }
        } catch (NumberFormatException nfe) {
            throw new TypeException(nfe);
        }
        setNull(false);
    }

    @Override
    public byte getByte() {
        if (isNull())
            return (byte) 0;

        if (type == 'd')
            return (byte) fp;
        else
            return (byte) li;
    }

    @Override
    public void setByte(byte val) {
        if (type == 'd')
            fp = val;
        else
            li = val;
        setNull(false);
    }

    @Override
    public short getShort() {
        if (isNull())
            return (short) 0;

        if (type == 'd')
            return (short) fp;
        else
            return (short) li;
    }

    @Override
    public void setShort(short val) {
        if (type == 'd')
            fp = val;
        else
            li = val;
        setNull(false);
    }

    @Override
    public int getInt() {
        if (isNull())
            return 0;

        if (type == 'd')
            return (int) fp;
        else
            return (int) li;
    }

    @Override
    public void setInt(int val) {
        if (type == 'd')
            fp = val;
        else
            li = val;
        setNull(false);
    }

    @Override
    public long getLong() {
        if (isNull())
            return 0;

        if (type == 'd')
            return (long) fp;
        else
            return li;
    }

    @Override
    public void setLong(long val) {
        if (type == 'd')
            fp = val;
        else
            li = val;
        setNull(false);
    }

    @Override
    public double getDouble() {
        if (isNull())
            return 0.0;

        if (type == 'd')
            return fp;
        else
            return li;
    }

    @Override
    public void setDouble(double val) {
        if (type == 'd')
            fp = val;
        else
            li = (long) val;
        setNull(false);
    }

    @Override
    public byte[] getBytes() throws TypeException {
        if (isNull())
            return new byte[0];

        switch (type) {
            case 'b':
                return ByteArrayUtils.byteToBytes(getByte());
            case 'i':
                return ByteArrayUtils.shortToBytes(getShort());
            case 'I':
                return ByteArrayUtils.intToBytes(getInt());
            case 'l':
                return ByteArrayUtils.longToBytes(getLong());
            case 'd':
                return ByteArrayUtils.doubleToBytes(getDouble());
            default:
                throw new TypeException("Unrecognized type.");
        }
    }

    @Override
    public void setBytes(byte[] val) throws TypeException {
        if (val.length == 0) {
            setNull(true);
            return;
        }
        switch (type) {
            case 'b':
                li = ByteArrayUtils.bytesToByte(val);
                break;
            case 'i':
                if (val.length < 2)
                    throw new TypeException(
                            "Too few bytes to be valid byte array representation for this number.");
                li = ByteArrayUtils.bytesToShort(val);
                break;
            case 'I':
                if (val.length < 4)
                    throw new TypeException(
                            "Too few bytes to be valid byte array representation for this number.");
                li = ByteArrayUtils.bytesToInt(val);
                break;
            case 'l':
                if (val.length < 8)
                    throw new TypeException(
                            "Too few bytes to be valid byte array representation for this number.");
                li = ByteArrayUtils.bytesToLong(val);
                break;
            case 'd':
                if (val.length < 8)
                    throw new TypeException(
                            "Too few bytes to be valid byte array representation for this number.");
                fp = ByteArrayUtils.bytesToDouble(val);
                break;
            default:
                throw new TypeException("Unrecognized type.");
        }

        setNull(false);
    }

    @Override
    public int encode(byte[] dst, int to) {
        int next = to;

        // encode the null indicator
        boolean desc = sortDirection == -1;
        byte b = (byte) (nullIndicator + 1);
        if (desc)
            b = (byte) ~b;
        dst[next++] = b;

        // encode the value
        if (nullIndicator == 0) {
            // invert sign bit
            switch (type) {
                case 'b':
                    b = (byte) li;
                    b ^= 1 << 7;
                    if (desc)
                        b = (byte) ~b;
                    dst[next] = b;
                    next++;
                    break;
                case 'i':
                    short s = (short) li;
                    s ^= 1 << 15;
                    if (desc)
                        s = (short) ~s;
                    ByteArrayUtils.putShort(dst, s, next);
                    next += 2;
                    break;
                case 'I':
                    int i = (int) li;
                    i ^= 1 << 31;
                    if (desc)
                        i = ~i;
                    ByteArrayUtils.putInt(dst, i, next);
                    next += 4;
                    break;
                case 'd':
                    li = Double.doubleToLongBits(fp);
                case 'l':
                    long l = li;
                    l ^= (1L << 63);
                    if (desc)
                        l = ~l;
                    ByteArrayUtils.putLong(dst, l, next);
                    next += 8;
                    break;
            }
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

        // decode the value (invert sign for integers)
        switch (type) {
            case 'b':
                b = src[next++];
                b ^= 1 << 7;
                if (desc)
                    b = (byte) ~b;
                li = b;
                break;
            case 'i':
                short s = ByteArrayUtils.getShort(src, next);
                s ^= 1 << 15;
                if (desc)
                    s = (short) ~s;
                li = s;
                next += 2;
                break;
            case 'I':
                int i = ByteArrayUtils.getInt(src, next);
                i ^= 1 << 31;
                if (desc)
                    i = ~i;
                li = i;
                next += 4;
                break;
            case 'l':
                li = ByteArrayUtils.getLong(src, next);
                li ^= 1L << 63;
                if (desc)
                    li = ~li;
                next += 8;
                break;
            case 'd':
                li = ByteArrayUtils.getLong(src, next);
                li ^= 1L << 63;
                if (desc)
                    li = ~li;
                fp = Double.longBitsToDouble(li);
                next += 8;
                break;
        }

        return next;
    }

    @Override
    public int encodingSize() {
        if (nullIndicator != 0)
            return 1;

        switch (type) {
            case 'b':
                return 2;
            case 'i':
                return 3;
            case 'I':
                return 5;
            default:
                return 9;
        }
    }

    // DBObject implementation
    @Override
    public void assign(DBObject o) {
        super.assign(o);
        NumberColumn nc = (NumberColumn) o;
        type = nc.type;
        li = nc.li;
        fp = nc.fp;
    }

    public DBObject copy() {
        return new NumberColumn(this);
    }

    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);

        if (!isNull()) {
            switch (type) {
                case 'b':
                    bb.put((byte) li);
                    break;
                case 'i':
                    bb.putShort((short) li);
                    break;
                case 'I':
                    bb.putInt((int) li);
                    break;
                case 'l':
                    bb.putLong(li);
                    break;
                case 'd':
                    bb.putDouble(fp);
                    break;
            }
        }
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);

        if (!isNull()) {
            switch (type) {
                case 'b':
                    li = bb.get();
                    break;
                case 'i':
                    li = bb.getShort();
                    break;
                case 'I':
                    li = bb.getInt();
                    break;
                case 'l':
                    li = bb.getLong();
                    break;
                case 'd':
                    fp = bb.getDouble();
                    break;
            }
        }
    }

    @Override
    public int storeSize() {
        int size = super.storeSize();

        if (!isNull()) {
            switch (type) {
                case 'b':
                    size += 1;
                    break;
                case 'i':
                    size += 2;
                    break;
                case 'I':
                    size += 4;
                    break;
                case 'l':
                case 'd':
                    size += 8;
                    break;
            }
        }

        return size;
    }

    @Override
    protected int _compareTo(Object obj) {
        NumberColumn o = (NumberColumn) obj;
        if (type == 'd')
            return Double.compare(fp, o.fp);
        else
            return Long.compare(li, o.li);
    }

    @Override
    public int hashCode() {
        if (isNull())
            return super.hashCode();

        if (type == 'd')
            return (int) fp;
        else
            return (int) li;
    }

    @Override
    protected void _toString(StringBuffer sbuf) {
        if (type == 'd')
            sbuf.append(fp);
        else
            sbuf.append(li);
    }
}
