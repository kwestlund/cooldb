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

package com.cooldb.util;

/**
 * Convert various integer types to/from a byte array in a format that when
 * compared byte-for-byte preserves the integer type's natural ordering.
 */

public class ByteArrayUtils {
    // byte
    public static byte[] byteToBytes(byte src) {
        return putByte(new byte[1], src, 0);
    }

    public static byte bytesToByte(byte[] src) {
        return getByte(src, 0);
    }

    public static byte[] putByte(byte[] dest, byte src, int offset) {
        dest[offset] = src;
        return dest;
    }

    public static byte getByte(byte[] src, int offset) {
        return src[offset];
    }

    // short
    public static byte[] shortToBytes(short src) {
        return putShort(new byte[2], src, 0);
    }

    public static short bytesToShort(byte[] src) {
        return getShort(src, 0);
    }

    public static byte[] putShort(byte[] dest, short src, int offset) {
        dest[offset] = (byte) (src >> 8);
        dest[offset + 1] = (byte) src;
        return dest;
    }

    public static short getShort(byte[] src, int offset) {
        return (short) (((src[offset] & 0xff) << 8) | ((src[offset + 1] & 0xff)));
    }

    // int
    public static byte[] intToBytes(int src) {
        return putInt(new byte[2], src, 0);
    }

    public static int bytesToInt(byte[] src) {
        return getInt(src, 0);
    }

    public static byte[] putInt(byte[] dest, int src, int offset) {
        dest[offset] = (byte) (src >> 24);
        dest[offset + 1] = (byte) (src >> 16);
        dest[offset + 2] = (byte) (src >> 8);
        dest[offset + 3] = (byte) src;
        return dest;
    }

    public static int getInt(byte[] src, int offset) {
        return ((src[offset] & 0xff) << 24) | ((src[offset + 1] & 0xff) << 16)
                | ((src[offset + 2] & 0xff) << 8) | ((src[offset + 3] & 0xff));
    }

    // long
    public static byte[] longToBytes(long src) {
        return putLong(new byte[2], src, 0);
    }

    public static long bytesToLong(byte[] src) {
        return getLong(src, 0);
    }

    public static byte[] putLong(byte[] dest, long src, int offset) {
        dest[offset] = (byte) (src >> 56);
        dest[offset + 1] = (byte) (src >> 48);
        dest[offset + 2] = (byte) (src >> 40);
        dest[offset + 3] = (byte) (src >> 32);
        dest[offset + 4] = (byte) (src >> 24);
        dest[offset + 5] = (byte) (src >> 16);
        dest[offset + 6] = (byte) (src >> 8);
        dest[offset + 7] = (byte) src;
        return dest;
    }

    public static long getLong(byte[] src, int offset) {
        return (((long) src[offset] & 0xff) << 56)
                | (((long) src[offset + 1] & 0xff) << 48)
                | (((long) src[offset + 2] & 0xff) << 40)
                | (((long) src[offset + 3] & 0xff) << 32)
                | (((long) src[offset + 4] & 0xff) << 24)
                | (((long) src[offset + 5] & 0xff) << 16)
                | (((long) src[offset + 6] & 0xff) << 8)
                | (((long) src[offset + 7] & 0xff));
    }

    // float
    public static byte[] floatToBytes(float src) {
        return putFloat(new byte[2], src, 0);
    }

    public static float bytesToFloat(byte[] src) {
        return getFloat(src, 0);
    }

    public static byte[] putFloat(byte[] dest, float src, int offset) {
        return putInt(dest, Float.floatToIntBits(src), offset);
    }

    public static float getFloat(byte[] src, int offset) {
        return Float.intBitsToFloat(getInt(src, offset));
    }

    // double
    public static byte[] doubleToBytes(double src) {
        return putDouble(new byte[2], src, 0);
    }

    public static double bytesToDouble(byte[] src) {
        return getDouble(src, 0);
    }

    public static byte[] putDouble(byte[] dest, double src, int offset) {
        return putLong(dest, Double.doubleToLongBits(src), offset);
    }

    public static double getDouble(byte[] src, int offset) {
        return Double.longBitsToDouble(getLong(src, offset));
    }
}
