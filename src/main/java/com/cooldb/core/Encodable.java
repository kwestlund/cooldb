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

/**
 * Encodable allows objects to be encoded and decoded to/from a specific position in a byte array.
 */
public interface Encodable {
    /**
     * Encodes the column using an order-preserving, lossless, normalized form.
     * Returns the offset of the place in the buffer one byte following the last
     * byte encoded here.
     */
    int encode(byte[] buffer, int offset);

    /**
     * Decodes the column from its normalized form.
     */
    int decode(byte[] buffer, int offset);

    /**
     * Returns the size required to store the encoded form of this column.
     */
    int encodingSize();
}
