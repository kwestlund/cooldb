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

package com.cooldb.api;

import com.cooldb.core.Column;

/**
 * <code>Varchar</code> represents a variable length string of characters or
 * bytes that can be stored in the database and set or retrieved as a column in
 * a <code>Row</code>.
 *
 * @see Table
 * @see Row
 */

public interface Varchar extends Column {
    /**
     * Gets the length of the string value of this column. The returned length
     * is undefined if the column is null.
     *
     * @return the length in number of characters of the variable-length string
     * value of this column
     */
    int getLength();

    /**
     * Gets the maximum possible length of the string value of this column.
     *
     * @return the maximum length in number of characters of the value of this
     * column
     */
    int getMaxLength();

    /**
     * Sets the bytes of this Varchar by copying length bytes from the src array at the given start position.
     *
     * @param src    the source byte array
     * @param start  the starting position within the source byte array
     * @param length the number of bytes to be copied from the source array
     * @throws TypeException if length exceeds the maximum allowed for this type of Varchar
     */
    void setBytes(byte[] src, int start, int length) throws TypeException;
}
