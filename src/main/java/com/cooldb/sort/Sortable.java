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

package com.cooldb.sort;

/**
 * Sortable describes the methods that an object must support to be sortable by
 * this package.
 */
public interface Sortable {
    /**
     * Writes the normalized data, key first, for this object to the byte array
     * starting at the specified offset. The exact number of bytes to be written
     * can be determined in advance by invoking the method sortableSize.
     *
     * @param buffer the byte array to which the state of this object will be
     *               written
     * @param offset the zero-based starting point of the location for the object
     *               in the array
     * @return the starting offset plus the number of bytes written
     */
    int writeSortable(byte[] buffer, int offset);

    /**
     * Reads the normalized data for this object, key first, from the byte array
     * starting at the specified offset.
     *
     * @param buffer the byte array from which the state of this object will be
     *               read
     * @param offset the zero-based starting point of the location of the object in
     *               the array
     * @return the starting offset plus the number of bytes read
     */
    int readSortable(byte[] buffer, int offset);

    /**
     * Determines the exact number of bytes that the key and non-key portions of
     * this object will consume when written to a byte array
     *
     * @return the exact number of bytes that the non-key portion of this object
     * will occupy when stored in a byte array
     */
    int sortableSize();

    /**
     * Determines the exact number of bytes that the key prefix will consume
     * when written to a byte array
     *
     * @return the exact number of bytes that the key prefix will occupy when
     * stored in a byte array
     */
    int keySize();

    /**
     * Creates a deep copy of this object.
     *
     * @return an exact copy of this object and all it contains
     */
    Object copy();
}
