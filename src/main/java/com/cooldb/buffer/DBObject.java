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

@SuppressWarnings("rawtypes")
public interface DBObject extends Comparable {
    /**
     * Writes this object to the <code>ByteBuffer</code> starting at the
     * buffer's current position. The exact number of bytes to be written can be
     * determined in advance by invoking the method {@link #storeSize}.
     *
     * @param bb the <code>ByteBuffer</code> to which the state of this
     *           object will be written
     */
    void writeTo(ByteBuffer bb);

    /**
     * Reads this object from the <code>ByteBuffer</code> starting at the
     * buffer's current position.
     *
     * @param bb the <code>ByteBuffer</code> from which the state of this
     *           object will be read
     */
    void readFrom(ByteBuffer bb);

    /**
     * Determines the exact number of bytes that this object will consume when
     * written to a <code>ByteBuffer</code>.
     *
     * @return the exact number of bytes that the state of this object will
     * occupy when stored in a <code>ByteBuffer</code>
     */
    int storeSize();

    /**
     * Creates a deep copy of this object.
     *
     * @return an exact copy of this object and all it contains
     */
    DBObject copy();

    /**
     * Copies the state of the specified object into this one. The copied object
     * must be of identical type to this one. Assign behaves in the same way as
     * <code>copy</code> but without needing to allocate a new object.
     *
     * @param o an object of this object's type whose state will be copied
     *          into this object
     */
    void assign(DBObject o);
}
