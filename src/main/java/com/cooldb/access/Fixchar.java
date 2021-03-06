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
import com.cooldb.core.VarcharColumn;

import java.nio.ByteBuffer;

/**
 * <code>Fixchar</code> represents a variable length string of characters or
 * bytes that is padded to its maximum length upon marshalling.
 *
 * @see VarcharColumn
 */

class Fixchar extends VarcharColumn {
    public Fixchar(int maxLength) {
        super(maxLength);
    }

    public Fixchar(Fixchar fc) {
        super(fc);
    }

    // DBObject implementation
    @Override
    public DBObject copy() {
        return new Fixchar(this);
    }

    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        bb.position(bb.position() + pad());
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        bb.position(bb.position() + pad());
    }

    @Override
    public int storeSize() {
        return super.storeSize() + pad();
    }

    private int pad() {
        return getMaxLength() - (isNull() ? -1 : getLength());
    }
}
