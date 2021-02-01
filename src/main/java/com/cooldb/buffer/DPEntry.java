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

public class DPEntry extends FilePage {
    private long recLSN;

    public DPEntry() {
        super();
    }

    public DPEntry(FilePage page, long recLSN) {
        super(page);
        this.recLSN = recLSN;
    }

    public void assign(DPEntry page) {
        super.assign(page);
        recLSN = page.recLSN;
    }

    public long getRecLSN() {
        return recLSN;
    }

    public void setRecLSN(long recLSN) {
        this.recLSN = recLSN;
    }

    // DBObject methods
    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        bb.putLong(recLSN);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        recLSN = bb.getLong();
    }

    @Override
    public int storeSize() {
        return super.storeSize() + 8;
    }
}
