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

package com.cooldb.log;

import java.nio.ByteBuffer;

public class UndoLog extends Log {

    private static final int OVERHEAD = UndoPointer.sizeOf() * 2;
    UndoPointer address; // Address of record in database
    UndoPointer pageUndoNxtLSN; // Address of preceding undo log record to same

    public UndoLog() {
        super();
        this.pageUndoNxtLSN = new UndoPointer();
    }

    // Copy constructor
    public UndoLog(UndoLog log) {
        super(log);
        if (log.address != null)
            address = new UndoPointer(log.address);
        if (log.pageUndoNxtLSN != null)
            pageUndoNxtLSN = new UndoPointer(log.pageUndoNxtLSN);
    }

    public UndoPointer getAddress() {
        return address;
    }

    public void setAddress(UndoPointer address) {
        this.address = new UndoPointer(address);
    }

    public UndoPointer getPageUndoNxtLSN() {
        return pageUndoNxtLSN;
    }

    public void setPageUndoNxtLSN(UndoPointer pageUndoNxtLSN) {
        this.pageUndoNxtLSN = new UndoPointer(pageUndoNxtLSN);
    }

    // Storable
    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        if (address == null)
            address = new UndoPointer();
        address.writeTo(bb);
        if (pageUndoNxtLSN == null)
            pageUndoNxtLSN = new UndoPointer();
        pageUndoNxtLSN.writeTo(bb);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        if (address == null)
            address = new UndoPointer();
        address.readFrom(bb);
        if (pageUndoNxtLSN == null)
            pageUndoNxtLSN = new UndoPointer();
        pageUndoNxtLSN.readFrom(bb);
    }
    // page

    @Override
    public int storeSize() {
        return super.storeSize() + OVERHEAD;
    }
}
