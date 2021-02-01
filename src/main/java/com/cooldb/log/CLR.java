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

/**
 * A Compensation Log Record (CLR) records the undoing of a prior update so that
 * the undoing may be redone if necessary. The undoNxtLSN of the CLR points to
 * the corresponding UndoCLR, which will point its undoNxtLSN to the undoNxtLSN
 * of the undone log update.
 */

public class CLR extends RedoLog {
    public CLR() {
        super();
    }

    public CLR(UndoPointer undoNxtLSN) {
        super();
        setType(CLR);
        setUndoNxtLSN(undoNxtLSN);
    }

    public CLR(UndoLog log) {
        super();
        setType(CLR);
        setUndoNxtLSN(log.getUndoNxtLSN());
        setTransID(log.getTransID());
        setSegmentId(log.getSegmentId());
        setPage(log.getPage());
        setSegmentType(log.getSegmentType());
        setPageType(log.getPageType());
    }
}
