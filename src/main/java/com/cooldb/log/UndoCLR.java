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
 * An Undo Compensation Log Record (UndoCLR) is used to skip over updates in the
 * undo log file that have already been undone. It corresponds to a specific CLR
 * redo record, which records the fact of the undo for redo.
 * <p>
 * The undoNxtLSN argument should specify the previous UndoLog for the page.
 */

public class UndoCLR extends UndoLog {
    public UndoCLR(CLR clr) {
        super();
        setType(CLR);
        setUndoNxtLSN(clr.getUndoNxtLSN());
        setTransID(clr.getTransID());
        setSegmentId(clr.getSegmentId());
        setPage(clr.getPage());
        setSegmentType(clr.getSegmentType());
        setPageType(clr.getPageType());
    }
}
