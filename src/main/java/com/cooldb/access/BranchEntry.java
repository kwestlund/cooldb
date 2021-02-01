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
import com.cooldb.buffer.FilePage;

/**
 * <code>BranchEntry</code> extends NodeEntry with attributes and methods
 * specific to branch nodes.
 */

class BranchEntry extends NodeEntry {
    BranchEntry(GiST.Predicate predicate) {
        super(predicate);
        pointer = new FilePage();
    }

    BranchEntry(BranchEntry entry) {
        super(entry);
    }

    @Override
    public void assign(DBObject o) {
        BranchEntry entry = (BranchEntry) o;
        predicate.assign(entry.getPredicate());
        pointer.assign(entry.getPointer());
    }

    public DBObject copy() {
        return new BranchEntry(this);
    }
}
