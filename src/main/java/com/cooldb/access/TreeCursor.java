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

import com.cooldb.buffer.FilePage;

/**
 * TreeCursor keeps information needed by fetch operations to scroll back and
 * forth through an index and maintain read consistency in the presence of
 * concurrent transactions modifying the same rows.
 */

public class TreeCursor implements GiST.Cursor {

    private final GiST.Entry entry;
    private final FilePage leaf = new FilePage();
    private long cusp; // Cursor Stability Point
    private int index;
    private long pageLSN;

    TreeCursor(GiST.Entry entry) {
        this.entry = entry;
    }

    // GiST.Cursor implementation
    public long getCusp() {
        return cusp;
    }

    public void setCusp(long cusp) {
        this.cusp = cusp;
    }

    public GiST.Entry getEntry() {
        return entry;
    }

    public void setEntry(GiST.Entry entry) {
        this.entry.assign(entry);
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public FilePage getLeaf() {
        return leaf;
    }

    public void setLeaf(FilePage leaf) {
        this.leaf.assign(leaf);
    }

    public long getPageLSN() {
        return pageLSN;
    }

    public void setPageLSN(long pageLSN) {
        this.pageLSN = pageLSN;
    }
}
