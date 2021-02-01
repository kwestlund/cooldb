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

package com.cooldb.storage;

import com.cooldb.buffer.FilePage;

/**
 * DatasetCursor keeps information needed by fetch operations to scroll back and
 * forth through a data store and maintain read consistency in the presence of
 * concurrent transactions modifying the same rows.
 */

public class DatasetCursor {

    private final Rowid rowid = new Rowid();
    private final FilePage lastPage = new FilePage();
    private long cusp; // Cursor Stability Point

    public Rowid getRowid() {
        return rowid;
    }

    public void setRowid(Rowid rowid) {
        this.rowid.assign(rowid);
    }

    public FilePage getLastPage() {
        return lastPage;
    }

    public void setLastPage(FilePage lastPage) {
        this.lastPage.assign(lastPage);
    }

    public long getCusp() {
        return cusp;
    }

    public void setCusp(long cusp) {
        this.cusp = cusp;
    }
}
