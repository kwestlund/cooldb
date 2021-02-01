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

class VersionPage extends FilePage {

    private long transId = 0;
    private long version = 0;

    VersionPage(VersionPage page) {
        super(page);
        transId = page.transId;
        version = page.version;
    }

    VersionPage(FilePage page) {
        super(page);
    }

    VersionPage(FilePage page, long transId, long version) {
        super(page);
        this.transId = transId;
        this.version = version;
    }

    public void assign(VersionPage page) {
        super.assign(page);
        transId = page.transId;
        version = page.version;
    }

    @Override
    public DBObject copy() {
        return new VersionPage(this);
    }

    public boolean isCopy() {
        return transId != 0;
    }

    // Comparable method
    @Override
    public int compareTo(Object o) {
        VersionPage other = (VersionPage) o;
        return (int) ((transId == other.transId) ? ((version == other.version) ? super
                .compareTo(other)
                : version - other.version)
                : (transId - other.transId));
    }
}
