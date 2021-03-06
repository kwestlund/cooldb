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

class Root {
    private final FilePage page;
    private int level;

    Root(FilePage page, int level) {
        this.page = new FilePage(page);
        this.level = level;
    }

    FilePage getPage() {
        return page;
    }

    void setPage(FilePage page) {
        this.page.assign(page);
    }

    int getLevel() {
        return level;
    }

    void setLevel(int level) {
        this.level = level;
    }
}
