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

package com.cooldb.segment;

import com.cooldb.buffer.DBObject;

/**
 * DBObjectArray specifies methods to read from an array of {@link DBObject}s.
 */

public interface DBObjectArray {
    /**
     * Gets the number of entries.
     *
     * @return the number of entries in the array
     */
    int getCount();

    /**
     * Gets the entry at the specified index.
     *
     * @param index the index containing the entry value
     * @param entry the object that reads the value at <code>index</code>
     * @return the same entry as the argument for invocation chaining
     */
    DBObject get(int index, DBObject entry);
}
