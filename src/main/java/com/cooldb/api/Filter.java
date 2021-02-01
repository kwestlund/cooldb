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

package com.cooldb.api;

/**
 * Filter specifies a single method that determines whether its argument passes
 * or fails some conditional requirement of the filter.
 */

public interface Filter {
    /**
     * Determines whether the object passes or fails some conditional
     * requirement of the filter.
     *
     * @param o the object to be tested by the filter
     * @return true if the object passes the filter, false otherwise
     */
    boolean passes(Object o);
}
