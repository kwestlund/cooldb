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
 * Defines the characteristics of a sort.
 */
public interface SortDelegate {
    /**
     * Requires the sort output to be distinct. Duplicates are discarded during
     * the sort so that the resulting output is guaranteed to be distinct.
     *
     * @return true if duplicates should be discarded to ensure that the sort output is distinct
     */
    boolean isDistinct();

    /**
     * Requires the sort input to be unique. An exception is generated and the
     * sort is abandoned if duplicates are detected during the sort.
     *
     * @return true if the sort should throw an exception if duplicates are found in the input
     */
    boolean isUnique();

    /**
     * Restricts the sort output to the top N records. If zero, no restriction
     * applies.
     *
     * @return the maximum number of records that the sort should output, or zero if all
     */
    long topN();
}
