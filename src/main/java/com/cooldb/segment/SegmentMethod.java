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

import com.cooldb.api.DatabaseException;
import com.cooldb.recovery.RecoveryDelegate;
import com.cooldb.transaction.Transaction;

public interface SegmentMethod extends RecoveryDelegate, SpaceDelegate {

    /**
     * Returns the Segment attributes.
     */
    Segment getSegment();

    /**
     * Initializes the external state.
     */
    void createSegmentMethod(Transaction trans) throws DatabaseException;

    /**
     * Removes the external state.
     */
    void dropSegmentMethod(Transaction trans) throws DatabaseException;

    /**
     * Gets the <code>CatalogMethod</code> containing the descriptor for this
     * method.
     */
    CatalogMethod getCatalogMethod();

    /**
     * Gets the <code>SegmentDescriptor</code> with attributes describing this
     * method.
     */
    SegmentDescriptor getDescriptor();
}
