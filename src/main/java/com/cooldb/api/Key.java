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
 * <code>Key</code> is an index key that uniquely identifies a single row in a
 * <code>Table</code>.
 * <p>
 * A <code>Key</code> can be obtained through an {@link Index} and remains
 * associated with that <code>Index</code> for the life of the
 * <code>Key</code>. It cannot be used with any other <code>Index</code>,
 * and if the <code>Index</code> becomes invalid, so does the key.
 * <p>
 * <code>Key</code> is comprised of one or more columns and in fact extends
 * {@link Row} for this purpose. Furthermore, the <code>Key</code> will
 * contain a <code>RID</code> if the columns alone are insufficient to
 * guarantee its uniqueness in the index. <code>Key</code> also adds methods
 * to set the key to the minimum or maximum value for its range.
 *
 * @see Row
 * @see RID
 * @see Index
 */

public interface Key extends Row {
    /**
     * Sets the value for this key to that which is less than all possible
     * values for the key domain.
     */
    void setMinValue();

    /**
     * Sets the value for this key to that which is greater than all possible
     * values for the key domain.
     */
    void setMaxValue();

    /**
     * Gets the row identifier appended to this key, if there is one.
     *
     * @return the row identifier of this key or null if there is none
     */
    RID getRID();

    /**
     * Sets the row identifier for this key. If the row identifier is non-null
     * then it will be appended to the value of the key in order to establish
     * the key's uniqueness. A reference to the RID object is kept in this key,
     * so subsequent changes to the RID object will be reflected in the value of
     * the key.
     *
     * @param rid the row identifier for this key, which may be null.
     */
    void setRID(RID rid);
}
