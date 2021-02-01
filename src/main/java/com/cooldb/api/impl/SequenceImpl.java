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

package com.cooldb.api.impl;

import com.cooldb.api.DatabaseException;
import com.cooldb.api.Sequence;
import com.cooldb.core.DBSequence;

public class SequenceImpl implements Sequence {

    private final SessionImpl session;
    private final DBSequence sequence;

    SequenceImpl(SessionImpl session, DBSequence sequence) {
        this.session = session;
        this.sequence = sequence;
    }

    @Override
    public String getName() throws DatabaseException {
        try {
            return sequence.getName();
        } catch (DatabaseException e) {
            throw new DatabaseException("Failed get sequence name.", e);
        }
    }

    @Override
    public long next() throws DatabaseException {
        try {
            return sequence.next(session.getTransaction());
        } catch (DatabaseException e) {
            throw new DatabaseException("Failed get next sequence number.", e);
        }
    }
}
