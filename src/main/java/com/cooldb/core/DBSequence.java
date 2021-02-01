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

package com.cooldb.core;

import com.cooldb.api.DatabaseException;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.storage.Dataset;
import com.cooldb.storage.Rowid;
import com.cooldb.transaction.NestedTopAction;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;

public class DBSequence {

    private final Dataset sequences;
    private final TransactionManager transactionManager;
    private final SegmentFactory segmentFactory;
    private final Rowid rowid;
    private final SequenceDescriptor sdbuf = new SequenceDescriptor();
    private SequenceDescriptor descriptor;
    private long nextval;
    private long end;

    DBSequence(Dataset sequences, TransactionManager transactionManager,
               SegmentFactory segmentFactory, SequenceDescriptor descriptor,
               Rowid rowid) {
        this.sequences = sequences;
        this.transactionManager = transactionManager;
        this.segmentFactory = segmentFactory;
        this.descriptor = descriptor;
        this.rowid = rowid;
        nextval = descriptor.getNextval();
        end = nextval;
    }

    public String getName() throws DatabaseException {
        validate();

        return descriptor.getName();
    }

    public synchronized long next(Transaction trans) throws DatabaseException {
        validate();

        if (nextval == end) {
            end += descriptor.getCacheSize();
            descriptor.setNextval(end);
            updateSequence(trans);
        }

        return nextval++;
    }

    synchronized void invalidate() {
        descriptor = null;
    }

    synchronized void validate() throws DatabaseException {
        if (descriptor == null)
            throw new DatabaseException("Invalid sequence.");
    }

    private void updateSequence(Transaction trans) throws DatabaseException {
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            // update descriptor in sequences dataset
            sequences.update(trans, descriptor, rowid, sdbuf);

            transactionManager.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint,
                                                       segmentFactory);
            throw new DatabaseException("Failed to update sequence: "
                                                + descriptor.getName(), e);
        }
    }
}
