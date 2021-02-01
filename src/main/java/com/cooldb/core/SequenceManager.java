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
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.segment.SpaceManager;
import com.cooldb.storage.Dataset;
import com.cooldb.storage.DatasetCursor;
import com.cooldb.storage.Rowid;
import com.cooldb.transaction.NestedTopAction;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;

import java.util.HashMap;

/**
 * SequenceManager is a central point of control for creating, dropping, and
 * accessing sequences.
 */

public class SequenceManager {
    private final Dataset sequences;
    private final TransactionManager transactionManager;
    private final SegmentFactory segmentFactory;
    private final SpaceManager spaceManager;
    private final HashMap<String, DBSequence> residentSequences = new HashMap<>();

    SequenceManager(Dataset sequences, TransactionManager transactionManager,
                    SegmentFactory segmentFactory, SpaceManager spaceManager) {
        this.sequences = sequences;
        this.transactionManager = transactionManager;
        this.segmentFactory = segmentFactory;
        this.spaceManager = spaceManager;
    }

    public synchronized DBSequence createSequence(Transaction trans, String name)
            throws DatabaseException {
        // TODO: grab table lock
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            SequenceDescriptor arg = new SequenceDescriptor();
            SequenceDescriptor res = new SequenceDescriptor();
            arg.setName(name);
            DatasetCursor cursor = new DatasetCursor();
            sequences.openCursor(trans, cursor);
            if (sequences.fetchNext(trans, cursor, res, arg))
                throw new DatabaseException("Sequence already exists: " + name);

            Segment segment = spaceManager.createSegment(trans);

            // insert descriptor into sequences dataset
            arg.setSegmentId(segment.getSegmentId());
            Rowid rowid = sequences.insert(trans, arg);

            transactionManager.commitNestedTopAction(trans, savePoint);

            DBSequence sequence = new DBSequence(sequences, transactionManager,
                                                 segmentFactory, arg, rowid);
            residentSequences.put(name, sequence);

            return sequence;
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint,
                                                       segmentFactory);
            throw new DatabaseException("Failed to create sequence: " + name, e);
        }
    }

    public synchronized void dropSequence(Transaction trans, String name)
            throws DatabaseException {
        // TODO: grab table lock
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            SequenceDescriptor arg = new SequenceDescriptor();
            SequenceDescriptor sd = new SequenceDescriptor();
            arg.setName(name);
            DatasetCursor cursor = new DatasetCursor();
            sequences.openCursor(trans, cursor);
            if (!sequences.fetchNext(trans, cursor, sd, arg))
                throw new DatabaseException("Sequence does not exist: " + name);

            // prevent any further operations on the sequence
            DBSequence sequence = residentSequences.get(name);
            if (sequence != null) {
                sequence.invalidate();
                residentSequences.remove(name);
            }

            // remove descriptor from sequences dataset
            sequences.remove(trans, cursor.getRowid(), arg);

            transactionManager.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint,
                                                       segmentFactory);
            throw new DatabaseException("Failed to drop sequence: " + name, e);
        }
    }

    public synchronized DBSequence getSequence(Transaction trans, String name)
            throws DatabaseException {
        DBSequence sequence = residentSequences.get(name);
        if (sequence != null)
            return sequence;

        SequenceDescriptor arg = new SequenceDescriptor();
        SequenceDescriptor res = new SequenceDescriptor();
        arg.setName(name);
        DatasetCursor cursor = new DatasetCursor();
        sequences.openCursor(trans, cursor);
        if (!sequences.fetchNext(trans, cursor, res, arg))
            return null;

        sequence = new DBSequence(sequences, transactionManager,
                                  segmentFactory, res, cursor.getRowid());
        residentSequences.put(name, sequence);

        return sequence;
    }
}
