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

import com.cooldb.buffer.*;
import com.cooldb.api.DatabaseException;
import com.cooldb.core.Core;
import com.cooldb.log.LogData;
import com.cooldb.log.LogExhaustedException;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.transaction.NestedTopAction;
import com.cooldb.transaction.Transaction;
import com.cooldb.buffer.*;

public abstract class AbstractSegmentMethod implements SegmentMethod {

    /**
     * Type code of the base Segment page.
     */
    public static final byte SEGMENT_PAGE_TYPE = 0;
    protected final Segment segment;
    protected final Core core;
    private final PageBroker pageBroker;

    public AbstractSegmentMethod(Segment segment, Core core) {
        this.segment = segment;
        this.core = core;

        pageBroker = new PageBroker(segment, core.getTransactionManager());
    }

    public void createSegmentMethod(Transaction trans) throws DatabaseException {
        getCatalogMethod().insert(trans, getDescriptor());

        // make sure the first page is allocated
        allocateNextPage(trans);
    }

    public void dropSegmentMethod(Transaction trans) throws DatabaseException {
        getCatalogMethod().remove(trans, getDescriptor());
    }

    public PageBroker allocPageBroker() {
        return new PageBroker(segment, core.getTransactionManager());
    }

    public FilePage allocateNextPage(Transaction trans) throws DatabaseException {
        return core.getSpaceManager().allocateNextPage(trans, segment, this);
    }

    public NestedTopAction beginNestedTopAction(Transaction trans) {
        return core.getTransactionManager().beginNestedTopAction(trans);
    }

    public void commitNestedTopAction(Transaction trans, NestedTopAction savePoint)
            throws DatabaseException, InterruptedException {
        core.getTransactionManager().commitNestedTopAction(trans, savePoint);
    }

    public void rollbackNestedTopAction(Transaction trans,
                                        NestedTopAction savePoint) throws DatabaseException {
        core.getTransactionManager().rollbackNestedTopAction(trans, savePoint,
                                                             core.getSegmentFactory());
    }

    // SegmentMethod implementation
    public Segment getSegment() {
        return segment;
    }

    // PageBroker wrappers for segment anchor page
    public PageBuffer readPin() throws BufferNotFound, InterruptedException {
        return pageBroker.readPin(segment.getSegmentId());
    }

    public PageBuffer writePin(boolean isNew) throws BufferNotFound,
            InterruptedException {
        return pageBroker.writePin(segment.getSegmentId(), isNew);
    }

    public PageBuffer logPin() throws BufferNotFound, InterruptedException {
        return pageBroker.logPin(segment.getSegmentId(), SEGMENT_PAGE_TYPE);
    }

    public PageBuffer redoPin(RedoLog redoLog) throws BufferNotFound,
            InterruptedException {
        return pageBroker.redoPin(redoLog);
    }

    public PageBuffer undoPin(UndoLog undoLog) throws BufferNotFound,
            InterruptedException {
        return pageBroker.undoPin(undoLog);
    }

    public PageBuffer bufferPin(PageBuffer pageBuffer) {
        return pageBroker.bufferPin(pageBuffer);
    }

    public void unPin(BufferPool.Affinity affinity)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        pageBroker.unPin(affinity);
    }

    public void unPin(Transaction trans, BufferPool.Affinity affinity)
            throws LogExhaustedException, BufferNotFound, InterruptedException {
        pageBroker.unPin(trans, affinity);
    }

    public LogData attachUndo(byte type, int size) {
        return pageBroker.attachUndo(type, size);
    }

    public LogData attachRedo(byte type, int size) {
        return pageBroker.attachRedo(type, size);
    }

    public LogData attachCLR(byte type, int size) {
        return pageBroker.attachCLR(type, size);
    }

    public void attachUndo(byte type, DBObject obj) {
        pageBroker.attachUndo(type, obj);
    }

    public void attachRedo(byte type, DBObject obj) {
        pageBroker.attachRedo(type, obj);
    }

    public void attachCLR(byte type, DBObject obj) {
        pageBroker.attachCLR(type, obj);
    }

    public void attachCLR(LogData rld) {
        pageBroker.attachCLR(rld);
    }
}
