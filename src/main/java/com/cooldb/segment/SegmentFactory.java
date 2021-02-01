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

import com.cooldb.buffer.FilePage;
import com.cooldb.transaction.RollbackException;
import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Core;
import com.cooldb.log.RedoLog;
import com.cooldb.log.UndoLog;
import com.cooldb.recovery.RecoveryDelegate;
import com.cooldb.recovery.RedoException;
import com.cooldb.recovery.SystemKey;
import com.cooldb.transaction.Transaction;

import java.util.HashMap;

public class SegmentFactory implements RecoveryDelegate {
    private final SystemKey systemKey;
    private final Core core;
    private SegmentManager segmentManager;
    private final HashMap<FilePage, SegmentMethod> segmentMethods = new HashMap<>();

    public SegmentFactory(SystemKey systemKey, Core core) {
        this.systemKey = systemKey;
        this.core = core;
    }

    /**
     * Create the SegmentManager and initialize the SegmentFactory for first
     * use.
     */
    public synchronized void create(Transaction trans,
                                    Segment segmentManagerSegment) throws DatabaseException {
        stop();

        // bootstrap the segment manager
        segmentManager = (SegmentManager) loadSegmentMethod(
                segmentManagerSegment, SegmentManager.class);

        // now create it
        createSegmentMethod(trans, segmentManager);
    }

    /**
     * Initialize the SegmentManager.
     */
    public synchronized void start(Segment segmentManagerSegment)
            throws DatabaseException {
        if (segmentManager == null) {
            // bootstrap the segment manager
            segmentManager = new SegmentManager(segmentManagerSegment, core);

            // now really load it, if it exists, otherwise stick with the
            // bootstrap instance
            SegmentManager sm = (SegmentManager) getSegmentMethod(segmentManagerSegment
                                                                          .getSegmentId());
            if (sm != null)
                segmentManager = sm;
        }
    }

    /**
     * Stop the SegmentManager.
     */
    public synchronized void stop() {
        // remove all mappings
        segmentMethods.clear();
        segmentManager = null;
    }

    /**
     * Instantiate the SegmentMethod, cache, and return a new SegmentMethod of
     * the specified class associated with the given segment.
     */
    public synchronized SegmentMethod loadSegmentMethod(Segment segment,
                                                        String className) throws DatabaseException {
        try {
            return loadSegmentMethod(segment, Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new DatabaseException("Failed to load segment.");
        }
    }

    public synchronized SegmentMethod loadSegmentMethod(Segment segment,
                                                        Class<?> clazz) throws DatabaseException {
        // load the segment manager
        try {
            // use a constructor that accepts a Segment parameter
            Class<?>[] params = new Class[]{Segment.class, Core.class};
            Object[] args = new Object[]{segment, core};

            SegmentMethod sm = (SegmentMethod) clazz.getConstructor(params)
                    .newInstance(args);

            segmentMethods.put(sm.getSegment().getSegmentId(), sm);

            return sm;
        } catch (Exception e) {
            throw new DatabaseException("Failed to load segment.");
        }
    }

    public synchronized void unloadSegmentMethod(SegmentMethod sm) {
        segmentMethods.remove(sm.getSegment().getSegmentId());
    }

    /**
     * Register the SegmentMethod class in the SystemKey (if not already there)
     * and set the registration type code in the segment, which is persisted,
     * then instantiate, cache, and return the new SegmentMethod associated with
     * the given segment.
     */
    public synchronized SegmentMethod createSegmentMethod(Transaction trans,
                                                          Segment segment, Class<?> clazz) throws DatabaseException {
        SegmentMethod sm = loadSegmentMethod(segment, clazz);
        try {
            createSegmentMethod(trans, sm);
        } catch (Exception e) {
            unloadSegmentMethod(sm);
            throw new DatabaseException("Failed to create segment method.");
        }
        return sm;
    }

    public synchronized void createSegmentMethod(Transaction trans,
                                                 SegmentMethod sm) throws DatabaseException {
        Segment segment = sm.getSegment();

        boolean isCreate = !segmentManager.select(segment);

        segment.setSegmentType(systemKey.registerSegmentMethod(sm.getClass()
                                                                       .getName()));

        sm.createSegmentMethod(trans);

        // Make sure this follows the creation of the segment method in case
        // it is the segmentManager itself that is being created
        if (isCreate)
            segmentManager.insert(trans, segment);
        else
            segmentManager.update(trans, segment);
    }

    /**
     * Get the SegmentMethod associated with the given segment id from the
     * cache, loading it into the cache first if necessary.
     */
    public synchronized SegmentMethod getSegmentMethod(FilePage segmentId)
            throws DatabaseException {
        SegmentMethod segmentMethod = segmentMethods.get(segmentId);

        if (segmentMethod == null)
            segmentMethod = loadSegmentMethod(segmentId);

        return segmentMethod;
    }

    /**
     * Remove the SegmentMethod from both the cache and external storage.
     */
    public synchronized void removeSegmentMethod(Transaction trans,
                                                 SegmentMethod sm) throws DatabaseException {
        sm.dropSegmentMethod(trans);
        segmentMethods.remove(sm.getSegment().getSegmentId());
        segmentManager.remove(trans, sm.getSegment());
    }

    // RecoveryDelegate implementation

    /**
     * Update the segment descriptor in external storage.
     */
    public synchronized void updateSegment(Transaction trans, Segment segment)
            throws DatabaseException {
        segmentManager.update(trans, segment);
    }

    public synchronized void insertSegment(Transaction trans, Segment segment)
            throws DatabaseException {
        segmentManager.insert(trans, segment);
    }

    public synchronized int selectSegment(Segment segment, int index,
                                          Filter filter) throws DatabaseException {
        return segmentManager.select(segment, index, filter);
    }

    public synchronized byte getSegmentType(Class<?> clazz) {
        return systemKey.registerSegmentMethod(clazz.getName());
    }

    public void redo(RedoLog log) throws RedoException {
        try {
            getSegmentMethod(log.getSegmentId()).redo(log);
        } catch (Exception e) {
            throw new RedoException(e);
        }
    }

    public void undo(UndoLog log, Transaction trans) throws RollbackException {
        try {
            getSegmentMethod(log.getSegmentId()).undo(log, trans);
        } catch (Exception e) {
            throw new RollbackException(e);
        }
    }

    public void undo(UndoLog log, PageBuffer pb) throws RollbackException {
        try {
            getSegmentMethod(log.getSegmentId()).undo(log, pb);
        } catch (Exception e) {
            throw new RollbackException(e);
        }
    }

    // debugging: dump segments
    public void dump() {
        segmentManager.dump();
    }

    private SegmentMethod loadSegmentMethod(FilePage segmentId)
            throws DatabaseException {
        // lookup the segment
        Segment segment = new Segment(segmentId);
        if (!segmentManager.select(segment))
            return null;

        return loadSegmentMethod(segment);
    }

    private SegmentMethod loadSegmentMethod(Segment segment)
            throws DatabaseException {
        return loadSegmentMethod(segment, systemKey.getSegmentMethod(segment
                                                                             .getSegmentType()));
    }
}
