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
import com.cooldb.api.Filter;
import com.cooldb.buffer.FileManager;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.segment.SpaceManager;
import com.cooldb.sort.SortArea;
import com.cooldb.storage.StorageMethod;
import com.cooldb.storage.TempStorageMethod;
import com.cooldb.transaction.NestedTopAction;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;

import java.util.ArrayList;

/**
 * SortManager provides resources for sorting.
 */

public class SortManager {

    private final Core core;
    private ArrayList<SortArea> areas;

    SortManager(Core core) {
        this.core = core;
        start();
    }

    /**
     * Compare two normalized keys.
     */
    public static int compare(byte[] b1, int i1, int l1, byte[] b2, int i2,
                              int l2) {
        int diff;
        int l = Math.min(l1, l2);
        for (int i = 0; i < l; i++) {
            diff = (0xff & b1[i1 + i]) - (0xff & b2[i2 + i]);
            if (diff != 0)
                return diff;
        }
        return l1 - l2;
    }

    // TODO: make the number of sort buffers (default=100) configurable
    public int getNBuffers() {
        return 100;
    }

    // TODO: make the in-memory threshold (default=1000) configurable
    public int getInMemoryThreshold() {
        return 1000;
    }

    /**
     * Creates and starts at least one sort area.
     */
    public void start() {
        if (areas == null) {
            areas = new ArrayList<>();
            SortArea sa = new SortArea(this);
            areas.add(sa);
        }
    }

    /**
     * Stops all sort areas.
     */
    public synchronized void stop() {
        if (areas != null) {
            for (SortArea sa : areas) {
                sa.stop();
            }
            areas.clear();
            areas = null;
        }
    }

    public int getBufferSize() {
        return FileManager.DEFAULT_PAGE_SIZE;
    }

    public synchronized SortArea grabSortArea(Transaction trans)
            throws InterruptedException {
        if (areas == null) {
            Thread.currentThread().interrupt();
            throw new InterruptedException();
        }
        SortArea sortArea = null;

        // Look for an available pre-existing SortArea in the pool
        for (SortArea sa : areas) {
            if (!sa.isHeld()) {
                sortArea = sa;
                break;
            }
        }

        // Create a new SortArea if necessary and add it to the pool
        if (sortArea == null) {
            sortArea = new SortArea(this);
            areas.add(sortArea);
        }

        // Reserve the SortArea for the life of the specified transaction or
        // until the SortArea is explicitly released, whichever comes first
        sortArea.grab(trans);

        return sortArea;
    }

    public StorageMethod createTemp(Transaction trans, int pages)
            throws DatabaseException {
        TransactionManager tm = core.getTransactionManager();
        SegmentFactory sf = core.getSegmentFactory();

        NestedTopAction savePoint = tm.beginNestedTopAction(trans);
        try {
            // Create and return temporary storage with fixed-size extents
            SpaceManager sm = core.getSpaceManager();
            Segment segment = sm.createSegment(trans, pages, pages, 0);
            StorageMethod temp = (StorageMethod) sf.createSegmentMethod(trans,
                                                                        segment, TempStorageMethod.class);

            tm.commitNestedTopAction(trans, savePoint);

            return temp;
        } catch (Exception e) {
            tm.rollbackNestedTopAction(trans, savePoint, sf);
            throw new DatabaseException("Failed to create temporary segment", e);
        }
    }

    public void dropTemp(Transaction trans, StorageMethod temp)
            throws DatabaseException {
        TransactionManager tm = core.getTransactionManager();
        SegmentFactory sf = core.getSegmentFactory();

        NestedTopAction savePoint = tm.beginNestedTopAction(trans);
        try {
            SpaceManager sm = core.getSpaceManager();

            sf.removeSegmentMethod(trans, temp);
            sm.dropSegment(trans, temp.getSegment());

            tm.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            tm.rollbackNestedTopAction(trans, savePoint, sf);
            throw new DatabaseException("Failed to drop temporary segment", e);
        }
    }

    /**
     * Removes any leftover temporary segments during restart recovery.
     */
    public void recover(Transaction trans) throws DatabaseException {
        SegmentFactory sf = core.getSegmentFactory();
        final byte tempSegmentType = sf.getSegmentType(TempStorageMethod.class);
        Segment segment = new Segment();
        Filter filter = o -> ((Segment) o).getSegmentType() == tempSegmentType;
        int i = 0;
        while ((i = sf.selectSegment(segment, i, filter)) != -1) {
            dropTemp(trans, (TempStorageMethod) sf.getSegmentMethod(segment
                                                                            .getSegmentId()));
        }
    }
}
