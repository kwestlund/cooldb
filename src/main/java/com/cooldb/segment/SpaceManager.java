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
import com.cooldb.transaction.RollbackDelegate;
import com.cooldb.transaction.RollbackException;
import com.cooldb.transaction.TransactionCancelledException;
import com.cooldb.transaction.TransactionManager;
import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.DBFile;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.log.UndoLog;
import com.cooldb.recovery.SystemKey;
import com.cooldb.transaction.*;
import com.cooldb.transaction.NestedTopAction;
import com.cooldb.transaction.Transaction;

import java.util.Comparator;

/**
 * SpaceManager manages space allocated to Segments, in variable-sized groups of
 * pages, or Extents.
 */

public class SpaceManager implements RollbackDelegate {

    static boolean testFailure1;
    static boolean testFailure2;
    private DBFile sysFile;
    private final SegmentFactory segmentFactory;
    private final TransactionManager transactionManager;
    private final SystemKey systemKey;
    private FreeExtentMethod freeExtents;
    private UsedExtentMethod usedExtents;
    private Extender extender;

    public SpaceManager(SegmentFactory segmentFactory,
                        TransactionManager transactionManager, SystemKey systemKey) {
        this.segmentFactory = segmentFactory;
        this.transactionManager = transactionManager;
        this.systemKey = systemKey;
    }

    public void create(Transaction trans, DBFile sysFile,
                       FilePage freeExtentsId, FilePage usedExtentsId, Extent sysExtent)
            throws DatabaseException {
        try {
            stop();

            this.sysFile = sysFile;
            freeExtents = (FreeExtentMethod) segmentFactory
                    .createSegmentMethod(trans, new Segment(freeExtentsId),
                                         FreeExtentMethod.class);
            usedExtents = (UsedExtentMethod) segmentFactory
                    .createSegmentMethod(trans, new Segment(usedExtentsId),
                                         UsedExtentMethod.class);

            freeExtents.insertExtent(trans, sysExtent);
        } catch (Exception e) {
            throw new DatabaseException("Failed to create space manager.", e);
        }
    }

    public void start(DBFile sysFile, FilePage freeExtentsId,
                      FilePage usedExtentsId) throws DatabaseException {
        if (this.sysFile == null) {
            this.sysFile = sysFile;
            freeExtents = (FreeExtentMethod) segmentFactory
                    .getSegmentMethod(freeExtentsId);
            usedExtents = (UsedExtentMethod) segmentFactory
                    .getSegmentMethod(usedExtentsId);

            extender = new Extender();
            extender.start();
        }
    }

    public void stop() {
        if (extender != null) {
            extender.shutdown();
            extender = null;
        }
        sysFile = null;
        freeExtents = null;
        usedExtents = null;
    }

    /**
     * Allocate the first extent to the segment, set the segmentId to the start
     * page of the newly allocated extent, and insert the segment into the
     * segment catalog.
     */
    public Segment createSegment(Transaction trans) throws DatabaseException {
        Segment segment = new Segment();

        allocateNextExtent(trans, segment, false);

        return segment;
    }

    // RollbackDelegate implementation

    /**
     * Allocate the first extent to the segment, set the segmentId to the start
     * page of the newly allocated extent, and insert the segment into the
     * segment catalog.
     */
    public Segment createSegment(Transaction trans, int initialSize,
                                 int nextSize, float growthRate) throws DatabaseException {
        Segment segment = new Segment();

        segment.setInitialSize(initialSize);
        segment.setNextSize(nextSize);
        segment.setGrowthRate(growthRate);

        allocateNextExtent(trans, segment, false);

        return segment;
    }

    /**
     * Create the segment with a pre-allocated extent.
     */
    public Segment createSegment(Transaction trans, Extent extent)
            throws DatabaseException {
        Segment segment = new Segment();

        allocateExtent(trans, segment, extent, segment.getNextSize(), false);

        return segment;
    }

    /**
     * Free all extents allocated to the segment.
     */
    public void dropSegment(Transaction trans, Segment segment)
            throws DatabaseException {
        freeExtents(trans, segment, false);
    }

    /**
     * Allocate the next extent to the segment in an atomic action.
     * <p>
     * If the segmentId is null, then the segmentId is set to the start page of
     * the newly allocated extent.
     * <p>
     * The size of the extent is based on the newExtent attribute of the
     * segment, and following the allocation the newExtent and nextSize
     * attributes of the segment itself are updated to reflect the allocation.
     * In particular, the newExtent is set to the allocated extent and nextSize
     * is multiplied by the growthRate.
     * <p>
     * If no space is available in the database, then extend the system datafile
     * by at least the amount required to satisfy this request. If there is no
     * space left in the file system, then throw an OutOfSpace exception.
     */
    public void allocateNextExtent(Transaction trans, Segment segment)
            throws DatabaseException {
        allocateNextExtent(trans, segment, true);
    }

    private synchronized void allocateNextExtent(Transaction trans,
                                                 Segment segment, boolean commit) throws DatabaseException {
        Extent extent = new Extent();

        // determine extent size
        int esize = 1;
        int nextSize = 1;

        if (segment.getNewExtent().isNull()) {
            esize = Math.max(esize, segment.getInitialSize());
            nextSize = Math.max(nextSize, segment.getNextSize());
        } else {
            esize = Math.max(esize, segment.getNextSize());
            nextSize = Math.max(1, (int) (segment.getNextSize() * segment
                    .getGrowthRate()));
        }

        // now look for an extent of that size in the free extents list
        try {
            Extent tmp = new Extent();
            boolean found;
            do {
                found = freeExtents.findExtent(esize, extent);

                // look for an extent 3 times its size and pre-extend the file
                // if not found
                if (!found || !freeExtents.findExtent(esize * 3, tmp))
                    extender.extend(esize * 3);

                if (!found)
                    extender.waitUntilDone();
            } while (!found);
        } catch (Exception e) {
            throw new DatabaseException("Failed to allocate extent.", e);
        }

        extent.setSize(esize);

        allocateExtent(trans, segment, extent, nextSize, commit);
    }

    /**
     * Free all extents allocated to the segment in an atomic action, and update
     * the newExtent and nextSize attributes of the segment itself to reflect
     * the action. In particular, the newExtent is set null and the nextSize is
     * set to the initialSize.
     */
    public void freeExtents(Transaction trans, Segment segment)
            throws DatabaseException {
        freeExtents(trans, segment, true);
    }

    private void allocateExtent(Transaction trans, Segment segment,
                                Extent extent, int nextSize, boolean commit) throws DatabaseException {

        Segment segmentCopy = new Segment(segment);

        // make sure this action is atomic
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            freeExtents.removeExtent(trans, extent);

            boolean isCreate = false;
            if (segmentCopy.getSegmentId().isNull()) {
                segmentCopy.setSegmentId(extent);
                isCreate = true;
            }

            usedExtents.insertExtent(trans, new SegmentExtent(segmentCopy
                                                                      .getSegmentId(), extent));

            segmentCopy.setNewExtent(extent);
            segmentCopy.setNextSize(nextSize);
            segmentCopy.setNextPage(extent.getPageId());
            segmentCopy.setPageCount(segmentCopy.getPageCount()
                                             + extent.getSize());

            if (isCreate)
                segmentFactory.insertSegment(trans, segmentCopy);
            else
                segmentFactory.updateSegment(trans, segmentCopy);

            // optionally commit the nested action
            if (commit)
                transactionManager.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint, this);
            throw new DatabaseException("Failed to allocate extent.", e);
        }

        segment.assign(segmentCopy);
    }

    private synchronized void freeExtents(Transaction trans, Segment segment,
                                          boolean commit) throws DatabaseException {

        // make sure this action is atomic
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            SegmentComparator sc = new SegmentComparator(segment);

            SegmentExtent segmentExtent = new SegmentExtent();
            while (usedExtents.findExtent(segmentExtent, sc)) {
                usedExtents.removeExtent(trans, segmentExtent);
                freeExtents.insertExtent(trans, new Extent(segmentExtent));
            }

            // optionally commit the nested action
            if (commit)
                transactionManager.commitNestedTopAction(trans, savePoint);
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint, this);
            throw new DatabaseException("Failed to free segment extents.", e);
        }
    }

    /**
     * Allocate the next page of the last extent allocated to the segment by
     * updating both the segment and the delegate in a single atomic action. If
     * necessary, first allocate to the segment a new extent.
     */
    public FilePage allocateNextPage(Transaction trans, Segment segment,
                                     SpaceDelegate delegate) throws DatabaseException {
        if (segment.getNewExtent().isNull()
                || segment.getNextPage() == segment.getNewExtent()
                .getEndPageId())
            allocateNextExtent(trans, segment);

        // save the allocation, making sure the action is atomic
        NestedTopAction savePoint = transactionManager.beginNestedTopAction(trans);
        try {
            FilePage newPage = new FilePage(segment.getNewExtent());
            newPage.setPageId(segment.getNextPage());

            // remove page from segment by incrementing page pointer
            segment.setNextPage(segment.getNextPage() + 1);

            // update the segment
            segmentFactory.updateSegment(trans, segment);

            // now notify the delegate
            delegate.didAllocatePage(trans, newPage);

            // commit the nested transaction
            transactionManager.commitNestedTopAction(trans, savePoint);

            return newPage;
        } catch (Exception e) {
            transactionManager.rollbackNestedTopAction(trans, savePoint, this);
            throw new DatabaseException("Failed allocate new page.", e);
        }
    }

    public void undo(UndoLog log, Transaction trans) throws RollbackException {
        if (log.getSegmentId().equals(freeExtents.getSegment().getSegmentId()))
            freeExtents.undo(log, trans);
        else if (log.getSegmentId().equals(
                usedExtents.getSegment().getSegmentId()))
            usedExtents.undo(log, trans);
        else
            segmentFactory.undo(log, trans);
    }

    public void undo(UndoLog log, PageBuffer pb) throws RollbackException {
        if (log.getSegmentId().equals(freeExtents.getSegment().getSegmentId()))
            freeExtents.undo(log, pb);
        else if (log.getSegmentId().equals(
                usedExtents.getSegment().getSegmentId()))
            usedExtents.undo(log, pb);
        else
            segmentFactory.undo(log, pb);
    }

    @SuppressWarnings("rawtypes")
    static class SegmentComparator implements Comparator {
        private final Segment segment = new Segment();

        SegmentComparator(Segment segment) {
            this.segment.assign(segment);
        }

        public int compare(Object o1, Object o2) {
            return ((SegmentExtent) o1).getSegmentId().compareTo(
                    ((SegmentExtent) o2).getSegmentId());
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SegmentExtent)) return false;
            return ((SegmentExtent) obj).getSegmentId().equals(
                    segment.getSegmentId());
        }
    }

    @SuppressWarnings("rawtypes")
    static class OverlapsComparator implements Comparator {
        private final SegmentExtent extent = new SegmentExtent();

        OverlapsComparator(SegmentExtent extent) {
            this.extent.assign(extent);
        }

        public int compare(Object o1, Object o2) {
            throw new RuntimeException("Not supported.");
        }

        @Override
        public boolean equals(Object obj) {
            if (!(obj instanceof SegmentExtent)) {
                return false;
            }
            SegmentExtent se = (SegmentExtent) obj;
            extent.setSegmentId(se.getSegmentId());
            return ExtentPage.overlaps(se, extent);
        }
    }

    /**
     * Asynchronously extend the system file.
     */

    private class Extender extends Thread {

        private int pages;
        private boolean shutdown;
        private boolean recovered;
        private DatabaseException failed;

        Extender() {
            super("Cooldb - FileExtender");
        }

        synchronized void extend(int pages) throws DatabaseException {
            if (shutdown)
                throw new DatabaseException("System shutdown.");

            if (failed != null)
                throw failed;

            this.pages = Math.max(this.pages, pages);
            notifyAll();
        }

        synchronized void shutdown() {
            this.shutdown = true;
            notifyAll();
        }

        @Override
        public synchronized void run() {

            while (!shutdown) {
                try {
                    while (failed == null && !shutdown && pages == 0)
                        wait();

                    if (shutdown || failed != null)
                        return;

                    if (!recovered) {
                        pages -= recover();
                        recovered = true;
                    }

                    if (pages > 0) {
                        int fsize = sysFile.size();
                        try {
                            sysFile.extend(pages);
                        } catch (Error error) {
                            failed = new OutOfSpace("Failed to extend file.",
                                                    error);
                            return;
                        }
                        insertExtent(new Extent((short) 0, fsize, pages));
                    }

                    pages = 0;
                    notifyAll();
                } catch (Exception e) {
                    failed = new DatabaseException("Failed to extend file.", e);
                    break;
                }
            }
        }

        public synchronized void waitUntilDone() throws DatabaseException,
                InterruptedException {
            while (failed == null && !shutdown && pages > 0)
                wait();

            if (shutdown)
                throw new DatabaseException("System shutdown.");

            if (failed != null)
                throw failed;
        }

        private void insertExtent(Extent extent) throws RollbackException,
                TransactionCancelledException {
            // insert the new extent into the freeExtents method

            // This is a system transaction for file extension
            Transaction trans = transactionManager.beginTransaction();
            try {
                freeExtents.insertExtent(trans, extent);

                if (testFailure1)
                    throw new RuntimeException("TestFailure1");

                transactionManager.commitTransaction(trans);

                if (testFailure2)
                    throw new RuntimeException("TestFailure2");
            } catch (Exception e) {
                transactionManager.rollback(trans, SpaceManager.this);
                failed = new DatabaseException("Failed to allocate extent.", e);
                return;
            }

            // update the SystemKey to reflect the new file size
            systemKey.setSysFileSize(sysFile.size());
            systemKey.flush();
        }

        /**
         * Recover from failure during file extension by adding any extra pages
         * at the end of the file to the freeExtents segment.
         */
        private int recover() throws DatabaseException {
            try {
                int fsize = systemKey.getSysFileSize();
                int diffpages = sysFile.size() - fsize;

                if (diffpages > 0) {
                    // insert the extent if it is not already there
                    Extent extent = new Extent((short) 0, fsize, diffpages);
                    OverlapsComparator oc = new OverlapsComparator(
                            new SegmentExtent(new FilePage(), extent));

                    // check both freeExtents and usedExtents
                    if (freeExtents.exists(extent)
                            || usedExtents.findExtent(oc))
                        return 0;

                    insertExtent(extent);
                }

                return diffpages;
            } catch (Exception e) {
                throw new DatabaseException(
                        "Failed to recover from file extension failure.", e);
            }
        }
    }
}
