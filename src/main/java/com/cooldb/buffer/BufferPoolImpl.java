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

package com.cooldb.buffer;

import java.nio.ByteBuffer;
import java.util.*;

public class BufferPoolImpl implements BufferPool {

    private static final Comparator<Slot> slotComparator = new SlotComparator<>();
    // We must ensure room enough for the BeginCheckpoint record to be written
    private static final int CHECKPOINT_THRESHOLD = FileManager.DEFAULT_PAGE_SIZE * 2;
    private final Transactions transactions;
    // The following are not private to allow the unit tests access
    Map<FilePage, Slot> pageMap = new HashMap<>();
    Set<Slot> dirtySlots = new HashSet<>();
    LinkedList<Slot> hated = new LinkedList<>(); // Pages first-in-line
    // for replacement
    LinkedList<Slot> liked = new LinkedList<>(); // Pages next in line
    // for replacement
    LinkedList<Slot> loved = new LinkedList<>(); // Pages last in line
    int capacity;
    int maxCapacity;
    int allocated;
    private WriteAheadLogging writeAheadLogging;
    private FileManager fileManager;
    private DatabaseWriter databaseWriter;
    private byte[] zeroPage;
    private double dirtyPageRatio;

    public BufferPoolImpl(FileManager fileManager,
                          WriteAheadLogging writeAheadLogging, Transactions transactions) {
        this.fileManager = fileManager;
        zeroPage = new byte[fileManager.getPageSize()];
        dirtyPageRatio = .5;
        this.writeAheadLogging = writeAheadLogging;
        this.transactions = transactions;
    }

    public BufferPoolImpl(FileManager fileManager) {
        this(fileManager, null, null);
    }

    public void start() {
        if (databaseWriter == null) {
            databaseWriter = new DatabaseWriter(this);
            databaseWriter.start();
        }
    }

    public void stop() {
        if (databaseWriter != null) {
            databaseWriter.stop();
            databaseWriter = null;
        }
        pageMap.clear();
        dirtySlots.clear();
        hated.clear();
        liked.clear();
        loved.clear();

        pageMap = null;
        dirtySlots = null;
        hated = null;
        liked = null;
        loved = null;
        zeroPage = null;
        fileManager = null;
        writeAheadLogging = null;
    }

    public void setWriteAheadLogging(WriteAheadLogging writeAheadLogging) {
        this.writeAheadLogging = writeAheadLogging;
    }

    public void setDirtyPageRatio(double ratio) {
        dirtyPageRatio = ratio;
    }

    public void ensureCapacity(int capacity) {
        // only permit an increase in capacity upto the maximum
        if (capacity > this.capacity
                && (capacity <= maxCapacity || maxCapacity == 0))
            this.capacity = capacity;
    }

    public void setMaximumCapacity(int maxCapacity) {
        this.maxCapacity = maxCapacity;
    }

    public PageBuffer pin(FilePage page, Mode mode) throws BufferNotFound,
            InterruptedException {
        return pin(new VersionPage(page), mode, true, false);
    }

    public PageBuffer tryPin(FilePage page, Mode mode) throws BufferNotFound {
        try {
            return pin(new VersionPage(page), mode, false, false);
        } catch (InterruptedException e) {
            return null;
        }
    }

    public synchronized PageBuffer pinVersion(FilePage page, long transId,
                                              long version) throws BufferNotFound, InterruptedException {
        // If the version is already cached, pin it in mode SHARED
        VersionPage versionPage = new VersionPage(page, transId, version);
        if (isCached(versionPage))
            return pin(versionPage, Mode.SHARED, true, false);

        // Otherwise, associate a new buffer with the page in mode EXCLUSIVE
        VersionPage currentPage = new VersionPage(page);

        PageBuffer versionBuffer = pin(versionPage, Mode.EXCLUSIVE, true, true);

        // And copy the original page into the new buffer
        PageBuffer currentBuffer = pin(currentPage, Mode.SHARED, true, false);
        try {
            versionBuffer.getByteBuffer().put(currentBuffer.getByteBuffer());
        } finally {
            unPin(currentBuffer, Affinity.HATED);
        }

        // Return the buffer copy pinned in mode EXCLUSIVE
        versionBuffer.clear();
        return versionBuffer;
    }

    public PageBuffer pinNew(FilePage page) throws BufferNotFound,
            InterruptedException {
        PageBuffer pageBuffer = pin(new VersionPage(page), Mode.EXCLUSIVE,
                                    true, true);

        pageBuffer.put(zeroPage, 0, zeroPage.length);
        pageBuffer.rewind();

        return pageBuffer;
    }

    public PageBuffer pinTemp(FilePage page, long transId)
            throws BufferNotFound, InterruptedException {
        PageBuffer pageBuffer = pin(new VersionPage(page), Mode.EXCLUSIVE,
                                    true, true);

        Slot slot = pageBuffer.getSlot();
        pageBuffer.put(zeroPage, 0, zeroPage.length);
        pageBuffer.rewind();
        slot.setTransId(transId);
        slot.setDirty(true);

        return pageBuffer;
    }

    public void unPin(PageBuffer page, Affinity affinity) {
        unPin(page, affinity, 0, false);
    }

    public void unPinDirty(PageBuffer page, Affinity affinity, long endLSN) {
        unPin(page, affinity, endLSN, true);
    }

    public void unPinDirty(PageBuffer page, Affinity affinity) {
        unPin(page, affinity, 0, true);
    }

    private synchronized void unPin(PageBuffer page, Affinity affinity,
                                    long endLSN, boolean isDirty) {
        if (page == null)
            return;

        Slot slot = page.getSlot();

        // Make sure it is actually pinned first
        if (slot == null || !slot.isPinned())
            return;

        // Decrement pin count
        slot.subPin();

        /*
         * The unpinned slot's affinity should be changed based on its current
         * affinity and the affinity passed by the caller.
         */
        switch (slot.getAffinity()) {
            case HATED:
                // upgrade
                switch (affinity) {
                    case LIKED:
                        slot.setAffinity(Affinity.LIKED);
                        liked.addLast(slot);
                        break;
                    case LOVED:
                        slot.setAffinity(Affinity.LOVED);
                        loved.addLast(slot);
                        break;
                    default:
                        slot.setClocked(true);
                        break;
                }
                break;
            case LIKED:
                // downgrade: keep the same affinity, but age the buffer
                switch (affinity) {
                    case HATED:
                        liked.remove(slot);
                        liked.addFirst(slot);
                        slot.setClocked(false);
                        break;
                    // upgrade
                    case LOVED:
                        slot.setAffinity(Affinity.LOVED);
                        loved.addLast(slot);
                        break;
                    default:
                        slot.setClocked(true);
                        break;
                }
                break;
            case LOVED:
                // downgrade: keep the same affinity, but age the buffer
                if (affinity != Affinity.LOVED) {
                    loved.remove(slot);
                    loved.addFirst(slot);
                    slot.setClocked(false);
                } else
                    slot.setClocked(true);
                break;
            case NONE:
                slot.setAffinity(affinity);
                switch (affinity) {
                    case HATED:
                        hated.addLast(slot);
                        break;
                    case LIKED:
                        liked.addLast(slot);
                        break;
                    case LOVED:
                        loved.addLast(slot);
                        break;
                    default:
                        break;
                }
                break;
            default:
                break;
        }
        // Is the page dirty? If not, remove it from dirty pages list
        if (slot.getMode() == Mode.EXCLUSIVE) {
            if (!slot.isCopy()) {
                if (isDirty)
                    slot.setDirty(true);
                else if (!slot.isDirty())
                    dirtySlots.remove(slot);

                // Note the endLSN so we can enforce the WAL protocol
                slot.setEndLSN(endLSN);
            }
        }

        if (slot.isDirty())
            slot.setClocked(true);

        // Make sure this PageBuffer instance can no longer be used.
        page.invalidate();

        if (slot.waiting() && !slot.isPinned())
            notifyAll();
    }

    public DPEntry[] checkPoint() {
        // HashSet dirtyFiles = new HashSet();
        ArrayList<DPEntry> dirtyPages = new ArrayList<>();
        TreeSet<Slot> sortedDirtySlots = new TreeSet<>(slotComparator);

        // Take a snapshot of the dirty pages list
        synchronized (this) {
            sortedDirtySlots.addAll(dirtySlots);
        }

        /*
         * Flush out as many dirty pages as possible and return the set of
         * remaining dirty pages
         */
        for (Slot slot : sortedDirtySlots) {
            synchronized (this) {
                flushSlot(slot);
                if (slot.isDirty())
                    dirtyPages.add(new DPEntry(slot.getPage(), slot.getRecLSN()));
            }
        }

        return dirtyPages.toArray(new DPEntry[0]);
    }

    synchronized boolean isCached(FilePage page) {
        return pageMap.containsKey(page);
    }

    private synchronized PageBuffer pin(VersionPage page, Mode mode,
                                        boolean isWaitOK, boolean isNew) throws BufferNotFound,
            InterruptedException {
        Slot slot;

        // If page is not already in memory, find a replacement buffer and page
        // it in
        if ((slot = pageMap.get(page)) == null) {
            // Find and lock a slot buffer to replace with the requested page
            slot = selectReplacement();

            // Write out contents of the replaced buffer if it is dirty and
            // enforce the WAL protocol
            flushSlot(slot);

            // Remove slot from pageMap, assign it the requested page, then
            // reenter into pageMap
            pageMap.remove(slot.getPage());
            slot.setPage((VersionPage) page.copy());
            slot.setAffinity(Affinity.NONE);
            pageMap.put(slot.getPage(), slot);

            // Read in page
            if (!slot.isCopy() && !isNew) {
                fileManager.fetch(slot.getPage(), slot.getBuffer());
            }
        }
        return pinSlot(slot, mode, isWaitOK);
    }

    private void flushSlot(Slot slot) {
        /*
         * Write out contents of the replaced buffer if it is dirty and enforce
         * the WAL protocol. Also make sure that the slot is not currently
         * pinned in Mode.EXCLUSIVE, that the slot is not a version copy, and
         * that the slot does not contain the temporary updates of an already
         * committed transaction (in which case they can simply be discarded).
         */
        if (slot.isDirty()
                && !(slot.isPinned() && slot.getMode() == Mode.EXCLUSIVE)
                && !slot.isCopy()
                && !(slot.getTransId() > 0 && transactions.isCommitted(slot
                                                                               .getTransId()))) {
            if (writeAheadLogging != null)
                writeAheadLogging.flushTo(slot.getEndLSN());

            // Write buffer without forcing file sync
            slot.flush();

            slot.setDirty(false);

            // Remove from dirtySlots
            dirtySlots.remove(slot);
        }
    }

    private PageBuffer pinSlot(Slot slot, Mode mode, boolean isWaitOK)
            throws InterruptedException {
        if (slot.isPinned()
                && (slot.getMode() == Mode.EXCLUSIVE || mode == Mode.EXCLUSIVE)) {
            // Wait unless the caller has specified a NOWAIT condition
            if (!isWaitOK)
                return null;

            try {
                slot.addWaiter();
                do {
                    wait();
                } while (slot.isPinned());
            } finally {
                slot.subWaiter();
            }
        }
        slot.setMode(mode);

        // Enter into the dirty pages table if mode is EXCLUSIVE and it is not
        // already dirty
        if (mode == Mode.EXCLUSIVE) {
            if (slot.isDirty()) {
                /*
                 * Make sure we flush updates that are still resident, perhaps
                 * due to an extremely hot page, that are older than
                 * CHECKPOINT_THRESHOLD, in order to allow subsequent
                 * checkpoints to reclaim log space and to keep restart recovery
                 * times low
                 */
                if (writeAheadLogging != null) {
                    if (writeAheadLogging.getRemaining(slot.getRecLSN()) < CHECKPOINT_THRESHOLD)
                        flushSlot(slot);
                }
            }
            if (!slot.isDirty()) {
                if (!slot.isCopy()) {
                    // The recLSN is the starting point in the log at which
                    // updates to this page might occur
                    if (writeAheadLogging != null) {
                        slot.setRecLSN(writeAheadLogging.getEndOfLog());
                    }
                    dirtySlots.add(slot);

                    /*
                     * If the number of dirty pages is greater than half the
                     * number of allocated buffers, then initiate a buffer pool
                     * checkpoint to run asynchronously.
                     */
                    if (databaseWriter != null
                            && (dirtySlots.size() * 1.0 / capacity) > dirtyPageRatio) {
                        databaseWriter.takeCheckpoint();
                    }
                }
            }
        }

        // Increment pin count
        slot.addPin();

        ByteBuffer buffer = mode == Mode.EXCLUSIVE ? slot.getBuffer() : slot
                .getBuffer().asReadOnlyBuffer();
        buffer.clear();

        return new PageBuffer(slot, buffer);
    }

    /**
     * Look first among unallocated slots, then hated pages, then among the
     * liked, and finally the loved. If no buffer slots are available (because
     * all of them are pinned), then increase capacity.
     */
    private Slot selectReplacement() throws BufferNotFound {
        Slot slot;
        if (((slot = allocateSlot()) == null)
                && ((slot = selectReplacement(hated, Affinity.HATED)) == null)
                && ((slot = selectReplacement(liked, Affinity.LIKED)) == null)
                && ((slot = selectReplacement(loved, Affinity.LOVED)) == null)) {
            // No slots available. All pages are pinned. Increase capacity if
            // possible.
            if (capacity < maxCapacity || maxCapacity == 0) {
                ensureCapacity(capacity + 1);
                return selectReplacement();
            } else
                throw new BufferNotFound(
                        "No buffers available.  Try increasing buffer pool capacity or limiting the number of " +
                                "simultaneous transactions.");
        }
        return slot;
    }

    private Slot selectReplacement(LinkedList<Slot> list, Affinity affinity) {
        Slot slot;
        Slot lastResort = null;
        int size = list.size();

        while (size-- > 0) {
            slot = list.removeFirst();

            // Remove slots that have been upgraded to a stronger affinity list
            if (slot.getAffinity() != affinity)
                continue;

            // If the slot is in use, add to the end of list and get the next
            // one in line
            if (slot.inUse()) {
                list.addLast(slot);
                continue;
            }

            // Give clocked slots another chance; use the first one encountered
            // as a last resort
            if (slot.isClocked()) {
                slot.setClocked(false);
                if (lastResort == null)
                    lastResort = slot;
                else
                    list.addLast(slot);
                continue;
            }

            // Put the lastResort back on the list since we do not need it
            if (lastResort != null)
                list.addLast(lastResort);

            return slot;
        }

        return lastResort;
    }
    // for replacement

    private Slot allocateSlot() {
        Slot slot = null;
        if (allocated < capacity) {
            slot = new Slot();
            slot.setBuffer(ByteBuffer.allocate(fileManager.getPageSize()));
            ++allocated;
        }
        return slot;
    }

    // JUnit testing use only
    public void stopDatabaseWriter() {
        if (databaseWriter != null) {
            databaseWriter.stop();
            databaseWriter = null;
        }
    }

    static class SlotComparator<T> implements Comparator<T> {
        public int compare(T o1, T o2) {
            return ((Slot) o1).getPage().compareTo(((Slot) o2).getPage());
        }
    }

    class Slot {
        private VersionPage page;
        private ByteBuffer buffer;
        private Mode mode;
        private Affinity affinity = Affinity.NONE;
        /**
         * The recLSN is the starting point in the log at which updates to this
         * page might occur, for recovery purposes.
         */
        private long recLSN;
        private long endLSN;
        private boolean isDirty;
        private boolean isClocked;
        private short waiters;
        private short pinned;
        private long transId;

        FilePage getPage() {
            return page;
        }

        void setPage(VersionPage page) {
            this.page = page;
        }

        ByteBuffer getBuffer() {
            return buffer;
        }

        void setBuffer(ByteBuffer buffer) {
            this.buffer = buffer;
        }

        Mode getMode() {
            return mode;
        }

        void setMode(Mode mode) {
            this.mode = mode;
        }

        Affinity getAffinity() {
            return affinity;
        }

        void setAffinity(Affinity affinity) {
            this.affinity = affinity;
        }

        long getEndLSN() {
            return endLSN;
        }

        void setEndLSN(long endLSN) {
            this.endLSN = endLSN;
        }

        long getRecLSN() {
            return recLSN;
        }

        void setRecLSN(long recLSN) {
            this.recLSN = recLSN;
        }

        long getTransId() {
            return transId;
        }

        void setTransId(long transId) {
            this.transId = transId;
        }

        boolean isDirty() {
            return isDirty;
        }

        void setDirty(boolean isDirty) {
            this.isDirty = isDirty;
        }

        boolean isClocked() {
            return isClocked;
        }

        void setClocked(boolean isClocked) {
            this.isClocked = isClocked;
        }

        void addWaiter() {
            ++waiters;
        }

        void subWaiter() {
            --waiters;
        }

        void addPin() {
            ++pinned;
        }

        void subPin() {
            --pinned;
        }

        boolean isPinned() {
            return pinned > 0;
        }

        boolean waiting() {
            return waiters > 0;
        }

        boolean inUse() {
            return pinned > 0 || waiters > 0;
        }

        @SuppressWarnings("BooleanMethodIsAlwaysInverted")
        boolean isCopy() {
            return page.isCopy();
        }

        synchronized void flush() {
            fileManager.flush(page, buffer, false);
        }
    }
}
