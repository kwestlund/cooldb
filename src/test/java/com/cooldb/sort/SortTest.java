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

package com.cooldb.sort;

import com.cooldb.api.*;
import com.cooldb.api.impl.RowImpl;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FileManager;
import com.cooldb.core.Column;
import com.cooldb.core.Core;
import com.cooldb.core.SortManager;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.segment.SpaceManager;
import com.cooldb.storage.*;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;
import org.junit.After;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.*;

public class SortTest implements SortDelegate {
    private final static int BUFFER_POOL_SIZE = 1000;
    private final static int NRECS = 100000;
    private final static int NRECS_LARGE = NRECS * 10;

    private void setUp(int size) throws DatabaseException {
        System.out.println("Creating Database: Inititial Size: " + size + "M");

        System.gc();
        File file = new File("build/tmp/cooldb");
        file.mkdirs();

        core = new Core(new File("build/tmp/cooldb"));
        core.createDatabase(true, (size * 1024 * 1024)
                / FileManager.DEFAULT_PAGE_SIZE);

        top = false;
        distinct = false;
        unique = false;
    }

    @After
    public void tearDown() {
        core.destroyDatabase();
        core = null;

        System.gc();
    }

    /**
     * Test sort with 2 records.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testSimpleSort() throws DatabaseException, InterruptedException {
        setUp(1);
        _testSimpleSort(false);
        _testSimpleSort(true);
    }

    /**
     * Test sort with 2 records.
     *
     * @throws InterruptedException
     * @throws DatabaseException
     * @throws TypeException
     */
    private void _testSimpleSort(boolean desc) throws InterruptedException,
            TypeException, DatabaseException {
        TransactionManager tm = core.getTransactionManager();
        SortManager sm = core.getSortManager();
        Sort sort = new Sort(sm);
        Row row1, row2, row3, row4, row5, row6;

        Transaction trans = tm.beginTransaction();

        // String
        Row proto;
        ArrayRowStream recs = new ArrayRowStream(proto = new TestRow("s"));
        if (desc) {
            proto.setDirection(0, Direction.DESC);
        }
        row1 = recs.allocateRow();
        row2 = recs.allocateRow();

        row1.setString(0, "World");
        row2.setString(0, "Hello");

        recs.add(row1);
        recs.add(row2);

        distinct = false;
        unique = false;
        RowStream rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 2);
        rs.close();

        // byte
        recs = new ArrayRowStream(proto = new TestRow("b"));
        if (desc) {
            proto.setDirection(0, Direction.DESC);
        }
        row1 = recs.allocateRow();
        row2 = recs.allocateRow();
        row3 = recs.allocateRow();
        row4 = recs.allocateRow();

        row1.setByte(0, (byte) 1);
        row2.setByte(0, (byte) -1);
        row3.setByte(0, (byte) -127);
        row4.setByte(0, (byte) 127);

        recs.add(row1);
        recs.add(row2);
        recs.add(row3);
        recs.add(row4);

        distinct = false;
        unique = false;
        rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 4);
        rs.close();

        // short
        recs = new ArrayRowStream(proto = new TestRow("i"));
        if (desc) {
            proto.setDirection(0, Direction.DESC);
        }
        row1 = recs.allocateRow();
        row2 = recs.allocateRow();
        row3 = recs.allocateRow();

        row1.setShort(0, (short) 1);
        row2.setShort(0, (short) -1);
        row3.setShort(0, (short) -256);

        recs.add(row1);
        recs.add(row2);
        recs.add(row3);

        rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 3);
        rs.close();

        // integer
        recs = new ArrayRowStream(proto = new TestRow("I"));
        if (desc) {
            proto.setDirection(0, Direction.DESC);
        }
        row1 = recs.allocateRow();
        row2 = recs.allocateRow();
        row3 = recs.allocateRow();

        row1.setInt(0, 1);
        row2.setInt(0, -1);
        row3.setInt(0, -256);

        recs.add(row1);
        recs.add(row2);
        recs.add(row3);

        rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 3);
        rs.close();

        // long
        recs = new ArrayRowStream(proto = new TestRow("l"));
        if (desc) {
            proto.setDirection(0, Direction.DESC);
        }
        row1 = recs.allocateRow();
        row2 = recs.allocateRow();
        row3 = recs.allocateRow();

        row1.setLong(0, 1);
        row2.setLong(0, -1);
        row3.setLong(0, -256);

        recs.add(row1);
        recs.add(row2);
        recs.add(row3);

        rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 3);
        rs.close();

        // double
        recs = new ArrayRowStream(proto = new TestRow("d"));
        if (desc) {
            proto.setDirection(0, Direction.DESC);
        }
        row1 = recs.allocateRow();
        row2 = recs.allocateRow();
        row3 = recs.allocateRow();

        row1.setDouble(0, .100000000003333333333);
        row2.setDouble(0, 0.0);
        row3.setDouble(0, -.256000000000000000);

        byte[] buf = new byte[100];
        ((TestRow) row1).writeSortable(buf, 0);

        recs.add(row1);
        recs.add(row2);
        recs.add(row3);

        rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 3);
        rs.close();

        recs = new ArrayRowStream(proto = new TestRow("ss"));
        if (desc) {
            proto.setDirection(0, Direction.DESC);
            proto.setDirection(1, Direction.DESC);
        }
        row1 = recs.allocateRow();
        row2 = recs.allocateRow();
        row3 = recs.allocateRow();
        row4 = recs.allocateRow();
        row5 = recs.allocateRow();
        row6 = recs.allocateRow();

        row1.setString(0, "Westlund");
        row1.setNull(1, true);
        row2.setString(0, "");
        row2.setString(1, "Westlund");
        row3.setString(0, "Westl");
        row3.setString(1, "und");
        row4.setString(0, "WestlundKen");
        row4.setString(1, "");
        row5.setString(0, "Westlund");
        row5.setString(1, "Ken");
        row6.setString(0, "WestlundKen");
        row6.setString(1, "Ken");

        recs.add(row1);
        recs.add(row2);
        recs.add(row3);
        recs.add(row4);
        recs.add(row5);
        recs.add(row6);

        rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 6);
        rs.close();

        tm.commitTransaction(trans);
    }

    /**
     * Test projection of key columns.
     *
     * @throws InterruptedException
     * @throws DatabaseException
     */
    @Test
    public void testKeyDataSeparation() throws InterruptedException,
            DatabaseException {
        setUp(1);
        TransactionManager tm = core.getTransactionManager();
        SortManager sm = core.getSortManager();
        Sort sort = new Sort(sm);
        TestRow row = new TestRow("ssi");
        Row row1 = (Row) row.copy();
        Row row2 = (Row) row.copy();
        Row row3 = (Row) row.copy();

        row1.setString(0, "D1");
        row2.setString(0, "D2");
        row3.setString(0, "D3");
        row1.setString(1, "A");
        row2.setString(1, "A");
        row3.setString(1, "A");
        row1.setInt(2, 3);
        row2.setInt(2, 2);
        row3.setInt(2, 1);

        byte[] pcols = new byte[]{1, 2, 0};
        Row p1 = row1.project(pcols, 2);
        Row p2 = row2.project(pcols, 2);
        Row p3 = row3.project(pcols, 2);

        Row p = row.project(pcols, 2);
        ArrayRowStream recs = new ArrayRowStream(p);
        recs.add(p1);
        recs.add(p2);
        recs.add(p3);

        Transaction trans = tm.beginTransaction();

        RowStream rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 3);
        rs.close();

        tm.commitTransaction(trans);
    }

    /**
     * Test removal of duplicate records during sort for small datasets.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testDistinctSmall() throws DatabaseException,
            InterruptedException {
        setUp(1);
        distinct = true;
        unique = true;
        testSmall();
    }

    /**
     * Test removal of duplicate records during sort for large datasets.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testDistinctLarge() throws DatabaseException,
            InterruptedException {
        setUp(100);
        distinct = true;
        unique = true;
        testLarge();
    }

    /**
     * Test unique constraint during sort for small datasets.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testUniqueSmall() throws DatabaseException,
            InterruptedException {
        setUp(1);
        distinct = false;
        unique = true;
        try {
            testSmall();
            fail("Expected UniqueConstraintException");
        } catch (UniqueConstraintException e) {
            // expected
        }
    }

    /**
     * Test unique constraint during sort for large datasets.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testUniqueLarge() throws DatabaseException,
            InterruptedException {
        setUp(100);
        distinct = false;
        unique = true;
        try {
            testLarge();
            fail("Expected UniqueConstraintException");
        } catch (UniqueConstraintException e) {
            // expected
        }
    }

    /**
     * Test top-n constraint during sort for small datasets.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testTopSmall() throws DatabaseException, InterruptedException {
        setUp(1);
        distinct = false;
        unique = false;
        top = true;
        testSmall();
    }

    /**
     * Test top-n constraint during sort for large datasets.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    @Test
    public void testTopLarge() throws DatabaseException, InterruptedException {
        setUp(100);
        distinct = false;
        unique = false;
        top = true;
        testLarge();
    }

    private void testSmall() throws InterruptedException, TypeException,
            DatabaseException {
        TransactionManager tm = core.getTransactionManager();
        SortManager sm = core.getSortManager();
        Sort sort = new Sort(sm);
        TestRow row = new TestRow("sss");
        Row row1 = (Row) row.copy();
        Row row2 = (Row) row.copy();
        Row row3 = (Row) row.copy();

        row1.setString(0, "D1");
        row2.setString(0, "D2");
        row3.setString(0, "D3");
        row1.setString(1, "A");
        row2.setString(1, "A");
        row3.setString(1, "A");
        row1.setString(2, "K2");
        row2.setString(2, "K2");
        row3.setString(2, "K1");

        // project the key as colums 1 and 2, but not 0
        byte[] pcols = new byte[]{1, 2, 0};
        Row p = row.project(pcols, 2);
        Row p1 = row1.project(pcols, 2);
        Row p2 = row2.project(pcols, 2);
        Row p3 = row3.project(pcols, 2);

        ArrayRowStream recs = new ArrayRowStream(p);
        recs.add(p1);
        recs.add(p2);
        recs.add(p3);

        Transaction trans = tm.beginTransaction();

        RowStream rs = sort.sort(trans, recs, this);
        print(trans, rs);
        validate(trans, rs, 2); // only 2 records left after one removed
        rs.close();

        tm.commitTransaction(trans);
    }

    /**
     * Test unique constraint during sort for large datasets.
     *
     * @throws DatabaseException
     * @throws InterruptedException
     */
    private void testLarge() throws DatabaseException, InterruptedException {
        core.setBufferPoolSize(BUFFER_POOL_SIZE);
        TransactionManager tm = core.getTransactionManager();
        SortManager sm = core.getSortManager();
        Sort sort = new Sort(sm);
        int nrecs = NRECS_LARGE;
        Transaction trans = tm.beginTransaction();
        int mod = 1000;
        RowStream recs = createDatasetForDistinctTest(trans, nrecs, mod);
        tm.commitTransaction(trans);
        RowStream rs = null;
        long total = 0;
        int ntimes = 1;
        System.out.println("Sort: Repeating sort " + ntimes + " times:");
        for (int i = 1; i <= ntimes; i++) {
            trans = tm.beginTransaction();
            recs.rewind();
            long now = System.currentTimeMillis();
            System.out.println("Sort " + i + ": Sorting " + nrecs
                                       + " records");
            rs = sort.sort(trans, recs, this);
            long end = System.currentTimeMillis();
            total += end - now;
            System.out.println("Sort " + i + ": Sort Time: " + (end - now)
                    / 1000.0 + " seconds");
            System.out.println("Sort " + i + ": Validating");
            if (top) {
                validate(trans, rs, 2);
                print(trans, rs);
            } else
                validate(trans, rs, mod);
            System.out.println("Sort " + i + ": Done");
            rs.close();
            tm.commitTransaction(trans);
        }
        System.out.println("Sort: Average Sort Time: " + total / ntimes
                / 1000.0 + " seconds");
    }

    /**
     * Create some temporary segments, then run the recovery process, then
     * verify they are removed.
     *
     * @throws InterruptedException
     * @throws DatabaseException
     */
    @Test
    public void testTempSegmentRecovery() throws InterruptedException,
            DatabaseException {
        setUp(1);
        TransactionManager tm = core.getTransactionManager();
        SegmentFactory sf = core.getSegmentFactory();
        SpaceManager spm = core.getSpaceManager();
        SortManager sm = core.getSortManager();
        Transaction trans = tm.beginTransaction();

        sm.createTemp(trans, 100);
        sm.createTemp(trans, 200);
        Segment segment = spm.createSegment(trans);
        sf.createSegmentMethod(trans, segment, Dataset.class);
        sm.createTemp(trans, 300);
        sm.createTemp(trans, 400);

        sm.recover(trans);

        final byte tempSegmentType = sf.getSegmentType(TempStorageMethod.class);
        Filter filter = new Filter() {
            public boolean passes(Object o) {
                return ((Segment) o).getSegmentType() == tempSegmentType;
            }
        };
        int i = 0;
        while ((i = sf.selectSegment(segment, i, filter)) != -1) {
            throw new DatabaseException(
                    "Found temp segment, but was expecting none.");
        }

        tm.commitTransaction(trans);
    }

    /**
     * Create a Dataset and insert many objects, then sort them and verify the
     * output is sorted.
     *
     * @throws InterruptedException
     * @throws DatabaseException
     */
    @Test
    public void testExerciseSort() throws InterruptedException,
            DatabaseException {
        setUp(100);
        core.setBufferPoolSize(BUFFER_POOL_SIZE);
        TransactionManager tm = core.getTransactionManager();
        SortManager sm = core.getSortManager();
        Sort sort = new Sort(sm);
        int nrecs = 1000000;
        Transaction trans = tm.beginTransaction();
        RowStream recs = createRandomDataset(trans, nrecs);
        tm.commitTransaction(trans);
        RowStream rs = null;
        long total = 0;
        int ntimes = 10;
        System.out.println("Sort: Repeating sort " + ntimes + " times:");
        for (int i = 1; i <= ntimes; i++) {
            trans = tm.beginTransaction();
            recs.rewind();
            long now = System.currentTimeMillis();
            System.out.println("Sort " + i + ": Sorting " + nrecs + " records");
            distinct = false;
            unique = false;
            // fail uniqueness on the first run, just to make sure we
            // recover properly
            if (i == 1)
                unique = true;
            try {
                rs = sort.sort(trans, recs, this);
                long end = System.currentTimeMillis();
                total += end - now;
                System.out.println("Sort " + i + ": Sort Time: " + (end - now)
                        / 1000.0 + " seconds");
                System.out.println("Sort " + i + ": Validating");
                validate(trans, rs, nrecs);
                System.out.println("Sort " + i + ": Done");
                rs.close();
                tm.commitTransaction(trans);
                assertFalse("Should not be unique", unique);
            } catch (UniqueConstraintException uce) {
                assertTrue("Should be unique", unique);
            }
        }
        System.out.println("Sort: Average Sort Time: " + total / ntimes
                / 1000.0 + " seconds");
    }

    /**
     * Create a random Dataset with the specified number of rows.
     */
    private RowStream createRandomDataset(Transaction trans, int nrecs)
            throws DatabaseException {
        System.out.println("Sort: Creating Dataset: " + nrecs
                                   + " records of type: long, long, long, string(12)");

        SpaceManager sm = core.getSpaceManager();
        SegmentFactory sf = core.getSegmentFactory();

        Segment segment = sm.createSegment(trans);
        byte[] pcols = new byte[]{0, 1, 2, 3};
        Row row = (new TestRow("IIIs")).project(pcols, 2);
        TempDataset ds = (TempDataset) sf.createSegmentMethod(trans, segment,
                                                              TempDataset.class);

        ds.beginInsert(trans);
        for (int i = 0; i < nrecs; i++) {
            // make sure it can't be unique
            if (i == 100 || i == 30000) {
                row.setInt(0, 33);
                row.setInt(1, 33);
            } else {
                row.setInt(0, random(1000000000));
                row.setInt(1, random(1000000000));
            }
            row.setInt(2, random(1000000000));
            row.setString(3, "fillerfiller");
            ds.insert(trans, row);
        }
        ds.endInsert(trans);

        return new StorageMethodIterator(ds, trans, row);
    }

    /**
     * Create a Dataset with the specified number of rows in which every mod
     * rows is a duplicate.
     */
    private RowStream createDatasetForDistinctTest(Transaction trans, int nrecs,
                                                   int mod) throws DatabaseException {
        System.out.println("Sort: Creating Dataset: " + nrecs
                                   + " records of type: long, long, long, string(12)");

        SpaceManager sm = core.getSpaceManager();
        SegmentFactory sf = core.getSegmentFactory();

        Segment segment = sm.createSegment(trans);
        byte[] pcols = new byte[]{0, 1, 2, 3};
        Row row = (new TestRow("IIIs")).project(pcols, 1);
        TempDataset ds = (TempDataset) sf.createSegmentMethod(trans, segment,
                                                              TempDataset.class);

        ds.beginInsert(trans);
        for (int i = 0; i < nrecs; i++) {
            row.setInt(0, i % mod);
            row.setInt(1, random(1000000000));
            row.setInt(2, random(1000000000));
            row.setString(3, "fillerfiller");
            ds.insert(trans, row);
        }
        ds.endInsert(trans);

        return new StorageMethodIterator(ds, trans, row);
    }

//	private void dropDataset(Transaction trans, Dataset ds)
//			throws DatabaseException {
//		SpaceManager sm = core.getSpaceManager();
//		SegmentFactory sf = core.getSegmentFactory();
//
//		sm.dropSegment(trans, ds.getSegment());
//		sf.removeSegmentMethod(trans, ds);
//	}

    private void print(Transaction trans, RowStream rs)
            throws DatabaseException {
        rs.rewind();
        Row row = rs.allocateRow();
        System.out.println("Result Set:");
        while (rs.fetchNext(row)) {
            System.out.println(row.toString());
        }
    }

    private void validate(Transaction trans, RowStream rs, int nrecs)
            throws DatabaseException {
        rs.rewind();
        Row prev = rs.allocateRow();
        Row row = rs.allocateRow();
        Row tmp;
        if (nrecs > 0 && !rs.fetchNext(prev))
            fail("Unexpected empty result set");
        int count = 1;
        while (rs.fetchNext(row)) {
            assertTrue(row.compareKeyTo(prev) >= 0);
            count++;
            // swap prev and next
            tmp = prev;
            prev = row;
            row = tmp;
        }
        assertTrue(count == nrecs);
    }

    private int random(int n) {
        return (int) (Math.random() * n);
    }

    public class TestRow extends RowImpl {
        public TestRow(String columnTypes) throws DatabaseException,
                TypeException {
            super(Column.createColumns(columnTypes));
        }

        public TestRow(TestRow row) {
            super(row);
        }

        @Override
        public Row copyRow() {
            return new TestRow(this);
        }

        // DBObject implementation
        @Override
        public DBObject copy() {
            return new TestRow(this);
        }
    }

    @Override
    public boolean isDistinct() {
        return distinct;
    }

    @Override
    public boolean isUnique() {
        return unique;
    }

    @Override
    public long topN() {
        return top ? 2 : 0;
    }

    Core core;
    int size;
    boolean distinct;
    boolean unique;
    boolean top;
}
