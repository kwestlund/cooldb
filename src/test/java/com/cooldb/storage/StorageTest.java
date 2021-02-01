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

package com.cooldb.storage;

import com.cooldb.api.TypeException;
import com.cooldb.api.Varchar;
import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.buffer.DBObject;
import com.cooldb.core.Core;
import com.cooldb.core.VarcharColumn;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.segment.SpaceManager;
import com.cooldb.transaction.DeadlockException;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class StorageTest {

	@Before
	public void setUp() throws DatabaseException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();

		core = new Core(new File("build/tmp/cooldb"));
		core.createDatabase(true);
		tmprec = new Record(0);
	}

	@After
	public void tearDown() {
		core.destroyDatabase();
		core = null;

		System.gc();
	}

	/**
	 * Create a Dataset and insert an object.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testInsert() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Dataset ds;
		long now = System.currentTimeMillis();

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Record rec = new Record(0);
		ds = (Dataset) sf.createSegmentMethod(trans, segment, Dataset.class);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();

		for (int i = 0; i < 100000; i++) {
			if (i % 100 == 0) {
				tm.commitTransaction(trans);
				trans = tm.beginTransaction();
			}
			rec.setValue(i);
			ds.insert(trans, rec);
		}

		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);

		tm.commitTransaction(trans);

		System.out
				.println("Time: " + (System.currentTimeMillis() - now) / 1000);
	}

	/**
	 * Create a Dataset and insert an object.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testTempDataset() throws DatabaseException,
			InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		TempDataset ds;
		long now = System.currentTimeMillis();

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Record rec = new Record(0);
		ds = (TempDataset) sf.createSegmentMethod(trans, segment,
				TempDataset.class);

		ds.beginInsert(trans);
		for (int i = 0; i < 10000; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		ds.endInsert(trans);

		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);

		tm.commitTransaction(trans);

		System.out
				.println("Time: " + (System.currentTimeMillis() - now) / 1000);
	}

	/**
	 * Create a Dataset, insert an object, then select it.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testSelect() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Dataset ds;

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Record rec = new Record(3);
		ds = (Dataset) sf.createSegmentMethod(trans, segment, Dataset.class);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();

		// insert
		Rowid rowid = ds.insert(trans, rec);

		for (int i = 0; i < 10000; i++) {
			if (i % 1 == 0) {
				tm.commitTransaction(trans);
				trans = tm.beginTransaction();
			}
			rec.setValue(0);
			ds.fetch(trans, rec, rowid);
			assertTrue(rec.getValue() == 3);
			rec.validate();
		}

		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);

		tm.commitTransaction(trans);
	}

	/**
	 * Create a Dataset, insert an object, then update it.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testUpdate() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Dataset ds;

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Record rec = new Record(3);
		ds = (Dataset) sf.createSegmentMethod(trans, segment, Dataset.class);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();

		// insert
		Rowid rowid = ds.insert(trans, rec);

		// update repeatedly
		for (int i = 0; i < 10000; i++) {
			if (i % 1 == 0) {
				tm.commitTransaction(trans);
				trans = tm.beginTransaction();
			}
			rec.setValue(i);
			ds.update(trans, rec, rowid, tmprec);

			rec.setValue(0);
			ds.fetch(trans, rec, rowid);
			assertTrue(rec.getValue() == i);
			rec.validate();
		}

		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);

		tm.commitTransaction(trans);
	}

	/**
	 * Create a Dataset, insert an object, then delete it.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testDelete() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Dataset ds;

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Record rec = new Record(3);
		ds = (Dataset) sf.createSegmentMethod(trans, segment, Dataset.class);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();

		// insert
		Rowid rowid = ds.insert(trans, rec);

		// delete
		assertTrue(ds.fetch(trans, rec, rowid));
		assertTrue(ds.remove(trans, rowid, tmprec));
		assertFalse(ds.fetch(trans, rec, rowid));
		assertFalse(ds.remove(trans, rowid, tmprec));

		tm.commitTransaction(trans);

		for (int j = 0; j < 10; j++) {
			trans = tm.beginTransaction();

			// insert a bunch of rows
			Rowid[] rows = new Rowid[10000];
			for (int i = 0; i < rows.length; i++) {
				if (i % 1 == 0) {
					tm.commitTransaction(trans);
					trans = tm.beginTransaction();
				}
				rec.setValue(i);
				rows[i] = ds.insert(trans, rec);
			}

			tm.commitTransaction(trans);
			trans = tm.beginTransaction();

			// now delete most of them
			for (int i = 0; i < rows.length; i++) {
				if (i % 1 == 0) {
					tm.commitTransaction(trans);
					trans = tm.beginTransaction();
				}
				if (i % 20 != 0)
					ds.remove(trans, rows[i], tmprec);
			}

			tm.commitTransaction(trans);
		}

		trans = tm.beginTransaction();
		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);
		tm.commitTransaction(trans);
	}

	/**
	 * Create a Dataset, insert objects, then scan for some in particular using
	 * a filter.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testScan() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Dataset ds;

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Record rec = new Record(0);
		ds = (Dataset) sf.createSegmentMethod(trans, segment, Dataset.class);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();

		for (int i = 0; i < 10000; i++) {
			if (i % 1 == 0) {
				tm.commitTransaction(trans);
				trans = tm.beginTransaction();
			}
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		DatasetCursor cursor = new DatasetCursor();
		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, 10000, true);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);
		tm.commitTransaction(trans);
	}

	/**
	 * Create a Dataset, insert objects in one transaction, then open a cursor
	 * and scan for them, then insert some more and scan for the new ones (the
	 * first set should be found, the second set should not be found), then scan
	 * for them in another transaction prior to committing the first
	 * transaction, then scan for them again after committing the first
	 * transaction, then reopen the cursor following the commit and scan again
	 * (in no case should the second transaction see the first transaction's
	 * data). Open a third transaction and scan for the data, which should be
	 * found. Try similar tests with updates & deletes.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testVersioning() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Transaction t2;
		Dataset ds;

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Record rec = new Record(0);
		ds = (Dataset) sf.createSegmentMethod(trans, segment, Dataset.class);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();

		// insert a bunch of records
		for (int i = 0; i < 10000; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}

		// scan in current transaction
		DatasetCursor cursor = new DatasetCursor();
		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, 10000, true);

		// scan using second transaction
		t2 = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		scan(t2, cursor, ds, 10000, false);

		for (int i = 10000; i < 20000; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		scan(trans, cursor, ds, 20000, false);
		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, 20000, true);

		tm.commitTransaction(trans);

		ds.openCursor(trans, cursor);
		scan(t2, cursor, ds, 20000, false);

		tm.commitTransaction(t2);

		// scan again using third transaction
		t2 = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		scan(t2, cursor, ds, 20000, true);
		tm.commitTransaction(t2);

		// update all of the records
		trans = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			rec.setValue(333);
			ds.update(trans, rec, cursor.getRowid(), tmprec);
		}

		// scan in current transaction
		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, 20000, false);
		ds.rewind(cursor);
		scan2(trans, cursor, ds, 333, true);
		ds.rewind(cursor);

		// scan using second transaction
		t2 = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		scan(t2, cursor, ds, 20000, true);

		// commit
		tm.commitTransaction(trans);
		tm.commitTransaction(t2);

		// scan again using third transaction
		t2 = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		scan(t2, cursor, ds, 20000, false);
		ds.rewind(cursor);
		scan2(t2, cursor, ds, 333, true);
		tm.commitTransaction(t2);

		// delete all of the records
		trans = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			ds.remove(trans, cursor.getRowid(), tmprec);
		}

		// scan in current transaction
		ds.openCursor(trans, cursor);
		scan2(trans, cursor, ds, 333, false);

		// scan using second transaction
		t2 = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		scan2(t2, cursor, ds, 333, true);

		// commit
		tm.commitTransaction(trans);
		tm.commitTransaction(t2);

		// scan again using third transaction
		t2 = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		scan2(t2, cursor, ds, 333, false);
		tm.commitTransaction(t2);

		trans = tm.beginTransaction();
		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);
		tm.commitTransaction(trans);
	}

	/**
	 * Test update conflicts between transactions using both isolation levels.
	 * First, using the default isolation level READ_COMMITTED, start a
	 * transaction, create a dataset, insert a row, then in a separate thread
	 * create a second transaction and attempt to update the new row. In the
	 * first thread, check that the second transaction is suspended. In a third
	 * thread, create a third transaction and set its isolation level to
	 * SERIALIZABLE, then attempt to update the same row and verify that an
	 * exception is thrown.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testConflict() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Dataset ds = (Dataset) sf.createSegmentMethod(trans, segment,
				Dataset.class);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();

		// insert a record
		Record rec = new Record(0);
		Rowid rowid = ds.insert(trans, rec);

		ConflictThread t2 = new ConflictThread(tm, ds, rowid);
		t2.start();
		for (int i = 0; i < 100 && !t2.trans.isSuspended(); i++)
			Thread.sleep(10);
		assertTrue(!t2.failed && t2.trans.isSuspended());

		ConflictThread t3 = new ConflictThread(tm, ds, rowid);
		t3.trans.setSerializable(true);
		t3.start();
		for (int i = 0; i < 100 && !t3.failed; i++)
			Thread.sleep(10);
		assertTrue(t3.failed);

		synchronized (t2) {
			tm.commitTransaction(trans);
			t2.wait();
		}
		assertTrue(!t2.failed && !t2.trans.isSuspended());
		tm.commitTransaction(t2.trans);

		trans = tm.beginTransaction();
		assertTrue(ds.fetch(trans, rec, rowid));
		assertTrue(rec.getValue() == 33);
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);
		tm.commitTransaction(trans);
	}

	/**
	 * Test deadlock among transactions by creating a dataset and inserting two
	 * rows and committing the initial transaction. Then while in one
	 * transaction update the first row and in a second transaction update the
	 * second row and in a separate thread attempt to update the first row held
	 * by the first transaction. Next, in the main thread, using the first
	 * transaction insert several more rows to increase the cost of aborting it,
	 * then attempt to update the second row held by the second transaction,
	 * then wait for the second transaction to abort due to deadlock. Try again
	 * but have the second transaction insert rows to change the cost equation
	 * and verify that it is the first transaction that is cancelled. Try again
	 * using three or more transactions to create the deadlock cycle.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testDeadlock() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;

		for (int run = 0; run < 2; run++) {
			trans = tm.beginTransaction();
			Segment segment = sm.createSegment(trans);
			Dataset ds = (Dataset) sf.createSegmentMethod(trans, segment,
					Dataset.class);

			// insert 2 records
			Record rec = new Record(0);
			Rowid rowid1 = ds.insert(trans, rec);
			Rowid rowid2 = ds.insert(trans, rec);
			Rowid rowid3 = ds.insert(trans, rec);

			tm.commitTransaction(trans);

			Transaction t1 = tm.beginTransaction();
			Transaction t2 = tm.beginTransaction();
			Transaction t3 = tm.beginTransaction();

			rec.setValue(1);
			ds.update(t1, rec, rowid1, tmprec);
			ds.update(t2, rec, rowid2, tmprec);
			ds.update(t3, rec, rowid3, tmprec);

			// 3 waits on 1
			ConflictThread ct3 = new ConflictThread(tm, ds, rowid1);
			tm.commitTransaction(ct3.trans);
			ct3.trans = t3;
			ct3.start();
			for (int i = 0; i < 100 && !ct3.trans.isSuspended(); i++)
				Thread.sleep(10);
			assertTrue(!ct3.failed && ct3.trans.isSuspended());

			// 2 waits on 3
			ConflictThread ct2 = new ConflictThread(tm, ds, rowid3);
			tm.commitTransaction(ct2.trans);
			ct2.trans = t2;
			ct2.start();
			for (int i = 0; i < 100 && !ct2.trans.isSuspended(); i++)
				Thread.sleep(10);
			assertTrue(!ct2.failed && ct2.trans.isSuspended());

			// insert a bunch of records
			for (int i = 0; i < 100; i++) {
				rec.setValue(i);
				if (run == 0)
					ds.insert(t1, rec);
				else
					ds.insert(t2, rec);
				ds.insert(t3, rec);
			}

			rec.setValue(33);
			try {
				// 1 waits on 2, causing deadlock
				ds.update(t1, rec, rowid2, tmprec);

				for (int i = 0; i < 100 && !ct2.deadlock; i++)
					Thread.sleep(10);
				assertTrue(ct2.deadlock);
				assertTrue(t2.isCancelled());

				assertTrue(run == 0);
			} catch (DeadlockException de) {
				assertTrue(run == 1);
				tm.rollback(t1, core.getSegmentFactory());
				tm.commitTransaction(t1);
				for (int i = 0; i < 100 && !ct3.success; i++)
					Thread.sleep(10);
				assertTrue(ct3.success);
			}

			if (run == 0) {
				// t2 is cancelled, t1 resumes
				assertTrue(ds.fetch(t1, rec, rowid1));
				int v = rec.getValue();
				assertTrue(v == 1);

				assertTrue(ds.fetch(t1, rec, rowid2));
				v = rec.getValue();
				assertTrue(v == 33);

				assertTrue(ds.fetch(t1, rec, rowid3));
				v = rec.getValue();
				assertTrue(v == 0);

				tm.commitTransaction(t1);

				// finally t3 resumes
				for (int i = 0; i < 100 && !ct3.success; i++)
					Thread.sleep(10);
				assertTrue(ct3.success);

				assertTrue(ds.fetch(t3, rec, rowid1));
				v = rec.getValue();
				assertTrue(v == 33);

				tm.commitTransaction(t3);
			} else {
				// t1 is cancelled, t3 resumes
				assertTrue(ds.fetch(t3, rec, rowid1));
				int v = rec.getValue();
				assertTrue(v == 33);

				assertTrue(ds.fetch(t3, rec, rowid2));
				v = rec.getValue();
				assertTrue(v == 0);

				assertTrue(ds.fetch(t3, rec, rowid3));
				v = rec.getValue();
				assertTrue(v == 1);

				tm.commitTransaction(t3);

				// finally t2 resumes
				for (int i = 0; i < 100 && !ct2.success; i++)
					Thread.sleep(10);
				assertTrue(ct2.success);

				assertTrue(ds.fetch(t2, rec, rowid3));
				v = rec.getValue();
				assertTrue(v == 33);

				tm.commitTransaction(t2);
			}

			trans = tm.beginTransaction();
			sm.dropSegment(trans, segment);
			sf.removeSegmentMethod(trans, ds);
			tm.commitTransaction(trans);
		}
	}

	class ConflictThread extends Thread {
		ConflictThread(TransactionManager tm, Dataset ds, Rowid rowid)
				throws DatabaseException, TypeException {
			this.tm = tm;
			this.ds = ds;
			this.rowid = rowid;
			this.trans = tm.beginTransaction();
		}

		@Override
		public void run() {
			try {
				Record rec = new Record(33);
				try {
					ds.update(trans, rec, rowid, tmprec);
					success = true;
				} catch (SerializationConflict sc) {
					failed = true;
				} catch (DeadlockException de) {
					deadlock = true;
				}
				if (failed || deadlock) {
					tm.rollback(trans, core.getSegmentFactory());
					tm.commitTransaction(trans);
					synchronized (this) {
						notifyAll();
					}
				}
			} catch (Exception e) {
				failed = true;
			}
		}

		TransactionManager tm;
		Transaction trans;
		Dataset ds;
		Rowid rowid;
		boolean failed;
		boolean deadlock;
		boolean success;
	}

	/**
	 * Create a dataset, insert, update, and delete a bunch of records then
	 * rollback and verify states are as they were prior to the cancelled
	 * transaction.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testRollback() throws DatabaseException, InterruptedException {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;

		// test rollback of the dataset creation itself
		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);
		Dataset ds = (Dataset) sf.createSegmentMethod(trans, segment,
				Dataset.class);
		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		Segment segment2 = sm.createSegment(trans);
		ds = (Dataset) sf.createSegmentMethod(trans, segment2, Dataset.class);
		tm.commitTransaction(trans);

		assertTrue(segment.getSegmentId().equals(segment2.getSegmentId()));
		assertTrue(segment.getSegmentType() == segment2.getSegmentType());
		assertTrue(segment.getNewExtent().equals(segment2.getNewExtent()));
		assertTrue(segment.getNextPage() == segment2.getNextPage());
		assertTrue(segment.getInitialSize() == segment2.getInitialSize());
		assertTrue(segment.getNextSize() == segment2.getNextSize());
		assertTrue(segment.getGrowthRate() == segment2.getGrowthRate());
		assertTrue(segment.getPageCount() == segment2.getPageCount());

		// test rollback of inserts
		Record rec = new Record(0);
		trans = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();
		int inserts = 10000;
		for (int i = 0; i < inserts; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		DatasetCursor cursor = new DatasetCursor();
		ds.openCursor(trans, cursor);

		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);
		scan(t2, cursor, ds, inserts, false);
		ds.rewind(cursor);

		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		scan(t2, cursor, ds, inserts, false);
		ds.rewind(cursor);

		// test rollback of updates
		trans = tm.beginTransaction();
		for (int i = 0; i < inserts; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}

		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		t2 = tm.beginTransaction();

		ds.openCursor(trans, cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			rec.setValue(inserts + 333);
			ds.update(trans, rec, cursor.getRowid(), tmprec);
		}
		ds.rewind(cursor);

		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);
		scan(t2, cursor, ds, inserts, true);
		ds.rewind(cursor);

		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, inserts, false);
		ds.rewind(cursor);
		scan(t2, cursor, ds, inserts, true);
		ds.rewind(cursor);

		// rollback the updates
		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		scan(t2, cursor, ds, inserts, true);
		ds.openCursor(trans, cursor);
		scan(t2, cursor, ds, inserts, true);
		ds.rewind(cursor);

		tm.commitTransaction(t2);
		trans = tm.beginTransaction();
		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);

		// test rollback of deletes
		trans = tm.beginTransaction();
		for (int i = 0; i < inserts; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}

		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		t2 = tm.beginTransaction();

		ds.openCursor(trans, cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			ds.remove(trans, cursor.getRowid(), tmprec);
		}
		ds.rewind(cursor);

		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);
		scan(t2, cursor, ds, inserts, true);
		ds.rewind(cursor);

		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, inserts, false);
		ds.rewind(cursor);
		scan(t2, cursor, ds, inserts, true);
		ds.rewind(cursor);

		// rollback the deletes
		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		scan(t2, cursor, ds, inserts, true);
		ds.openCursor(trans, cursor);
		scan(t2, cursor, ds, inserts, true);
		ds.rewind(cursor);

		tm.commitTransaction(t2);
		trans = tm.beginTransaction();
		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);

		// test rollback of inserts, updates, and deletes
		trans = tm.beginTransaction();
		for (int i = 0; i < inserts; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		t2 = tm.beginTransaction();

		ds.openCursor(trans, cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			rec.setValue(inserts + 333);
			ds.update(trans, rec, cursor.getRowid(), tmprec);
			ds.remove(trans, cursor.getRowid(), tmprec);
			rec.setValue(inserts + 444);
			ds.insert(trans, rec);
		}
		ds.rewind(cursor);
		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);
		scan(t2, cursor, ds, inserts, true);

		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, inserts, false);
		ds.rewind(cursor);
		scan2(trans, cursor, ds, inserts + 444, true);
		ds.rewind(cursor);
		scan(t2, cursor, ds, inserts, true);
		ds.rewind(cursor);
		scan2(t2, cursor, ds, inserts + 444, false);

		// rollback the updates, deletes, and inserts
		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);
		tm.commitTransaction(t2);

		trans = tm.beginTransaction();
		ds.openCursor(trans, cursor);
		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);

		sm.dropSegment(trans, segment);
		sf.removeSegmentMethod(trans, ds);
		tm.commitTransaction(trans);
	}

	/**
	 * Create a dataset, insert, update, and delete a bunch of records, then
	 * both with and without committing, kill and restart the core and validate
	 * the recovered state.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testRecovery1() throws DatabaseException, InterruptedException {
		testRollbackRecovery(false);
	}

	/**
	 * Create a dataset, insert, update, and delete a bunch of records, then
	 * rollback the transaction prior to killing and restarting the core and
	 * validate the recovered state.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testRecovery2() throws DatabaseException, InterruptedException {
		testRollbackRecovery(true);
	}

	/**
	 * Create a dataset, insert, update, and delete a bunch of records, then
	 * rollback the transaction prior to killing and restarting the core and
	 * validate the recovered state.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	private void testRollbackRecovery(boolean testRollback)
			throws DatabaseException, InterruptedException {
		Transaction trans;
		DatasetCursor cursor = new DatasetCursor();

		// test recovery of segment creation
		trans = core.getTransactionManager().beginTransaction();
		Segment segment = core.getSpaceManager().createSegment(trans);
		Dataset ds = (Dataset) core.getSegmentFactory().createSegmentMethod(
				trans, segment, Dataset.class);
		if (testRollback)
			core.getTransactionManager().rollback(trans,
					core.getSegmentFactory());

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());
		assertTrue(ds == null);

		segment = core.getSpaceManager().createSegment(trans);
		ds = (Dataset) core.getSegmentFactory().createSegmentMethod(trans,
				segment, Dataset.class);
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());
		assertTrue(ds != null);

		// test recovery of inserts
		Record rec = new Record(0);
		trans = core.getTransactionManager().beginTransaction();
		int inserts = 10000;
		for (int i = 0; i < inserts; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		if (testRollback)
			core.getTransactionManager().rollback(trans,
					core.getSegmentFactory());

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());

		ds.openCursor(trans, cursor);

		scan(trans, cursor, ds, inserts, false);

		for (int i = 0; i < inserts; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());

		ds.openCursor(trans, cursor);

		scan(trans, cursor, ds, inserts, true);

		// test recovery of updates
		ds.rewind(cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			rec.setValue(inserts + 333);
			ds.update(trans, rec, cursor.getRowid(), tmprec);
		}
		if (testRollback)
			core.getTransactionManager().rollback(trans,
					core.getSegmentFactory());

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());

		ds.openCursor(trans, cursor);

		scan(trans, cursor, ds, inserts, true);
		ds.rewind(cursor);
		scan2(trans, cursor, ds, inserts + 333, false);

		ds.rewind(cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			rec.setValue(inserts + 333);
			ds.update(trans, rec, cursor.getRowid(), tmprec);
		}
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());

		ds.openCursor(trans, cursor);

		scan(trans, cursor, ds, inserts, false);
		ds.rewind(cursor);
		scan2(trans, cursor, ds, inserts + 333, true);

		// test recovery of deletes
		ds.rewind(cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			ds.remove(trans, cursor.getRowid(), tmprec);
		}
		if (testRollback)
			core.getTransactionManager().rollback(trans,
					core.getSegmentFactory());

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());

		ds.openCursor(trans, cursor);

		for (int i = 0; i < inserts; i++)
			scan2(trans, cursor, ds, inserts + 333, true);

		ds.rewind(cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			ds.remove(trans, cursor.getRowid(), tmprec);
		}
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());

		ds.openCursor(trans, cursor);

		scan2(trans, cursor, ds, inserts + 333, false);

		// test recovery of inserts, updates, and deletes
		for (int i = 0; i < inserts; i++) {
			rec.setValue(i);
			ds.insert(trans, rec);
		}
		core.getTransactionManager().commitTransaction(trans);

		trans = core.getTransactionManager().beginTransaction();

		ds.openCursor(trans, cursor);
		while (ds.fetchNext(trans, cursor, rec, null)) {
			rec.setValue(inserts + 333);
			ds.update(trans, rec, cursor.getRowid(), tmprec);
			ds.remove(trans, cursor.getRowid(), tmprec);
			rec.setValue(inserts + 444);
			ds.insert(trans, rec);
		}
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());

		ds.openCursor(trans, cursor);

		for (int i = 0; i < inserts; i++)
			scan2(trans, cursor, ds, inserts + 444, true);

		// test recovery of drop segment
		core.getSpaceManager().dropSegment(trans, segment);
		core.getSegmentFactory().removeSegmentMethod(trans, ds);
		if (testRollback)
			core.getTransactionManager().rollback(trans,
					core.getSegmentFactory());

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());
		assertTrue(ds != null);

		ds.openCursor(trans, cursor);

		for (int i = 0; i < inserts; i++)
			scan2(trans, cursor, ds, inserts + 444, true);

		core.getSpaceManager().dropSegment(trans, segment);
		core.getSegmentFactory().removeSegmentMethod(trans, ds);
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		ds = (Dataset) core.getSegmentFactory().getSegmentMethod(
				segment.getSegmentId());
		assertTrue(ds == null);
	}

	private void scan(Transaction trans, DatasetCursor cursor, Dataset ds,
			int max, boolean see) throws DatabaseException, TypeException {
		Record rec = new Record(0);
		RecordFilter rc = new RecordFilter();
		rc.setValue(0);
		if (see) {
			assertTrue(ds.fetchNext(trans, cursor, rec, rc));
			assertTrue(rec.getValue() == 0);
			rec.validate();
		} else {
			assertFalse(ds.fetchNext(trans, cursor, rec, rc));
		}

		rc.setValue(max / 2);
		if (see) {
			assertTrue(ds.fetchNext(trans, cursor, rec, rc));
			assertTrue(rec.getValue() == max / 2);
			rec.validate();
		} else {
			assertFalse(ds.fetchNext(trans, cursor, rec, rc));
		}

		rc.setValue(max - 1);
		if (see) {
			assertTrue(ds.fetchNext(trans, cursor, rec, rc));
			assertTrue(rec.getValue() == max - 1);
			rec.validate();
		} else {
			assertFalse(ds.fetchNext(trans, cursor, rec, rc));
		}

		rc.setValue(max);
		assertFalse(ds.fetchNext(trans, cursor, rec, rc));
		rc.setValue(0);
		assertFalse(ds.fetchNext(trans, cursor, rec, rc));

		scanBackwards(trans, cursor, ds, max, see);

		rc.setValue(max - 1);
		if (see) {
			assertTrue(ds.fetchNext(trans, cursor, rec, rc));
			assertTrue(rec.value == max - 1);
			rec.validate();
		} else {
			assertFalse(ds.fetchNext(trans, cursor, rec, rc));
		}
	}

	private void scanBackwards(Transaction trans, DatasetCursor cursor,
			Dataset ds, int max, boolean see) throws DatabaseException,
			TypeException {
		Record rec = new Record(0);
		RecordFilter rc = new RecordFilter();

		rc.setValue(max - 1);
		if (see) {
			assertTrue(ds.fetchPrev(trans, cursor, rec, rc));
			assertTrue(rec.getValue() == max - 1);
			rec.validate();
		} else {
			assertFalse(ds.fetchPrev(trans, cursor, rec, rc));
		}

		rc.setValue(max / 2);
		if (see) {
			assertTrue(ds.fetchPrev(trans, cursor, rec, rc));
			assertTrue(rec.getValue() == max / 2);
			rec.validate();
		} else {
			assertFalse(ds.fetchPrev(trans, cursor, rec, rc));
		}

		rc.setValue(0);
		if (see) {
			assertTrue(ds.fetchPrev(trans, cursor, rec, rc));
			assertTrue(rec.getValue() == 0);
			rec.validate();
		} else {
			assertFalse(ds.fetchPrev(trans, cursor, rec, rc));
		}

		rc.setValue(max);
		assertFalse(ds.fetchPrev(trans, cursor, rec, rc));
		rc.setValue(0);
		assertFalse(ds.fetchPrev(trans, cursor, rec, rc));
	}

	private void scan2(Transaction trans, DatasetCursor cursor, Dataset ds,
			int value, boolean see) throws DatabaseException, TypeException {
		Record rec = new Record(0);
		RecordFilter rc = new RecordFilter();
		rc.setValue(value);
		if (see) {
			assertTrue(ds.fetchNext(trans, cursor, rec, rc));
			assertTrue(rec.getValue() == value);
			rec.validate();
		} else {
			assertFalse(ds.fetchNext(trans, cursor, rec, rc));
		}
	}

	public class RecordFilter extends Record implements Filter {
		public RecordFilter() throws DatabaseException, TypeException {
			super(0);
		}

		public boolean passes(Object o) {
			return equals(o);
		}
	}

	public class Record implements DBObject {
		public Record(int value) throws DatabaseException, TypeException {
			setValue(value);
		}

		public Record(Record record) {
			data = record.data.clone();
			vchar = (Varchar) record.vchar.copy();
		}

		public void assign(DBObject o) {
			Record record = (Record) o;
			data = record.data.clone();
			vchar.assign(record.vchar);
		}

		public void validate() {
			assertTrue(data[0] == (byte) (value % DATASIZE));
			assertTrue(data[value % DATASIZE] == (byte) (value % DATASIZE));
			assertTrue(data[DATASIZE - 1] == (byte) (value % DATASIZE));
		}

		public void setValue(int value) throws TypeException {
			this.value = value;
			data[0] = (byte) (value % DATASIZE);
			data[value % DATASIZE] = (byte) (value % DATASIZE);
			data[DATASIZE - 1] = (byte) (value % DATASIZE);
			switch (random(3)) {
			case 0:
				vchar.setString("HELLO");
				break;
			case 1:
				vchar.setString("HELLO WORLD");
				break;
			case 2:
				vchar
						.setString("ABCDEFGHIJKLMNOPQRSTUVWXYZ ABCDEFGHIJKLMNOPQRSTUVWXYZ");
				break;
			case 3:
				vchar.setString("HELLO WORLD WITH LOTS OF ROOM TO GROW");
				break;
			}
		}

		public int getValue() {
			return value;
		}

		// Object overrides
		@Override
		public boolean equals(Object obj) {
			return compareTo(obj) == 0;
		}

		@Override
		public int hashCode() {
			return value;
		}

		// Comparable method
		public int compareTo(Object o) {
			Record other = (Record) o;
			return value - other.value;
		}

		// DBObject methods
		public DBObject copy() {
			return new Record(this);
		}

		public void writeTo(ByteBuffer bb) {
			bb.put(data);
			bb.putInt(value);
			vchar.writeTo(bb);
		}

		public void readFrom(ByteBuffer bb) {
			bb.get(data);
			value = bb.getInt();
			vchar.readFrom(bb);
		}

		public int storeSize() {
			return data.length + 4 + vchar.storeSize();
		}

		private Varchar vchar = new VarcharColumn(500);
		private byte[] data = new byte[DATASIZE];
		private int value;
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	int DATASIZE = 255;
	Record tmprec;
	Core core;
}
