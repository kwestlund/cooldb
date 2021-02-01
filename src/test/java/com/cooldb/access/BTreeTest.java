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

package com.cooldb.access;

import com.cooldb.api.DatabaseException;
import com.cooldb.api.Key;
import com.cooldb.api.TypeException;
import com.cooldb.api.UniqueConstraintException;
import com.cooldb.core.Core;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.segment.SpaceManager;
import com.cooldb.storage.Rowid;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertTrue;

public class BTreeTest {

	@Before
	public void setUp() throws DatabaseException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();
		core = new Core(file);
		core.createDatabase(true);
	}

	@After
	public void tearDown() {
		core.destroyDatabase();
		core = null;

		System.gc();
	}

	/**
	 * Test various functions of the PrefixKey, including comparison functions,
	 * using negative and positive numbers.
	 */
//	@Test
//	public void testPrefixKey() {
//		PrefixKey key1 = new PrefixKey();
//		PrefixKey key2 = new PrefixKey();
//		key1.append(100);
//		key2.append(-100);
//
//		assertTrue(key1.compareTo(key2) > 0);
//		assertTrue(key1.compareTo(key1) == 0);
//		assertTrue(key2.compareTo(key1) < 0);
//		assertTrue(key2.compareTo(key2) == 0);
//	}

	/**
	 * Create an index, insert enough keys to cause splits and a couple of new
	 * levels in the tree, and fetch keys to validate.
	 *
	 * @throws Exception
	 */
	@Test
	public void testInsert() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		BTree bt;
		Rowid retval;

		bt = createBTree();

		trans = tm.beginTransaction();

		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < 150000; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);

			retval = bt.fetch(trans, key);
			assertTrue(retval != null && value.equals(retval));
		}

		key.setInt(0, 150000);
		retval = bt.fetch(trans, key);
		assertTrue(retval == null);

		// more fetching
		for (int i = 0; i < 150000; i++) {
			key.setInt(0, i);
			retval = bt.fetch(trans, key);
			assertTrue(retval != null && retval.getPage().getPageId() == i);
		}

		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);

		tm.commitTransaction(trans);
	}

	/**
	 * Create an index, insert a few keys, then insert a duplicate key and
	 * verify a UniqueConstraintViolation is thrown.
	 *
	 * @throws Exception
	 */
	@Test
	public void testUniqueConstraint() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		BTree bt;
		Rowid retval;

		bt = createBTree();

		trans = tm.beginTransaction();

		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		key.setInt(0, 33);
		value.getPage().setPageId(33);

		bt.insert(trans, key, value);

		retval = bt.fetch(trans, key);
		assertTrue(retval != null && value.equals(retval));

		try {
			bt.insert(trans, key, value);
			throw new Exception("Was expecting UniqueConstraintViolation");
		} catch (UniqueConstraintException e) {
		}

		retval = bt.fetch(trans, key);
		assertTrue(retval != null && value.equals(retval));

		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);

		tm.commitTransaction(trans);
	}

	/**
	 * Create an index, insert random keys and fetch to validate.
	 *
	 * @throws Exception
	 */
	@Test
	public void testRandomFetch() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		BTree bt;
		Rowid retval;

		bt = createBTree();

		trans = tm.beginTransaction();

		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		Rowid[] vals = new Rowid[150000];
		for (int i = 0; i < vals.length; i++) {
			int k = random(1000000);
			key.setInt(0, k);
			value.getPage().setPageId(k);
			try {
				bt.insert(trans, key, value);
				vals[i] = (Rowid) value.copy();
			} catch (UniqueConstraintException e) {
			}
		}

		for (int i = 0; i < vals.length; i++) {
			if (vals[i] == null)
				continue;
			int k = vals[i].getPage().getPageId();
			key.setInt(0, k);

			retval = bt.fetch(trans, key);
			assertTrue(retval != null && vals[i].equals(retval));
		}

		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);

		tm.commitTransaction(trans);
	}

	/**
	 * Create an index, insert enough keys to cause splits and a couple of new
	 * levels in the tree, perform various types of range scans to validate.
	 *
	 * @throws Exception
	 */
	@Test
	public void testRangeScans() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		BTree bt;
		Rowid retval;

		bt = createBTree();

		trans = tm.beginTransaction();

		int rows = 150000;
		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);

			retval = bt.fetch(trans, key);
			assertTrue(retval != null && value.equals(retval));
		}

		runRangeScans(trans, bt, rows, true);

		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);

		tm.commitTransaction(trans);
	}

	/**
	 * Create an index, insert some keys in one transaction, then in a second
	 * transaction verify that the inserted keys cannot be seen. Then delete
	 * some keys in one transaction and verify that the second transaction still
	 * observes them.
	 *
	 * @throws Exception
	 */
	@Test
	public void testReadCommitted() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		BTree bt;
		Rowid retval;

		bt = createBTree();

		trans = tm.beginTransaction();

		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < 1500; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}

		// Validate inserts by first transaction not seen by second
		key.setInt(0, 333);
		retval = bt.fetch(trans, key);
		assertTrue(retval != null && retval.getPage().getPageId() == 333);

		Transaction t2 = tm.beginTransaction();

		retval = bt.fetch(t2, key);
		assertTrue(retval == null);

		tm.commitTransaction(trans);

		// make sure the second transaction still cannot see inserts
		retval = bt.fetch(t2, key);
		assertTrue(retval == null);

		tm.commitTransaction(t2);

		trans = tm.beginTransaction();

		// delete the rows
		for (int i = 0; i < 1500; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}

		t2 = tm.beginTransaction();
		tm.commitTransaction(trans);

		// make sure t2 can see the deleted rows
		for (int i = 0; i < 1500; i++) {
			key.setInt(0, i);
			retval = bt.fetch(t2, key);
			assertTrue(retval != null && retval.getPage().getPageId() == i);
		}

		tm.commitTransaction(t2);

		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);

		tm.commitTransaction(trans);
	}

	/**
	 * Create an index, insert keys, delete keys and fetch to validate.
	 *
	 * @throws Exception
	 */
	@Test
	public void testDelete() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		BTree bt;
		Rowid retval;

		bt = createBTree();

		trans = tm.beginTransaction();

		int rows = 150000;
		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}

		// Delete a single key, but first verify its presence
		key.setInt(0, 333);
		retval = bt.fetch(trans, key);
		assertTrue(retval != null && retval.getPage().getPageId() == 333);
		bt.remove(trans, key);
		retval = bt.fetch(trans, key);
		assertTrue(retval == null);

		// Now delete all keys
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}

		tm.commitTransaction(trans);
		trans = tm.beginTransaction();

		// Now scan the tree to verify no keys are found
		for (int i = 0; i < 100; i++) {
			BTreePredicate p = bt.allocatePredicate();
			p.setStartOp(BTreePredicate.GT);
			p.setEndOp(BTreePredicate.LT);
			key = p.getStartKey();
			key.setMinValue();
			Key endKey = p.getEndKey();
			endKey.setMaxValue();
			GiST.Cursor cursor = bt.allocateCursor();
			retval = (Rowid) bt.findFirst(trans, p, cursor, false);
			assertTrue(retval == null);
		}

		// re-insert the keys
		tm.commitTransaction(trans);
		trans = tm.beginTransaction();

		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}

		runRangeScans(trans, bt, rows, true);

		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);

		tm.commitTransaction(trans);
	}

	/**
	 * Create a BTree, insert keys in one transaction, then open a cursor and
	 * scan for them, then insert some more and scan for the new ones (the first
	 * set should be found, the second set should not be found), then scan for
	 * them in another transaction prior to committing the first transaction,
	 * then scan for them again after committing the first transaction, then
	 * reopen the cursor following the commit and scan again (in no case should
	 * the second transaction see the first transaction's data). Open a third
	 * transaction and scan for the data, which should be found. Try similar
	 * tests with deletes.
	 *
	 * @throws Exception
	 */
	@Test
	public void testVersioning() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Transaction t2;
		BTree bt;

		bt = createBTree();

		trans = tm.beginTransaction();

		// insert a bunch of records
		int rows = 10000;
		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}

		// scan in current transaction
		GiST.Cursor cursor = bt.allocateCursor();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, true);

		// scan using second transaction
		t2 = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan(t2, cursor, bt, rows, false);

		int offset = 10000;
		for (int i = rows; i < rows + offset; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}
		rows += offset;
		scan(trans, cursor, bt, 0, offset, true, false);
		scan(trans, cursor, bt, offset, rows, false, false);
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, true);

		tm.commitTransaction(trans);

		bt.openCursor(trans, cursor);
		scan(t2, cursor, bt, rows, false);

		tm.commitTransaction(t2);

		// scan again using third transaction
		t2 = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan(t2, cursor, bt, rows, true);
		tm.commitTransaction(t2);

		// delete all of the records
		trans = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}

		// scan in current transaction
		bt.openCursor(trans, cursor);
		scan2(trans, cursor, bt, 333, false);

		// scan using second transaction
		t2 = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan2(t2, cursor, bt, 333, true);

		// commit
		tm.commitTransaction(trans);
		tm.commitTransaction(t2);

		// scan again using third transaction
		t2 = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan2(t2, cursor, bt, 333, false);
		tm.commitTransaction(t2);

		trans = tm.beginTransaction();
		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);
		tm.commitTransaction(trans);
	}

	/**
	 * Cause the BTree to need to reconstruct keys from previous versions read
	 * from the undo log file: create a BTree, create 2 transactions, insert and
	 * commit a bunch of keys, which will not be universally committed as long
	 * as the second transaction remains active, then create a third
	 * transaction, delete the keys, and create a fourth transaction. Then,
	 * still using the 3rd transaction insert a new set of keys (with different
	 * pointer values), and commit the third transaction. Then scan the keys
	 * using the fourth transaction and verify that the first set of
	 * keys/pointer values are observed.
	 *
	 * @throws Exception
	 */
	@Test
	public void testVersioning2() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Transaction t2;
		Transaction t3;
		BTree bt;

		bt = createBTree();

		trans = tm.beginTransaction();
		t2 = tm.beginTransaction();

		// insert a bunch of records
		int rows = 10000;
		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}

		tm.commitTransaction(trans);

		// delete all of the records
		trans = tm.beginTransaction();
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}

		t3 = tm.beginTransaction();

		// scan in t3
		GiST.Cursor cursor = bt.allocateCursor();
		bt.openCursor(trans, cursor);
		scan(t3, cursor, bt, rows, true);

		// replace keys with new values
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(rows + i);
			bt.insert(trans, key, value);
		}

		tm.commitTransaction(trans);

		// scan in t3
		bt.openCursor(trans, cursor);
		scan(t3, cursor, bt, rows, true);

		tm.commitTransaction(t2);
		tm.commitTransaction(t3);

		trans = tm.beginTransaction();
		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);
		tm.commitTransaction(trans);
	}

	/**
	 * Create a BTree, insert and delete a bunch of keys then rollback and
	 * verify states are as they were prior to the cancelled transaction.
	 *
	 * @throws Exception
	 */
	@Test
	public void testRollback() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		Transaction t2;
		BTree bt;

		bt = createBTree();

		// test rollback of inserts, case 1
		trans = tm.beginTransaction();
		int rows = 10000;
		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}

		// try to catch a nested-top-action failure to preserve splits
		t2 = tm.beginTransaction();
		key.setInt(0, rows);
		value.getPage().setPageId(rows);
		bt.insert(t2, key, value);
		tm.commitTransaction(t2);

		GiST.Cursor cursor = bt.allocateCursor();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, true);

		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, false);
		scan2(trans, cursor, bt, rows, true);

		// test rollback of inserts, case 2
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}
		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}
		t2 = tm.beginTransaction();
		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}
		tm.commitTransaction(t2);

		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, false);

		// test rollback of inserts, case 3
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}
		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}
		t2 = tm.beginTransaction();
		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i + rows);
			bt.insert(trans, key, value);
		}

		bt.openCursor(trans, cursor);
		scan(t2, cursor, bt, rows, true);

		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, false);

		bt.openCursor(trans, cursor);
		scan(t2, cursor, bt, rows, true);
		tm.commitTransaction(t2);

		// test rollback of deletes, case 1
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}
		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}
		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);

		trans = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, true);

		// test rollback of deletes, case 2
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
			value.getPage().setPageId(i);
			bt.insert(trans, key, value);
		}
		t2 = tm.beginTransaction();
		tm.commitTransaction(trans);
		trans = tm.beginTransaction();
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i);
			bt.remove(trans, key);
		}
		tm.rollback(trans, core.getSegmentFactory());
		tm.commitTransaction(trans);
		tm.commitTransaction(t2);

		trans = tm.beginTransaction();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, true);

		sm.dropSegment(trans, bt.getSegment());
		sf.removeSegmentMethod(trans, bt);
		tm.commitTransaction(trans);
	}

	/**
	 * Create a btree, insert and delete a bunch of records, then kill and
	 * restart the core and validate the recovered state.
	 *
	 * @throws Exception
	 */
	@Test
	public void testRecovery() throws Exception {
		testRollbackRecovery(false, false);
		testRollbackRecovery(false, true);
		testRollbackRecovery(true, false);
		testRollbackRecovery(true, true);
	}

	private BTree createBTree() throws Exception {
		SpaceManager sm = core.getSpaceManager();
		SegmentFactory sf = core.getSegmentFactory();
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;

		trans = tm.beginTransaction();
		Segment segment = sm.createSegment(trans);

		BTree bt = (BTree) sf.loadSegmentMethod(segment, BTree.class);
		TreeDescriptor treeDescriptor = (TreeDescriptor) bt.getDescriptor();
		treeDescriptor.setKeyTypes("I");
		treeDescriptor.setUnique(true);
		sf.createSegmentMethod(trans, bt);

		tm.commitTransaction(trans);

		return bt;
	}

	/**
	 * Create a btree, insert and delete a bunch of records, then kill and
	 * restart the core and validate the recovered state.
	 *
	 * @throws Exception
	 */
	private void testRollbackRecovery(boolean testRollback, boolean negnums)
			throws Exception {
		TransactionManager tm = core.getTransactionManager();
		Transaction trans;
		BTree bt;
		int dir = negnums ? -1 : 1;

		bt = createBTree();

		// test recovery of inserts
		trans = tm.beginTransaction();
		int rows = 10000;
		Key key = bt.allocateKey();
		Rowid value = new Rowid((short) 0, 0, (short) 0);
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i * dir);
			value.getPage().setPageId(i * dir);
			bt.insert(trans, key, value);
		}
		if (testRollback)
			core.getTransactionManager().rollback(trans,
					core.getSegmentFactory());

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		bt = (BTree) core.getSegmentFactory().getSegmentMethod(
				bt.getSegment().getSegmentId());

		GiST.Cursor cursor = bt.allocateCursor();
		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, false, negnums);

		for (int i = 0; i < rows; i++) {
			key.setInt(0, i * dir);
			value.getPage().setPageId(i * dir);
			bt.insert(trans, key, value);
		}
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		bt = (BTree) core.getSegmentFactory().getSegmentMethod(
				bt.getSegment().getSegmentId());

		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, true, negnums);

		// test recovery of deletes
		for (int i = 0; i < rows; i++) {
			key.setInt(0, i * dir);
			bt.remove(trans, key);
		}
		if (testRollback)
			core.getTransactionManager().rollback(trans,
					core.getSegmentFactory());

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		bt = (BTree) core.getSegmentFactory().getSegmentMethod(
				bt.getSegment().getSegmentId());

		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, true, negnums);

		for (int i = 0; i < rows; i++) {
			key.setInt(0, i * dir);
			bt.remove(trans, key);
		}
		core.getTransactionManager().commitTransaction(trans);

		// hard stop, restart
		core.stopDatabase();
		core.startDatabase();

		trans = core.getTransactionManager().beginTransaction();
		bt = (BTree) core.getSegmentFactory().getSegmentMethod(
				bt.getSegment().getSegmentId());

		bt.openCursor(trans, cursor);
		scan(trans, cursor, bt, rows, false, negnums);
	}

	private void runRangeScans(Transaction trans, BTree bt, int rows,
			boolean see) throws DatabaseException, TypeException {
		GiST.Cursor cursor = bt.allocateCursor();
		bt.openCursor(trans, cursor);
		runRangeScans(trans, cursor, bt, rows, see);
	}

	private void runRangeScans(Transaction trans, GiST.Cursor cursor, BTree bt,
			int rows, boolean see) throws DatabaseException, TypeException {
		runRangeScans(trans, cursor, bt, false, rows, see);
		runRangeScans(trans, cursor, bt, true, rows, see);
	}

	private void runRangeScans(Transaction trans, GiST.Cursor cursor, BTree bt,
			boolean reverse, int rows, boolean see) throws DatabaseException,
			TypeException {
		BTreePredicate pred = bt.allocatePredicate();
		Key startKey = pred.getStartKey();
		Key endKey = pred.getEndKey();
		int dir = reverse ? -1 : 1;
		int batchsize = 1000;
		Rowid retval;

		// test GTE
		if (reverse) {
			pred.setEndOp(BTreePredicate.GTE);
			pred.setStartOp(BTreePredicate.LTE);
		} else
			pred.setStartOp(BTreePredicate.GTE);

		for (int i = 0; i < 100; i++) {
			int k = random(rows - (1 + batchsize));
			if (reverse)
				k += batchsize;
			startKey.setInt(0, k);
			endKey.setInt(0, k + (batchsize * dir));
			retval = (Rowid) bt.findFirst(trans, pred, cursor, reverse);
			if (see)
				assertTrue(retval != null && retval.getPage().getPageId() == k);
			else
				assertTrue(retval == null);

			for (int j = 1; j < batchsize; j++) {
				retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
				if (see)
					assertTrue(retval != null
							&& retval.getPage().getPageId() == k + (j * dir));
				else
					assertTrue(retval == null);
			}
		}

		// test GT
		if (reverse) {
			pred.setEndOp(BTreePredicate.GT);
			pred.setStartOp(BTreePredicate.LT);
		} else
			pred.setStartOp(BTreePredicate.GT);

		for (int i = 0; i < 100; i++) {
			int k = random(rows - 1 - batchsize);
			if (reverse)
				k += batchsize;
			startKey.setInt(0, k);
			endKey.setInt(0, k + (batchsize * dir));
			retval = (Rowid) bt.findFirst(trans, pred, cursor, reverse);
			if (see)
				assertTrue(retval != null
						&& retval.getPage().getPageId() == k + (1 * dir));
			else
				assertTrue(retval == null);

			for (int j = 2; j < batchsize; j++) {
				retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
				if (see)
					assertTrue(retval != null
							&& retval.getPage().getPageId() == k + (j * dir));
				else
					assertTrue(retval == null);
			}
		}

		// test LTE
		if (reverse) {
			pred.setStartOp(BTreePredicate.LTE);
			pred.setEndOp(BTreePredicate.GTE);
		} else {
			pred.setEndOp(BTreePredicate.LTE);
			pred.setStartOp(BTreePredicate.GTE);
		}

		for (int i = 0; i < 100; i++) {
			int k = random(rows - 1 - batchsize);
			if (reverse)
				k += batchsize;
			startKey.setInt(0, k);
			endKey.setInt(0, k + (batchsize * dir));
			retval = (Rowid) bt.findFirst(trans, pred, cursor, reverse);
			if (see)
				assertTrue(retval != null && retval.getPage().getPageId() == k);
			else
				assertTrue(retval == null);

			for (int j = 1; j <= batchsize; j++) {
				retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
				if (see)
					assertTrue(retval != null
							&& retval.getPage().getPageId() == k + (j * dir));
				else
					assertTrue(retval == null);
			}

			retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
			assertTrue(retval == null);
		}

		// test LT
		if (reverse) {
			pred.setStartOp(BTreePredicate.LT);
			pred.setEndOp(BTreePredicate.GT);
		} else {
			pred.setStartOp(BTreePredicate.GT);
			pred.setEndOp(BTreePredicate.LT);
		}

		for (int i = 0; i < 100; i++) {
			int k = random(rows - 1 - batchsize);
			if (reverse)
				k += batchsize;
			startKey.setInt(0, k);
			endKey.setInt(0, k + (batchsize * dir));
			retval = (Rowid) bt.findFirst(trans, pred, cursor, reverse);
			if (see)
				assertTrue(retval != null
						&& retval.getPage().getPageId() == k + (1 * dir));
			else
				assertTrue(retval == null);

			for (int j = 2; j < batchsize; j++) {
				retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
				if (see)
					assertTrue(retval != null
							&& retval.getPage().getPageId() == k + (j * dir));
				else
					assertTrue(retval == null);
			}

			retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
			assertTrue(retval == null);
		}

		// test open start
		int startval = reverse ? rows - 1 : 0;
		if (reverse) {
			pred.setStartOp(BTreePredicate.LT);
			pred.setEndOp(BTreePredicate.GT);
			startKey.setMaxValue();
		} else {
			pred.setStartOp(BTreePredicate.GT);
			pred.setEndOp(BTreePredicate.LT);
			startKey.setMinValue();
		}
		endKey.setInt(0, startval + (batchsize * dir));

		retval = (Rowid) bt.findFirst(trans, pred, cursor, reverse);
		if (see)
			assertTrue(retval != null
					&& retval.getPage().getPageId() == startval);
		else
			assertTrue(retval == null);

		for (int j = 1; j < batchsize; j++) {
			retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
			if (see)
				assertTrue(retval != null
						&& retval.getPage().getPageId() == startval + (j * dir));
			else
				assertTrue(retval == null);
		}

		retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
		assertTrue(retval == null);

		// test full scan
		if (reverse) {
			endKey.setMinValue();
		} else {
			endKey.setMaxValue();
		}
		retval = (Rowid) bt.findFirst(trans, pred, cursor, reverse);
		if (see)
			assertTrue(retval != null
					&& retval.getPage().getPageId() == startval);
		else
			assertTrue(retval == null);

		for (int j = 1; j < rows; j++) {
			retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
			if (see)
				assertTrue(retval != null
						&& retval.getPage().getPageId() == startval + (j * dir));
			else
				assertTrue(retval == null);
		}

		retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
		assertTrue(retval == null);
	}

	private void scan(Transaction trans, GiST.Cursor cursor, BTree bt,
			int rows, boolean see) throws DatabaseException, TypeException {
		scan(trans, cursor, bt, 0, rows, see, false);
	}

	private void scan(Transaction trans, GiST.Cursor cursor, BTree bt,
			int rows, boolean see, boolean negnums) throws DatabaseException,
			TypeException {
		scan(trans, cursor, bt, 0, rows, see, negnums);
	}

	private void scan(Transaction trans, GiST.Cursor cursor, BTree bt,
			int start, int rows, boolean see, boolean negnums)
			throws DatabaseException, TypeException {
		BTreePredicate pred = bt.allocatePredicate();
		Key startKey = pred.getStartKey();
		int dir = negnums ? -1 : 1;
		Rowid retval;

		// lookup each key individually:
		pred.setStartOp(BTreePredicate.EQ);
		for (int i = start; i < rows; i++) {
			startKey.setInt(0, i * dir);
			retval = (Rowid) bt.findFirst(trans, pred, cursor, false);
			if (see)
				assertTrue(retval != null
						&& retval.getPage().getPageId() == i * dir);
			else
				assertTrue(retval == null);
		}

		// test full scan
		fullScan(trans, cursor, bt, start, rows, see, false, negnums);
		fullScan(trans, cursor, bt, start, rows, see, true, negnums);
	}

	private void fullScan(Transaction trans, GiST.Cursor cursor, BTree bt,
			int start, int end, boolean see, boolean reverse, boolean negnums)
			throws DatabaseException, TypeException {

		BTreePredicate pred = bt.allocatePredicate();
		Key startKey = pred.getStartKey();
		Key endKey = pred.getEndKey();
		int dir = (reverse ? -1 : 1);
		Rowid retval;
		int count = end - start;
		int startval;
		int startOp;
		int endOp;

		if (negnums) {
			int tmp = end;
			end = -start;
			start = -tmp;
			startval = (reverse ? end : start + 1);
			startOp = (reverse ? BTreePredicate.LTE : BTreePredicate.GT);
			endOp = (reverse ? BTreePredicate.GT : BTreePredicate.LTE);
		} else {
			startval = (reverse ? end - 1 : start);
			startOp = (reverse ? BTreePredicate.LT : BTreePredicate.GTE);
			endOp = (reverse ? BTreePredicate.GTE : BTreePredicate.LT);
		}

		if (reverse) {
			pred.setStartOp(startOp);
			startKey.setInt(0, end);
			pred.setEndOp(endOp);
			endKey.setInt(0, start);
		} else {
			pred.setStartOp(startOp);
			startKey.setInt(0, start);
			pred.setEndOp(endOp);
			endKey.setInt(0, end);
		}

		retval = (Rowid) bt.findFirst(trans, pred, cursor, reverse);
		if (see)
			assertTrue(retval != null
					&& retval.getPage().getPageId() == startval);
		else
			assertTrue(retval == null);

		for (int j = 1; j < count; j++) {
			retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
			if (see)
				assertTrue(retval != null
						&& retval.getPage().getPageId() == startval + (j * dir));
			else
				assertTrue(retval == null);
		}

		retval = (Rowid) bt.findNext(trans, pred, cursor, reverse);
		assertTrue(retval == null);
	}

	private void scan2(Transaction trans, GiST.Cursor cursor, BTree bt,
			int value, boolean see) throws DatabaseException, TypeException {
		Key key = bt.allocateKey();
		Rowid retval;
		key.setInt(0, value);
		retval = bt.fetch(trans, key);
		if (see)
			assertTrue(retval != null && retval.getPage().getPageId() == value);
		else
			assertTrue(retval == null);
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	private Core core;
}
