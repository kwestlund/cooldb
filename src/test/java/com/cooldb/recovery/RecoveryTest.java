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

package com.cooldb.recovery;

import com.cooldb.buffer.*;
import com.cooldb.log.*;
import com.cooldb.transaction.*;
import com.cooldb.transaction.*;
import com.cooldb.buffer.*;
import com.cooldb.log.*;
import com.cooldb.transaction.TransactionLogger;
import com.cooldb.transaction.TransactionState;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertTrue;

public class RecoveryTest {

	// TODO: add test for multi-threaded transactions and recovery
	// TODO: add test for undoing a transaction to a savepoint, then making
	// more updates, then undoing the whole thing
	// TODO: add test for restarting after a failure during the recovery
	// process itself

	@Before
	public void setUp() throws BufferNotFound, InterruptedException,
			LogExhaustedException, IOException {
		System.gc();

		createDatabase();
		startDatabase();

		undo.create();

		cpw.syncCheckPoint();
	}

	@After
	public void tearDown() throws BufferNotFound, LogExhaustedException,
			InterruptedException, IOException {
		cpw.syncCheckPoint();

		killDatabase();
		removeDatabase();

		System.gc();
	}

	/**
	 * Create 3 transactions, pin 3 pages exclusively, modify them with an unPin
	 * w/lsn, then pin them again so the buffer pool will consider them dirty
	 * but not be able to flush them when a checkpoint is taken, then take a
	 * checkpoint so that the transactions and dirty pages must be written in
	 * the checkpoint record, then read the checkpoint record from the log and
	 * make sure it contains the correct data
	 *
	 * @throws LogNotFoundException
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws LogExhaustedException
	 */
	@Test
	public void testCheckpoint() throws LogNotFoundException,
			TransactionCancelledException, BufferNotFound,
			InterruptedException, LogExhaustedException {
		// in order for this to work, we have to stop the DatabaseWriter
		((BufferPoolImpl) bp).stopDatabaseWriter();

		long start = lm.getEndOfLog();
		BeginCheckpointLog beginCheckpointLog = new BeginCheckpointLog();

		// create 3 transactions
		tp.beginTransaction();
		tp.beginTransaction();
		tp.beginTransaction();

		// pin 3 pages exclusively, modify them
		PageBuffer[] pb = new PageBuffer[3];
		for (int i = 0; i < 3; i++) {
			page.setPageId(i);
			pb[i] = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
			lm.writeRedo(beginCheckpointLog);
			LogPage.setPageLSN(pb[i], beginCheckpointLog.getAddress());
			bp.unPinDirty(pb[i], BufferPool.Affinity.LIKED, lm.getEndOfLog());
		}

		// pin the 3 pages exclusively again and hold the pins
		for (int i = 0; i < 3; i++) {
			page.setPageId(i);
			pb[i] = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
		}

		// take a checkpoint
		long master = lm.getEndOfLog();
		cpw.syncCheckPoint();

		// validate system key
		assertTrue(sk.getMaster() == master);
		assertTrue(sk.getNextTransId() == 4);

		// now read the log
		RedoLog log = new RedoLog();
		log.setAddress(sk.getMaster());
		lm.readRedo(log);
		assertTrue(log.getType() == Log.BEGIN_CHECKPOINT);
		log.setAddress(log.getAddress() + log.storeSize() + 3);
		lm.readRedo(log);
		assertTrue(log.getType() == Log.END_CHECKPOINT);
		RecoveryManager rm = new RecoveryManager(sk, lm, tp, tl, bp, cpw,
				new SimpleDelegate());
		rm.readCheckPoint(log);

		int cnt = 0;
		for (DPEntry dpe : rm.dirtyPages.values()) {
			assertTrue(dpe.getRecLSN() == start
					+ (cnt * (beginCheckpointLog.storeSize() + 3)));
			assertTrue(dpe.getFileId() == 1);
			assertTrue(dpe.getPageId() == cnt++);
		}
		assertTrue(cnt == 3);

		cnt = 1;
		for (TransactionState tte : rm.transTab.values()) {
			assertTrue(tte.getTransId() == cnt++);
		}
		assertTrue(cnt == 4);
	}

	/**
	 * Create a transaction, modify a page, log the update, take a checkpoint so
	 * that the update is flushed, kill the database prior to committing the
	 * transaction, start the database, invoke the RecoveryManager and verify
	 * that the update is gone (undone).
	 *
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws RecoveryException
	 * @throws LogExhaustedException
	 * @throws TransactionCancelledException
	 */
	@Test
	public void testUndo() throws BufferNotFound, InterruptedException,
			LogExhaustedException, RecoveryException,
			TransactionCancelledException {
		Transaction trans = tp.beginTransaction();
		page.setPageId(3);
		pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
		pb.putInt(333, 333);
		UndoLog undoLog = new UndoLog();
		RedoLog redoLog = new RedoLog();
		undoLog.setPage(page);
		redoLog.setPage(page);
		undoLog.setPageType((byte) 3);
		redoLog.setPageType((byte) 3);
		tl.writeUndoRedo(trans, pb, undoLog, redoLog);
		LogPage.setPageLSN(pb, redoLog.getAddress());
		bp.unPinDirty(pb, BufferPool.Affinity.HATED, redoLog.getAddress());
		cpw.syncCheckPoint();
		killDatabase();
		startDatabase();
		page.setPageId(3);
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		assertTrue(pb.getInt(333) == 333);
		bp.unPin(pb, BufferPool.Affinity.HATED);
		recover(new SimpleDelegate());
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		assertTrue(pb.getInt(333) == 0);
		bp.unPin(pb, BufferPool.Affinity.HATED);
	}

	private void recover(RecoveryContext delegate) throws BufferNotFound,
			LogExhaustedException, RecoveryException, InterruptedException {
		recover(delegate, null, null);
	}

	private void recover(RecoveryContext delegate, TransactionState trans,
			RedoLog redoLog) throws BufferNotFound, LogExhaustedException,
			InterruptedException, RecoveryException {
		RecoveryManager rm = new RecoveryManager(sk, lm, tp, tl, bp, cpw,
				delegate);

		rm.analyze();

		pb = bp.pin(page, BufferPool.Mode.SHARED);
		bp.unPin(pb, BufferPool.Affinity.HATED);

		if (rm.dirtyPages.size() > 0)
			rm.redo();

		delegate.didRedoPass();

		pb = bp.pin(page, BufferPool.Mode.SHARED);
		bp.unPin(pb, BufferPool.Affinity.HATED);

		if (rm.transTab.size() > 0)
			rm.undo();

		rm.restart();

		cpw.syncCheckPoint();

		// start checkpoint thread
		cpw.start();
	}

	/**
	 * Create a transaction, modify a page, log the update, take a checkpoint so
	 * that the update is flushed, rollback the transaction so that the update
	 * is undone in the buffer pool (but not saved to disk), write a CLR record
	 * to compensate for the undo, commit the transaction (including the undo),
	 * then kill the database prior to flushing the undone page buffer to disk,
	 * and finally invoke the RecoveryManager and verify that the update remains
	 * undone.
	 *
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws TransactionCancelledException
	 * @throws LogExhaustedException
	 * @throws RollbackException
	 * @throws RecoveryException
	 */
	@Test
	public void testRedoUndo() throws BufferNotFound, InterruptedException,
			TransactionCancelledException, LogExhaustedException,
			RollbackException, RecoveryException {
		Transaction trans = tp.beginTransaction();
		page.setPageId(3);
		pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
		pb.putInt(333, 333);
		UndoLog undoLog = new UndoLog();
		RedoLog redoLog = new RedoLog();
		undoLog.setPage(page);
		redoLog.setPage(page);
		undoLog.setPageType((byte) 3);
		redoLog.setPageType((byte) 3);
		tl.writeUndoRedo(trans, pb, undoLog, redoLog);
		LogPage.setPageLSN(pb, redoLog.getAddress());
		bp.unPinDirty(pb, BufferPool.Affinity.HATED, redoLog.getAddress());
		cpw.syncCheckPoint();
		tl.rollback(trans, null, new SimpleDelegate());
		tl.writeCommitLog(trans);
		killDatabase();
		startDatabase();
		page.setPageId(3);
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		assertTrue(pb.getInt(333) == 333);
		bp.unPin(pb, BufferPool.Affinity.HATED);
		recover(new SimpleDelegate());
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		assertTrue(pb.getInt(333) == 0);
		bp.unPin(pb, BufferPool.Affinity.HATED);
	}

	/**
	 * Create a transaction, modify a page, log the update, commit the
	 * transaction, kill the database prior to flushing the page buffers, start
	 * the database, invoke the RecoveryManager and verify that the update is
	 * restored.
	 *
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws LogExhaustedException
	 * @throws TransactionCancelledException
	 * @throws RecoveryException
	 */
	@Test
	public void testRedo() throws BufferNotFound, InterruptedException,
			LogExhaustedException, TransactionCancelledException,
			RecoveryException {
		Transaction trans = tp.beginTransaction();
		page.setPageId(3);
		pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
		pb.putInt(333, 333);
		UndoLog undoLog = new UndoLog();
		RedoLog redoLog = new RedoLog();
		undoLog.setPage(page);
		redoLog.setPage(page);
		undoLog.setPageType((byte) 3);
		redoLog.setPageType((byte) 3);
		tl.writeUndoRedo(trans, pb, undoLog, redoLog);
		bp.unPinDirty(pb, BufferPool.Affinity.LOVED, redoLog.getAddress());
		tl.writeCommitLog(trans);
		killDatabase();
		startDatabase();
		page.setPageId(3);
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		assertTrue(pb.getInt(333) == 0);
		bp.unPin(pb, BufferPool.Affinity.HATED);
		recover(new SimpleDelegate());
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		assertTrue(pb.getInt(333) == 333);
		bp.unPin(pb, BufferPool.Affinity.HATED);
	}

	/**
	 * Test the buffering, checkpointing and log writing functions when a single
	 * page hot spot is modified repeatedly and intensely by a large number of
	 * successive transactions.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 * @throws TransactionCancelledException
	 */
	@Test
	public void testHotSpot() throws BufferNotFound, LogExhaustedException,
			InterruptedException, TransactionCancelledException {
		page.setPageId(33);

		// read the integer at location 333 and increment it a
		// number of times per transaction, keeping track
		// of the total increments per transaction

		for (int c = 0; c < 1000; c++) {
			// begin 3 transactions
			for (int i = 0; i < trans.length; i++)
				trans[i] = tp.beginTransaction();

			// do a batch of 100 updates each time
			for (int i = 0; i < 100; i++) {
				int t = i % trans.length;
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				pb.putInt(333, pb.getInt(333) + 1);
				UndoLog undoLog = new UndoLog();
				RedoLog redoLog = new RedoLog();
				undoLog.setPage(page);
				redoLog.setPage(page);
				undoLog.setPageType((byte) 3);
				redoLog.setPageType((byte) 3);
				tl.writeUndoRedo(trans[t], pb, undoLog, redoLog);
				bp.unPinDirty(pb, BufferPool.Affinity.LOVED, redoLog
						.getAddress());
				++incs[t];
			}

			// commit the transactions, create 3 more
			for (int i = 0; i < trans.length; i++) {
				tl.writeCommitLog(trans[i]);
				tp.endTransaction(trans[i]);
			}
		}
	}

	/**
	 * Test the buffering, checkpointing and log writing functions when a single
	 * page hot spot is modified repeatedly and intensely by a large number of
	 * successive transactions.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 * @throws TransactionCancelledException
	 */
	@Test
	public void testHotSpotRandom() throws BufferNotFound,
			LogExhaustedException, InterruptedException,
			TransactionCancelledException {
		// pick a random page
		page.setPageId(random(100));

		// read the integer at location 333 and increment it a
		// random number of times per transaction, keeping track
		// of the total increments per transaction

		for (int c = 0; c < 100000; c++) {
			// begin 3 transactions
			for (int i = 0; i < trans.length; i++)
				trans[i] = tp.beginTransaction();

			// do a batch of 100 updates each time
			for (int i = 0; i < 100; i++) {
				int t = random(trans.length);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				pb.putInt(333, pb.getInt(333) + 1);
				UndoLog undoLog = new UndoLog();
				RedoLog redoLog = new RedoLog();
				undoLog.setPage(page);
				redoLog.setPage(page);
				undoLog.setPageType((byte) 3);
				redoLog.setPageType((byte) 3);
				tl.writeUndoRedo(trans[t], pb, undoLog, redoLog);
				bp.unPinDirty(pb, BufferPool.Affinity.LOVED, redoLog
						.getAddress());
				++incs[t];
			}

			// commit the transactions, create 3 more
			for (int i = 0; i < trans.length; i++) {
				tl.writeCommitLog(trans[i]);
				tp.endTransaction(trans[i]);
			}
		}
	}

	/**
	 * Test the rollback and recovery functions when a single page hot spot is
	 * modified repeatedly and intensely by a large number of successive
	 * transactions. Decide to abort a transaction, then validate the page
	 * following recovery.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws LogExhaustedException
	 * @throws RecoveryException
	 */
	@Test
	public void testRecovery() throws TransactionCancelledException,
			BufferNotFound, InterruptedException, LogExhaustedException,
			RecoveryException {
		// pick a page
		int pageId = 3;
		page.setPageId(pageId);

		// read the integer at location 333 and increment it a
		// number of times per transaction, keeping track
		// of the total increments per transaction
		Transaction trans;
		for (int c = 0; c < 100; c++) {
			// begin a new transaction
			trans = tp.beginTransaction();
			int incs = 0;

			// do a batch of upto 1000 updates (all increments ) each time
			RedoLog redoLog = null;
			int num = 10000;
			for (int i = 0; i < num; i++) {
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				pb.putInt(333, pb.getInt(333) + 1);
				UndoLog undoLog = new UndoLog();
				redoLog = new RedoLog();
				undoLog.setPage(page);
				redoLog.setPage(page);
				undoLog.setPageType((byte) 3);
				redoLog.setPageType((byte) 3);

				// attach the incs value to the log data
				++incs;
				LogData ld = redoLog.allocate((byte) 0, 4);
				ld.getData().putInt(incs);
				ld = undoLog.allocate((byte) 0, 4);
				ld.getData().putInt(incs);

				tl.writeUndoRedo(trans, pb, undoLog, redoLog);
				bp.unPinDirty(pb, BufferPool.Affinity.LOVED, redoLog
						.getAddress());
			}

			// kill the db, then restart
			int totalLostIncs = 0;

			// determine total of updates lost to uncommitted transactions
			if (!trans.isCommitted())
				totalLostIncs += incs;

			// calculate the expected count after the database is recovered
			pb = bp.pin(page, BufferPool.Mode.SHARED);
			int expected = pb.getInt(333) - totalLostIncs;
			bp.unPin(pb, BufferPool.Affinity.LOVED);

			killDatabase();
			startDatabase();

			page.setPageId(pageId);

			recover(new HotSpotDelegate(), trans, redoLog);

			// verify the count is now what we expected
			pb = bp.pin(page, BufferPool.Mode.SHARED);
			int found = pb.getInt(333);
			assertTrue(found == expected);
			bp.unPin(pb, BufferPool.Affinity.LOVED);
		}
	}

	/**
	 * Test the rollback and recovery functions when a single page hot spot is
	 * modified repeatedly and intensely by a large number of successive
	 * transactions. Randomly decide to abort a transaction, then validate the
	 * page following rollback.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws LogExhaustedException
	 * @throws RollbackException
	 * @throws RecoveryException
	 */
	@Test
	public void testRecovery2() throws TransactionCancelledException,
			BufferNotFound, InterruptedException, LogExhaustedException,
			RollbackException, RecoveryException {
		int pageId = 3;
		page.setPageId(pageId);

		// read the integer at location 333 and increment it a
		// random number of times per transaction, keeping track
		// of the total increments per transaction

		for (int c = 0; c < 10; c++) {
			// begin new transactions following committed ones
			for (int i = 0; i < trans.length; i++) {
				if (trans[i] == null || trans[i].isCommitted()) {
					trans[i] = tp.beginTransaction();
					incs[i] = 0;
				}
			}

			// write updates and rollback 10 times in a single transaction
			for (int r = 0; r < 100; r++) {
				int num = 100;
				for (int i = 0; i < num; i++) {
					int t = 0;
					pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
					pb.putInt(333, pb.getInt(333) + 1);
					UndoLog undoLog = new UndoLog();
					RedoLog redoLog = new RedoLog();
					undoLog.setPage(page);
					redoLog.setPage(page);
					undoLog.setPageType((byte) 3);
					redoLog.setPageType((byte) 3);

					++incs[t];

					tl.writeUndoRedo(trans[t], pb, undoLog, redoLog);
					bp.unPinDirty(pb, BufferPool.Affinity.LOVED, redoLog
							.getAddress());
				}

				int t = 0;

				// calculate the expected count after the transaction is
				// rolled back
				pb = bp.pin(page, BufferPool.Mode.SHARED);
				int expected = pb.getInt(333) - incs[t];
				bp.unPin(pb, BufferPool.Affinity.LOVED);

				// rollback the transaction
				tl.rollback(trans[t], null, new RandomRecoveryDelegate());

				// verify the count is now what we expected
				pb = bp.pin(page, BufferPool.Mode.SHARED);
				assertTrue(pb.getInt(333) == expected);
				bp.unPin(pb, BufferPool.Affinity.LOVED);

				incs[t] = 0;
			}

			// calculate the expected count after the database is recovered
			pb = bp.pin(page, BufferPool.Mode.SHARED);
			int expected = pb.getInt(333);
			bp.unPin(pb, BufferPool.Affinity.LOVED);

			killDatabase();
			startDatabase();

			page.setPageId(pageId);

			recover(new RandomRecoveryDelegate());

			// verify the count is now what we expected
			pb = bp.pin(page, BufferPool.Mode.SHARED);
			int found = pb.getInt(333);
			assertTrue(found == expected);
			bp.unPin(pb, BufferPool.Affinity.LOVED);
		}
	}

	/**
	 * Test the rollback and recovery functions when a single page hot spot is
	 * modified repeatedly and intensely by a large number of successive
	 * transactions. Randomly decide to abort a transaction, then validate the
	 * page following rollback.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws LogExhaustedException
	 * @throws RollbackException
	 * @throws RecoveryException
	 */
	@Test
	public void testRandomRecovery() throws TransactionCancelledException,
			BufferNotFound, InterruptedException, LogExhaustedException,
			RollbackException, RecoveryException {
		// pick a random page
		int pageId = random(100);
		page.setPageId(pageId);

		// read the integer at location 333 and increment it a
		// random number of times per transaction, keeping track
		// of the total increments per transaction

		for (int c = 0; c < 1000; c++) {
			// begin new transactions following committed ones
			for (int i = 0; i < trans.length; i++) {
				if (trans[i] == null || trans[i].isCommitted()) {
					trans[i] = tp.beginTransaction();
					incs[i] = 0;
				}
			}

			// do a random batch of upto 1000 updates (all increments ) each
			// time
			int num = random(10000);
			for (int i = 0; i < num; i++) {
				int t = random(trans.length);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				pb.putInt(333, pb.getInt(333) + 1);
				UndoLog undoLog = new UndoLog();
				RedoLog redoLog = new RedoLog();
				undoLog.setPage(page);
				redoLog.setPage(page);
				undoLog.setPageType((byte) 3);
				redoLog.setPageType((byte) 3);

				++incs[t];

				tl.writeUndoRedo(trans[t], pb, undoLog, redoLog);
				bp.unPinDirty(pb, BufferPool.Affinity.LOVED, redoLog
						.getAddress());
			}

			// choose a transaction to abort 20% of the time
			if (random(100) < 20) {
				// pick a random transaction
				int t = random(trans.length);

				// calculate the expected count after the transaction is
				// rolled back
				pb = bp.pin(page, BufferPool.Mode.SHARED);
				int expected = pb.getInt(333) - incs[t];
				bp.unPin(pb, BufferPool.Affinity.LOVED);

				// rollback the transaction
				tl.rollback(trans[t], null, new RandomRecoveryDelegate());

				// verify the count is now what we expected
				pb = bp.pin(page, BufferPool.Mode.SHARED);
				assertTrue(pb.getInt(333) == expected);
				bp.unPin(pb, BufferPool.Affinity.LOVED);

				incs[t] = 0;
			}

			// randomly kill the db, then restart
			if (random(100) < 5) {
				int totalLostIncs = 0;

				// commit the transactions randomly
				for (int i = 0; i < trans.length; i++) {
					if (random(100) < 20) {
						tl.writeCommitLog(trans[i]);
						tp.endTransaction(trans[i]);
					}
				}

				// determine total of updates lost to uncommitted
				// transactions
				for (int i = 0; i < trans.length; i++) {
					if (!trans[i].isCommitted())
						totalLostIncs += incs[i];
				}

				// calculate the expected count after the database is
				// recovered
				pb = bp.pin(page, BufferPool.Mode.SHARED);
				int expected = pb.getInt(333) - totalLostIncs;
				bp.unPin(pb, BufferPool.Affinity.LOVED);

				killDatabase();
				startDatabase();

				page.setPageId(pageId);

				recover(new RandomRecoveryDelegate());

				// verify the count is now what we expected
				pb = bp.pin(page, BufferPool.Mode.SHARED);
				int found = pb.getInt(333);
				assertTrue(found == expected);
				bp.unPin(pb, BufferPool.Affinity.LOVED);
			}

			// commit the transactions randomly
			for (int i = 0; i < trans.length; i++) {
				if (random(100) < 20) {
					if (trans[i] != null && !trans[i].isCommitted()) {
						tl.writeCommitLog(trans[i]);
						tp.endTransaction(trans[i]);
					}
				}
			}
		}
	}

	/**
	 * Test rollback.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 * @throws LogExhaustedException
	 * @throws RollbackException
	 */
	@Test
	public void testRollback() throws TransactionCancelledException,
			BufferNotFound, InterruptedException, LogExhaustedException,
			RollbackException {
		// pick a random page
		page.setPageId(3);

		// read the integer at location 333 and increment it a
		// random number of times per transaction, keeping track
		// of the total increments per transaction

		// begin new transactions following committed ones
		int i = 0;
		trans[i] = tp.beginTransaction();
		incs[i] = 0;
		int t = 0;
		pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
		pb.putInt(333, pb.getInt(333) + 1);
		UndoLog undoLog = new UndoLog();
		RedoLog redoLog = new RedoLog();
		undoLog.setPage(page);
		redoLog.setPage(page);
		undoLog.setPageType((byte) 3);
		redoLog.setPageType((byte) 3);

		// attach the incs value to the log data
		++incs[t];
		LogData ld = redoLog.allocate((byte) 0, 4);
		ld.getData().putInt(sum(incs));
		ld = undoLog.allocate((byte) 0, 4);
		ld.getData().putInt(sum(incs));

		tl.writeUndoRedo(trans[t], pb, undoLog, redoLog);
		bp.unPinDirty(pb, BufferPool.Affinity.LOVED, redoLog.getAddress());

		// calculate the expected count after the transaction is rolled back
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		int expected = pb.getInt(333) - incs[t];
		bp.unPin(pb, BufferPool.Affinity.LOVED);

		// rollback the transaction
		tl.rollback(trans[t], null, new HotSpotDelegate());

		// verify the count is now what we expected
		pb = bp.pin(page, BufferPool.Mode.SHARED);
		assertTrue(pb.getInt(333) == expected);
		bp.unPin(pb, BufferPool.Affinity.LOVED);
	}

	private int sum(int[] a) {
		int s = 0;
		for (int i = 0; i < a.length; i++)
			s += a[i];
		return s;
	}

	private void createDatabase() throws IOException {
		// create the files
		File file = new File("build/tmp/cooldb");
		file.mkdirs();

		FileChannel channel;
		files[0] = new File(System.getProperty("user.home") + "/test/test.redo");
		files[1] = new File(System.getProperty("user.home") + "/test/test.undo");
		files[2] = new File(System.getProperty("user.home") + "/test/test.key");
		files[3] = new File(System.getProperty("user.home") + "/test/test.db1");
		for (int i = 0; i < 4; i++) {
			raf[i] = new RandomAccessFile(files[i], "rw");
			channel = raf[i].getChannel();
			channel.truncate(0);
			dbf[i] = new DBFile(raf[i]);
			if (i == 2 || i == 1)
				dbf[i].extend(1);
			else
				dbf[i].extend(100);
		}
	}

	private void startDatabase() throws BufferNotFound, InterruptedException {
		// Create the file manager
		FileManager fm = new FileManagerImpl();

		// add the undo log file and the db file to the FileManager
		fm.addFile((short) 1, dbf[3]);

		// Create a buffer pool with 50 buffers
		bp = new BufferPoolImpl(fm);
		bp.start();

		// create the log writers, system key, checkpoint writer,
		// transaction and log managers
		redo = new RedoLogWriter(dbf[0]);
		undo = new UndoLogWriter(dbf[1]);
		undo.start();
		sk = new SystemKey(dbf[2]);
		cpw = new CheckpointWriter(50 * fm.getPageSize());
		lm = new LogManager(redo, undo);
		tl = new TransactionLogger(lm, cpw);
		tp = new TransactionPool(lm, 1);

		bp.setWriteAheadLogging(lm);
		bp.ensureCapacity(50);
		page = new FilePage();
		page.setFileId((short) 1);

		// start the checkpoint writer
		cpw.init(sk, lm, bp, tp);
	}

	private void killDatabase() {
		cpw.stop();
		bp.stop();
		undo.stop();
		redo.stop();
		sk.stop();

		cpw = null;
		bp = null;
		undo = null;
		redo = null;
		sk = null;
		lm = null;
		tl = null;
		tp = null;
		page = null;

		for (int i = 0; i < trans.length; i++) {
			trans[i] = null;
			incs[i] = 0;
		}

		System.gc();
	}

	private void removeDatabase() throws IOException {
		// close the files
		for (int i = 0; i < 4; i++) {
			raf[i].close();

			raf[i] = null;
			dbf[i] = null;
		}
		tp = null;
		lm = null;
		tl = null;
		page = null;
		bp = null;
		cpw = null;
		sk = null;

		System.gc();

		// remove the files
		for (int i = 0; i < 4; i++) {
			files[i].delete();
			files[i] = null;
		}
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	class SimpleDelegate implements RecoveryContext {
		public void redo(RedoLog log) {
			try {
				if (log.getType() == Log.CLR) {
					redoUndo(log);
					return;
				}
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				pb.putInt(333, 333);
				LogPage.setPageLSN(pb, log.getAddress());
				LogPage.setPageUndoNxtLSN(pb, log.getUndoNxtLSN());
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, log.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void didRedoPass() {
		}

		public void undo(UndoLog log, Transaction trans) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(trans.getTransId() == 1);
				assertTrue(log.getPageType() == 3);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				pb.putInt(333, 0);
				// write a CLR record and set pageLSN on buffer to the CLR
				// address
				CLR clr = new CLR(log);
				tl.writeCLR(trans, pb, clr);
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, clr.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void undo(UndoLog log, PageBuffer pb) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getTransID() == 1);
				assertTrue(log.getPageType() == 3);
				pb.putInt(333, 0);
				LogPage.setPageUndoNxtLSN(pb, log.getPageUndoNxtLSN());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void redoUndo(RedoLog log) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getTransID() == 1);
				assertTrue(log.getPageType() == 3);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				pb.putInt(333, 0);
				LogPage.setPageLSN(pb, log.getAddress());
				LogPage.setPageUndoNxtLSN(pb, log.getUndoNxtLSN());
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, log.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}
	}

	class HotSpotDelegate implements RecoveryContext {
		public void redo(RedoLog log) {
			try {
				if (log.getType() == Log.CLR) {
					redoUndo(log);
					return;
				}
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);

				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				int inc = pb.getInt(333) + 1;
				pb.putInt(333, inc);

				int val = log.getData().getData().getInt();

				assertTrue(inc == val);

				LogPage.setPageLSN(pb, log.getAddress());
				LogPage.setPageUndoNxtLSN(pb, log.getUndoNxtLSN());
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, log.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void didRedoPass() {
		}

		public void undo(UndoLog log, Transaction trans) {
			try {
				assertTrue(log.getUndoNxtLSN().getLSN() == 0
						|| log.getUndoNxtLSN().getLSN() == log.getAddress()
								.getLSN() - 1);
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				int dec = pb.getInt(333) - 1;
				pb.putInt(333, dec);
				int val = log.getData().getData().getInt() - 1;

				assertTrue(dec == val);

				// write a CLR record and set pageLSN on buffer to the CLR
				// address
				CLR clr = new CLR(log);
				tl.writeCLR(trans, pb, clr);
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, clr.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void undo(UndoLog log, PageBuffer pb) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);
				int dec = pb.getInt(333) - 1;
				pb.putInt(333, dec);

				int val = log.getData().getData().getInt() - 1;
				assertTrue(dec == val);

				LogPage.setPageUndoNxtLSN(pb, log.getPageUndoNxtLSN());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void redoUndo(RedoLog log) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				int dec = pb.getInt(333) - 1;
				pb.putInt(333, dec);
				LogPage.setPageLSN(pb, log.getAddress());
				LogPage.setPageUndoNxtLSN(pb, log.getUndoNxtLSN());
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, log.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}
	}

	class RandomRecoveryDelegate implements RecoveryContext {
		public void redo(RedoLog log) {
			try {
				if (log.getType() == Log.CLR) {
					redoUndo(log);
					return;
				}
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);

				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				int inc = pb.getInt(333) + 1;
				pb.putInt(333, inc);

				LogPage.setPageLSN(pb, log.getAddress());
				LogPage.setPageUndoNxtLSN(pb, log.getUndoNxtLSN());
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, log.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void didRedoPass() {
		}

		public void undo(UndoLog log, Transaction trans) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				int dec = pb.getInt(333) - 1;
				pb.putInt(333, dec);

				// write a CLR record and set pageLSN on buffer to the CLR
				// address
				CLR clr = new CLR(log);
				tl.writeCLR(trans, pb, clr);
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, clr.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void undo(UndoLog log, PageBuffer pb) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);
				int dec = pb.getInt(333) - 1;
				pb.putInt(333, dec);

				LogPage.setPageUndoNxtLSN(pb, log.getPageUndoNxtLSN());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}

		public void redoUndo(RedoLog log) {
			try {
				assertTrue(log.getPage().equals(page));
				assertTrue(log.getPageType() == 3);
				pb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
				int dec = pb.getInt(333) - 1;
				pb.putInt(333, dec);
				LogPage.setPageLSN(pb, log.getAddress());
				LogPage.setPageUndoNxtLSN(pb, log.getUndoNxtLSN());
				bp.unPinDirty(pb, BufferPool.Affinity.HATED, log.getAddress());
			} catch (Throwable t) {
				Assert.fail(t.getMessage());
			}
		}
	}

	private TransactionPool tp;
	private UndoLogWriter undo;
	private RedoLogWriter redo;
	private LogManager lm;
	private TransactionLogger tl;
	private BufferPool bp;
	private CheckpointWriter cpw;
	private SystemKey sk;
	private RandomAccessFile[] raf = new RandomAccessFile[4];
	private File[] files = new File[4];
	private DBFile[] dbf = new DBFile[4];
	private FilePage page;
	private PageBuffer pb;
	private Transaction[] trans = new Transaction[3];
	private int[] incs = new int[trans.length];
}
