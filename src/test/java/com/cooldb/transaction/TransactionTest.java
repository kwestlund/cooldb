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

package com.cooldb.transaction;

import com.cooldb.buffer.FilePage;
import com.cooldb.log.UndoPointer;
import com.cooldb.log.WAL;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

import static org.junit.Assert.assertTrue;

public class TransactionTest implements WAL {

	@Before
	public void setUp() {
		dd = new DeadlockDetector();
		System.gc();
		endOfLog = 0;
	}

	/**
	 * Test MasterCommitList and CommitList.
	 */
	@Test
	public void testCommitList() {
		MasterCommitList clist = new MasterCommitList(10, new UndoPointer(0));

		clist.enlist(10);
		clist.enlist(11);
		clist.enlist(12);
		assertTrue(clist.isCommitted(9));
		assertTrue(!clist.isCommitted(10));
		assertTrue(!clist.isCommitted(11));
		assertTrue(!clist.isCommitted(12));
		assertTrue(clist.getBaseTransId() == 10);
		assertTrue(clist.getCommitLSN().getLSN() == 0);
		assertTrue(clist.getCount() == 3);
		clist.commit(11);
		clist.setCommitLSN(new UndoPointer(333));
		clist.setCommitTransId(clist.getBaseTransId());
		assertTrue(clist.isCommitted(11));
		assertTrue(clist.getBaseTransId() == 10);
		assertTrue(clist.getCommitLSN().getLSN() == 333);
		assertTrue(clist.getCount() == 3);
		CommitList cl = clist.copy();
		assertTrue(cl.isCommitted(9));
		assertTrue(!cl.isCommitted(10));
		assertTrue(cl.isCommitted(11));
		assertTrue(!cl.isCommitted(12));
		assertTrue(cl.getBaseTransId() == 10);
		assertTrue(cl.getCommitLSN().getLSN() == 333);
		assertTrue(clist.getCount() == 3);
		clist.commit(10);
		clist.setCommitLSN(new UndoPointer(334));
		clist.setCommitTransId(clist.getBaseTransId());
		assertTrue(clist.isCommitted(9));
		assertTrue(clist.isCommitted(10));
		assertTrue(clist.isCommitted(11));
		assertTrue(!clist.isCommitted(12));
		assertTrue(clist.getBaseTransId() == 12);
		assertTrue(clist.getCommitLSN().getLSN() == 334);
		assertTrue(clist.getCount() == 1);
		assertTrue(cl.isCommitted(9));
		assertTrue(!cl.isCommitted(10));
		assertTrue(cl.isCommitted(11));
		assertTrue(!cl.isCommitted(12));
		assertTrue(cl.getBaseTransId() == 10);
		assertTrue(cl.getCommitLSN().getLSN() == 333);
		assertTrue(cl.getCount() == 3);
		clist.commit(12);
		clist.setCommitLSN(new UndoPointer(335));
		clist.setCommitTransId(clist.getBaseTransId());
		assertTrue(clist.isCommitted(9));
		assertTrue(clist.isCommitted(10));
		assertTrue(clist.isCommitted(11));
		assertTrue(clist.isCommitted(12));
		assertTrue(clist.getBaseTransId() == 13);
		assertTrue(clist.getCommitLSN().getLSN() == 335);
		assertTrue(clist.getCount() == 0);
		clist.enlist(13);
		clist.enlist(14);
		clist.enlist(15);
		assertTrue(clist.isCommitted(12));
		assertTrue(!clist.isCommitted(13));
		assertTrue(!clist.isCommitted(14));
		assertTrue(!clist.isCommitted(15));
		assertTrue(clist.getBaseTransId() == 13);
		assertTrue(clist.getCommitLSN().getLSN() == 335);
		assertTrue(clist.getCount() == 3);
		clist.commit(15);
		clist.setCommitLSN(new UndoPointer(336));
		clist.setCommitTransId(clist.getBaseTransId());
		assertTrue(clist.isCommitted(12));
		assertTrue(!clist.isCommitted(13));
		assertTrue(!clist.isCommitted(14));
		assertTrue(clist.isCommitted(15));
		assertTrue(clist.getBaseTransId() == 13);
		assertTrue(clist.getCommitLSN().getLSN() == 336);
		assertTrue(clist.getCount() == 3);

		clist = new MasterCommitList(0, new UndoPointer(0));

		// exercise commit list
		for (int i = 0; i < 1000; i++) {
			int j = 0;
			assertTrue(clist.getBaseTransId() == i * 1000);
			for (j = 0; j < 1000; j++)
				clist.enlist(i * 1000 + j);
			assertTrue(clist.getBaseTransId() == i * 1000);
			assertTrue(clist.getCount() == 1000);
			while (clist.getBaseTransId() != (i * 1000 + j)) {
				int t = i * 1000 + random(j);
				if (!clist.isCommitted(t)) {
					clist.commit(t);
					clist.setCommitLSN(new UndoPointer(0));
					clist.setCommitTransId(clist.getBaseTransId());
					if (clist.getBaseTransId() == t + 1)
						assertTrue(clist.getCount() == j - (t % 1000 + 1));
				}
			}
			assertTrue(clist.getBaseTransId() == i * 1000 + j);
			assertTrue(clist.getCount() == 0);
		}
	}

	/**
	 * Test Transaction.
	 *
	 * @throws TransactionCancelledException
	 */
	@Test
	public void testTransaction() throws TransactionCancelledException {
		TransactionPool tm = new TransactionPool(this, 10);

		// create a transaction
		Transaction trans = tm.beginTransaction();
		trans.setFirstLSN(new UndoPointer(333));
		trans.setUndoNxtLSN(new UndoPointer(999));
		assertTrue(trans.getTransId() == 10);

		// marshall/unmarshall the transaction state
		ByteBuffer bb = ByteBuffer.allocate(trans.storeSize());
		trans.writeTo(bb);
		TransactionState tte = new TransactionState();
		bb.rewind();
		tte.readFrom(bb);
		assertTrue(trans.getTransId() == 10);
		assertTrue(trans.getFirstLSN().getLSN() == 333);
		assertTrue(trans.getUndoNxtLSN().getLSN() == 999);
	}

	/**
	 * Test TransactionPool.
	 *
	 * @throws TransactionCancelledException
	 */
	@Test
	public void testTransactionPool() throws TransactionCancelledException {
		endOfLog = 1;
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		t1
				.setFirstLSN(new UndoPointer(new FilePage((short) 0, 0),
						(short) 0, 1));
		endOfLog = 2;
		assertTrue(t1.getCommitLSN().getLSN() == 1);
		assertTrue(tm.calcMinCommitLSN().getLSN() == 1);
		Transaction t2 = tm.beginTransaction();
		t2
				.setFirstLSN(new UndoPointer(new FilePage((short) 0, 0),
						(short) 0, 2));
		endOfLog = 3;
		assertTrue(t2.getCommitLSN().getLSN() == 1);
		assertTrue(tm.calcMinCommitLSN().getLSN() == 1);
		tm.endTransaction(t1);
		Transaction t3 = tm.beginTransaction();
		t3
				.setFirstLSN(new UndoPointer(new FilePage((short) 0, 0),
						(short) 0, 3));
		endOfLog = 4;
		assertTrue(t2.getCommitLSN().getLSN() == 1);
		assertTrue(t3.getCommitLSN().getLSN() == 2);
		assertTrue(tm.calcMinCommitLSN().getLSN() == 1);
		tm.endTransaction(t2);
		Transaction t4 = tm.beginTransaction();
		assertTrue(t3.getCommitLSN().getLSN() == 2);
		assertTrue(t4.getCommitLSN().getLSN() == 3);
		assertTrue(tm.calcMinCommitLSN().getLSN() == 2);

		TransactionState[] active = tm.getActiveTransactions();
		assertTrue(active.length == 2);
	}

	/**
	 * Create several transactions in separate threads and call the waitsFor
	 * method of DeadlockDetector, then verify that the deadlock is detected.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 */
	@Test
	public void testDeadlockDetector() throws TransactionCancelledException,
			InterruptedException {
		endOfLog = 1;
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();
		Transaction t3 = tm.beginTransaction();

		t1.addRollbackCost(100);
		t3.addRollbackCost(100);

		ConflictThread ct1 = new ConflictThread(t1, t2);
		ct1.start();
		ConflictThread ct2 = new ConflictThread(t2, t3);
		ct2.start();
		ConflictThread ct3 = new ConflictThread(t3, t1);
		ct3.start();

		for (int i = 0; i < 100 && !ct1.success; i++)
			Thread.sleep(10);
		assertTrue(ct1.success);

		for (int i = 0; i < 100 && !ct2.failed; i++)
			Thread.sleep(10);
		assertTrue(ct2.failed);

		for (int i = 0; i < 100 && !ct3.success; i++)
			Thread.sleep(15);
		assertTrue(ct3.success);
	}

	// WriteAheadLogging (no-op)
	public long flushTo(long lsn) {
		return endOfLog;
	}

	public long getEndOfLog() {
		return endOfLog;
	}

	public UndoPointer getEndOfUndoLog() {
		return new UndoPointer(new FilePage((short) 0, 0), (short) 0, endOfLog);
	}

	public int getRemaining(long recLSN) {
		return 1000000;
	}

	class ConflictThread extends Thread {
		ConflictThread(Transaction trans, Transaction holder) {
			this.trans = trans;
			this.holder = holder;
		}

		@Override
		public void run() {
			try {
				dd.waitFor(trans, holder);
				success = true;
			} catch (Exception e) {
				failed = true;
			}
			trans.setCommitted(true);
			dd.didCommit(trans);
		}

		Transaction trans;
		Transaction holder;
		boolean failed;
		boolean success;
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	private long endOfLog;
	private DeadlockDetector dd;
}
