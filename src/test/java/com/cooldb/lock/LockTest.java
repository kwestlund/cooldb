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

package com.cooldb.lock;

import com.cooldb.buffer.FilePage;
import com.cooldb.log.UndoPointer;
import com.cooldb.log.WAL;
import com.cooldb.transaction.DeadlockDetector;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionCancelledException;
import com.cooldb.transaction.TransactionPool;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class LockTest implements WAL {

	@Before
	public void setUp() {
		dd = new DeadlockDetector();
		System.gc();
		endOfLog = 0;
	}

	/**
	 * Grab read lock, read lock, write lock and verify that the first two
	 * succeed while the last fails. Then commit the first one and verify that
	 * the writer is still waiting, then commit the second reader and verify
	 * that the writer has succeeded.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 */
	@Test
	public void testWriterWaitsForReaders()
			throws TransactionCancelledException, InterruptedException {
		endOfLog = 1;
		ResourceLock rlock = new ResourceLock(dd);
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();
		Transaction t3 = tm.beginTransaction();

		LockThread ct1 = new LockThread(rlock.readLock(t1));
		ct1.start();
		Thread.sleep(100);
		LockThread ct2 = new LockThread(rlock.readLock(t2));
		ct2.start();
		Thread.sleep(100);
		LockThread ct3 = new LockThread(rlock.writeLock(t3));
		ct3.start();
		Thread.sleep(100);

		assertTrue(ct1.success);
		assertTrue(ct2.success);
		assertTrue("ct3 failed", !ct3.failed);
		assertTrue("ct3 successful but not supposed to be", !ct3.success);
		assertTrue("ct3 not waiting but is supposed to be", ct3.waiting);

		commit(t2);
		Thread.sleep(100);

		assertTrue(ct3.waiting);

		commit(t1);
		Thread.sleep(100);

		assertTrue("ct3 not successful", ct3.success);
		assertTrue("ct3 failed", !ct3.failed);
		assertTrue("ct3 still waiting", !ct3.waiting);
	}

	/**
	 * Grab write lock, read lock, read lock and verify that the first one
	 * succeeds while the last two fail. Then commit the first one and verify
	 * that both readers have succeeded.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 */
	@Test
	public void testReadersWaitForWriter()
			throws TransactionCancelledException, InterruptedException {
		endOfLog = 1;
		ResourceLock rlock = new ResourceLock(dd);
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();
		Transaction t3 = tm.beginTransaction();

		LockThread ct1 = new LockThread(rlock.writeLock(t1));
		ct1.start();
		Thread.sleep(100);
		LockThread ct2 = new LockThread(rlock.readLock(t2));
		ct2.start();
		Thread.sleep(100);
		LockThread ct3 = new LockThread(rlock.readLock(t3));
		ct3.start();
		Thread.sleep(100);

		assertTrue(ct1.success);
		assertTrue("ct2 failed", !ct2.failed);
		assertTrue("ct2 successful but not supposed to be", !ct2.success);
		assertTrue("ct2 not waiting but is supposed to be", ct2.waiting);
		assertTrue("ct3 failed", !ct3.failed);
		assertTrue("ct3 successful but not supposed to be", !ct3.success);
		assertTrue("ct3 not waiting but is supposed to be", ct3.waiting);

		commit(t1);
		Thread.sleep(100);

		assertTrue(ct2.success);
		assertTrue(ct3.success);
	}

	/**
	 * Grab several read locks on the same resource by the same transaction.
	 * Grab several write locks on the same resource by the same transaction.
	 * Grab a read lock after the write locks. Grab a write lock several times,
	 * then using a second and third transaction wait for read and a write lock
	 * respectively, then using the first transaction make sure it can still
	 * re-acquire both read and write locks.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 */
	@Test
	public void testReentrant() throws TransactionCancelledException,
			InterruptedException {
		endOfLog = 1;
		ResourceLock rlock = new ResourceLock(dd);
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();
		Transaction t3 = tm.beginTransaction();

		rlock.readLock(t1).lock();
		rlock.readLock(t1).lock();
		rlock.readLock(t1).lock();

		commit(t1);
		t1 = tm.beginTransaction();

		rlock.writeLock(t1).lock();
		rlock.writeLock(t1).lock();
		rlock.writeLock(t1).lock();
		rlock.readLock(t1).lock();
		rlock.readLock(t1).lock();
		rlock.readLock(t1).lock();

		commit(t1);
		t1 = tm.beginTransaction();

		rlock.writeLock(t1).lock();
		rlock.writeLock(t1).lock();
		rlock.writeLock(t1).lock();
		rlock.readLock(t1).lock();
		rlock.readLock(t1).lock();
		rlock.readLock(t1).lock();

		LockThread ct2 = new LockThread(rlock.readLock(t2), t2);
		ct2.start();
		Thread.sleep(100);
		LockThread ct3 = new LockThread(rlock.writeLock(t3), t3);
		ct3.start();
		Thread.sleep(100);

		assertTrue("ct2 failed", !ct2.failed);
		assertTrue("ct2 successful but not supposed to be", !ct2.success);
		assertTrue("ct2 not waiting but is supposed to be", ct2.waiting);
		assertTrue("ct3 failed", !ct3.failed);
		assertTrue("ct3 successful but not supposed to be", !ct3.success);
		assertTrue("ct3 not waiting but is supposed to be", ct3.waiting);

		rlock.readLock(t1).lock();
		rlock.writeLock(t1).lock();

		assertTrue(ct2.waiting);
		assertTrue(ct3.waiting);

		commit(t1);
		Thread.sleep(100);

		assertTrue(ct2.success && ct3.success);
	}

	/**
	 * Grab a read lock, then a write lock, which should succeed. Grab a read
	 * lock, then in a second transaction grab another read lock, then try to
	 * acquire a write lock for the first transaction, which should wait for the
	 * second to finish. Grab a read lock, then in a second transaction try to
	 * acquire a write lock, then grab a write lock for the first transaction,
	 * which should succeed.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 */
	@Test
	public void testConvert() throws TransactionCancelledException,
			InterruptedException {
		endOfLog = 1;
		ResourceLock rlock = new ResourceLock(dd);
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();

		rlock.readLock(t1).lock();
		rlock.writeLock(t1).lock();

		commit(t1);
		t1 = tm.beginTransaction();

		rlock.readLock(t1).lock();
		rlock.readLock(t2).lock();

		LockThread ct1 = new LockThread(rlock.writeLock(t1));
		ct1.start();
		Thread.sleep(100);

		assertTrue("ct1 should be waiting", ct1.waiting);

		commit(t2);
		Thread.sleep(100);

		assertTrue(ct1.success);
		commit(t1);

		t1 = tm.beginTransaction();
		t2 = tm.beginTransaction();

		rlock.readLock(t1).lock();

		LockThread ct2 = new LockThread(rlock.writeLock(t2));
		ct2.start();
		Thread.sleep(100);

		assertTrue("ct2 should be waiting", ct2.waiting);
		rlock.writeLock(t1).lock();
		Thread.sleep(100);
		assertTrue("ct2 should be waiting", ct2.waiting);

		commit(t1);
		Thread.sleep(100);

		assertTrue("ct2 should not be waiting", !ct2.waiting);
		assertTrue("ct2 should have been successful", ct2.success);
	}

	/**
	 * Use three resources for this test: T1 locks R1, T2 locks R2, T3 locks R3,
	 * then T1 tries to lock R2, T2 tries to lock R3, and T3 tries to lock T1,
	 * at which point a deadlock exception should be thrown.
	 *
	 * @throws TransactionCancelledException
	 * @throws InterruptedException
	 */
	@Test
	public void testDeadlock() throws TransactionCancelledException,
			InterruptedException {
		endOfLog = 1;
		ResourceLock r1 = new ResourceLock(dd);
		ResourceLock r2 = new ResourceLock(dd);
		ResourceLock r3 = new ResourceLock(dd);
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();
		Transaction t3 = tm.beginTransaction();

		r1.writeLock(t1).lock();
		r2.writeLock(t2).lock();
		r3.writeLock(t3).lock();

		LockThread ct1 = new LockThread(r2.readLock(t1));
		ct1.start();
		Thread.sleep(100);
		LockThread ct2 = new LockThread(r3.readLock(t2));
		ct2.start();
		Thread.sleep(100);
		LockThread ct3 = new LockThread(r1.readLock(t3));
		ct3.start();
		Thread.sleep(100);

		assertTrue("ct1 should be waiting", ct1.waiting);
		assertTrue("ct2 should be waiting", ct2.waiting);
		assertTrue("ct3 should have failed", ct3.failed);

		commit(t3);
		Thread.sleep(100);
		assertTrue("ct2 should be successful", ct2.success);

		commit(t2);
		Thread.sleep(100);
		assertTrue("ct1 should be successful", ct1.success);
	}

	/**
	 * Use T1 to grab a write lock, then use T2 to try to grab a write lock.
	 *
	 * @throws TransactionCancelledException
	 */
	@Test
	public void testTryLock() throws TransactionCancelledException {
		ResourceLock rlock = new ResourceLock(dd);
		TransactionPool tm = new TransactionPool(this, 1);
		Transaction t1 = tm.beginTransaction();
		Transaction t2 = tm.beginTransaction();

		rlock.readLock(t1).lock();

		assertFalse(rlock.writeLock(t2).trylock());
		assertTrue(rlock.readLock(t2).trylock());
	}

	private void commit(Transaction trans) {
		// release locks and inform the deadlock detector
		trans.releaseLocks();
		dd.didCommit(trans);
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

	class LockThread extends Thread {
		LockThread(Lock lock) {
			this.lock = lock;
		}

		LockThread(Lock lock, Transaction trans) {
			this.lock = lock;
			this.trans = trans;
		}

		@Override
		public void run() {
			waiting = true;
			try {
				lock.lock();
				success = true;
			} catch (Exception e) {
				failed = true;
			} finally {
				waiting = false;
				if (trans != null)
					commit(trans);
			}
		}

		Lock lock;
		Transaction trans;
		boolean failed;
		boolean success;
		boolean waiting;
	}

	private long endOfLog;
	private DeadlockDetector dd;
}
