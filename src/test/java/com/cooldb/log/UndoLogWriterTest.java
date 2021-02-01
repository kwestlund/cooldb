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

package com.cooldb.log;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import com.cooldb.buffer.FilePage;
import com.cooldb.buffer.BufferNotFound;
import com.cooldb.buffer.DBFile;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class UndoLogWriterTest {

	@Before
	public void setUp() throws IOException, BufferNotFound,
			InterruptedException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();

		// create the log file
		String fileName = System.getProperty("user.home") + "/test/test.db";
		raf = new RandomAccessFile(fileName, "rw");
		FileChannel channel = raf.getChannel();
		channel.truncate(0);
		DBFile dbf = new DBFile(raf);
		dbf.extend(1);

		// create the log writer
		lw = new UndoLogWriter(dbf);
		lw.create();
	}

	@After
	public void tearDown() throws IOException {
		lw.stop();

		// remove the file
		raf.close();

		raf = null;
		lw = null;

		System.gc();

		String fileName = System.getProperty("user.home") + "/test/test.db";
		File file = new File(fileName);
		file.delete();
	}

	/**
	 * Write a commit log record and read it back again.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 * @throws LogNotFoundException
	 */
	@Test
	public void testSimpleReadWrite() throws BufferNotFound,
			LogExhaustedException, InterruptedException, LogNotFoundException {
		UndoLog log = new UndoLog();
		log.setTransID(3);
		lw.write(log);
		lw.read(log);
		assertTrue(log.getTransID() == 3);
	}

	/**
	 * Write enough undoLogs to cause extent allocation.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testExtentAllocation() throws BufferNotFound,
			LogExhaustedException, InterruptedException {
		UndoPointer first = lw.getEndOfLog();
		UndoLog log = new UndoLog();
		log.allocate((byte) 0, 0);
		int writes = 10000;
		for (int i = 0; i < writes; i++) {
			log.setTransID(i);
			lw.write(log);
		}

		// now scan all the log records from the first one
		int transId = 0;
		log.setAddress(first);
		Iterator<UndoLog> iter = lw.iterator(log);
		while (iter.hasNext()) {
			iter.next();

			assertTrue(log.getTransID() == transId);

			transId++;
		}
	}

	/**
	 * Write enough undoLogs to require an additional extent allocation, then
	 * advance the minUndo pointer to initiate garbage collection of the first
	 * extent, then write enough undoLogs to require another extent allocation,
	 * and repeat several times.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testGarbageCollection() throws BufferNotFound,
			LogExhaustedException, InterruptedException {
		// Write a batch of undoLogs to cause extent allocation
		testExtentAllocation();

		// Advance the minUndo pointer to the current endOfLog
		lw.setMinUndo(lw.getEndOfLog());

		// Write another batch of undoLogs to cause extent allocation again
		testExtentAllocation();
	}

	/**
	 * Exercise the UndoLogWriter. Write updates to the log, then read one of
	 * them and validate it, then commit them, then write some more records,
	 * then read the same one and validate again, then move the minUndo pointer
	 * to reclaim space, and repeat.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 * @throws LogNotFoundException
	 */
	@Test
	public void testExercise() throws BufferNotFound, LogExhaustedException,
			InterruptedException, LogNotFoundException {
		long savedTransId = -1;
		UndoLog savedLog = new UndoLog();
		FilePage page3 = new FilePage((short) 3, 3);
		savedLog.setPage(page3);
		UndoLog log = new UndoLog();
		log.setPage(page3);
		log.setPageUndoNxtLSN(new UndoPointer(page3, (short) 0, 0));
		for (int i = 0; i < 10000; i++) {
			for (int j = 0; j < 10; j++) {
				log.allocate((byte) 0, random(100));
				LogData ld = log.allocate((byte) 0, random(100) + 10);
				ld.getData().putInt(3, i);
				log.setTransID(i);
				lw.write(log);
				log.freeData();

				if (random(100) == 1) {
					if (savedTransId == -1) {
						savedTransId = i;
						savedLog.setAddress(log.getAddress());
					}
				}
			}

			if (savedTransId != -1) {
				lw.read(savedLog);
				assertTrue(savedLog.getTransID() == savedTransId);
				LogData ld = savedLog.getData().next();
				assertTrue(ld.getData().getInt(3) == savedTransId);
				assertTrue(savedLog.getPageUndoNxtLSN().getPage().equals(page3));
				savedLog.setTransID(0);
			}

			if (random(100) < 20)
				lw.flush();

			if (random(100) < 10) {
				savedTransId = -1;
				lw.setMinUndo(lw.getEndOfLog());
			}
		}
	}

	@Test
	public void testExercise2() throws BufferNotFound, LogExhaustedException,
			InterruptedException, LogNotFoundException {
		long savedTransId = -1;
		UndoLog savedLog = new UndoLog();
		FilePage page3 = new FilePage((short) 3, 3);
		savedLog.setPage(page3);
		UndoLog log = new UndoLog();
		log.setPage(page3);
		log.setPageUndoNxtLSN(new UndoPointer(page3, (short) 0, 0));
		for (int i = 0; i < 1000000; i++) {
			log.allocate((byte) 0, random(100));
			LogData ld = log.allocate((byte) 0, random(100) + 10);
			ld.getData().putInt(3, i);
			log.setTransID(i);
			lw.write(log);
			log.freeData();

			if (random(10) == 1) {
				if (savedTransId == -1) {
					savedTransId = i;
					savedLog.setAddress(log.getAddress());
				}
			}

			if (savedTransId != -1) {
				lw.read(savedLog);
				assertTrue(savedLog.getTransID() == savedTransId);
				ld = savedLog.getData().next();
				assertTrue(ld.getData().getInt(3) == savedTransId);
				assertTrue(savedLog.getPageUndoNxtLSN().getPage().equals(page3));
				savedLog.setTransID(0);
			}

			if (random(1000) == 1)
				savedTransId = -1;

			if (random(100) < 20)
				lw.flush();

			if (i % 10000 == 0) {
				lw.setMinUndo(lw.getEndOfLog());
				savedTransId = -1;
			}
		}
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	private UndoLogWriter lw;
	private RandomAccessFile raf;
}
