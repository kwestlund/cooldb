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

import com.cooldb.buffer.DBFile;
import com.cooldb.buffer.FileManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.util.Iterator;

import static org.junit.Assert.assertTrue;

public class RedoLogWriterTest {

	@Before
	public void setUp() throws IOException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();

		// create the log file
		String fileName = "build/tmp/cooldb/test/test.db";
		raf = new RandomAccessFile(fileName, "rw");
		FileChannel channel = raf.getChannel();
		channel.truncate(0);
		DBFile dbf = new DBFile(raf);
		dbf.extend(500);

		// create the log writer
		lw = new RedoLogWriter(dbf);
	}

	@After
	public void tearDown() throws IOException {
		// remove the file
		raf.close();

		raf = null;
		lw = null;

		System.gc();

		String fileName = "build/tmp/cooldb/test/test.db";
		File file = new File(fileName);
		file.delete();
	}

	/**
	 * Write a commit log record and read it back again.
	 *
	 * @throws LogExhaustedException
	 * @throws LogNotFoundException
	 */
	@Test
	public void testSimpleReadWrite() throws LogExhaustedException,
			LogNotFoundException {
		RedoLog log = new CommitLog(3);
		lw.write(log);
		lw.flushTo(log.getAddress());
		lw.read(log);
		assertTrue(log.getTransID() == 3);
	}

	/**
	 * Write commit logs until the log is full, then make sure the next write
	 * throws a LogExhaustedException, then advance the DoNotOverwrite mark,
	 * then write again, then read the record, then scan all records.
	 *
	 * @throws Exception
	 */
	@Test
	public void testWrapAroundWrite() throws Exception {
		// # writes to fill log = ((500 * FileManager.DEFAULT_PAGE_SIZE) -
		// overhead) / (log.storeSize() + overhead)
		// log size = 28 + logdata
		// logdata size = 5 + allocated size
		// redolog overhead = 8 bytes
		// total size = 41 bytes
		int overhead = RedoLogWriter.OVERHEAD - 1;
		RedoLog log = new CommitLog(0);
		log.allocate((byte) 0, 0);
		int writes = ((500 * FileManager.DEFAULT_PAGE_SIZE) - overhead)
				/ (log.storeSize() + overhead);
		for (int i = 0; i < writes; i++) {
			log.setTransID(i);
			lw.write(log);
		}
		// try to write again after filling the log
		log.setTransID(writes);
		try {
			lw.write(log);
			throw new Exception("Expected LogExhaustedException");
		} catch (LogExhaustedException lee) {
		}

		long firewall = lw.getDoNotOverwrite() + 100
				* (log.storeSize() + overhead) + 1;

		// advance do-not-overwrite mark by 1000 bytes (plus 1 because we
		// start at lsn 1, not zero)
		lw.moveFirewallTo(firewall);

		// now write again causing wrap around to beginning of file
		lw.write(log);

		lw.flushTo(log.getAddress());

		// now scan all the log records
		long address = 100 * (log.storeSize() + overhead) + 1;
		long transId = address / (log.storeSize() + overhead);
		log.setAddress(address);
		Iterator<RedoLog> iter = lw.iterator(log);
		while (iter.hasNext()) {
			iter.next();

			assertTrue(log.getTransID() == transId);

			transId++;
		}
		assertTrue(log.getTransID() == writes);
		assertTrue(log.getAddress() == 500 * FileManager.DEFAULT_PAGE_SIZE
				+ log.storeSize() + overhead);

		// try reading the 1st log record, which had been overwritten
		log.setAddress(1);
		try {
			lw.read(log);
			throw new Exception("Expected LogNotFoundException");
		} catch (LogNotFoundException lnfe) {
		}
		// try reading the 2nd log record, which has not been overwritten
		log.setAddress(log.storeSize() + overhead);
		lw.read(log);
	}

	/**
	 * Exercise the RedoLogWriter.
	 *
	 * @throws LogExhaustedException
	 * @throws LogNotFoundException
	 */
	@Test
	public void testExercise() throws LogExhaustedException,
			LogNotFoundException {
		long savedTransId = -1;
		RedoLog savedLog = new RedoLog();
		RedoLog log = new RedoLog();
		for (int i = 0; i < 1000000; i++) {
			log.allocate((byte) 0, random(100));
			LogData ld = log.allocate((byte) 0, random(100) + 10);
			ld.getData().putInt(3, i);
			log.setTransID(i);
			try {
				lw.write(log);
			} catch (LogExhaustedException lee) {
				long firewall = log.getAddress();
				lw.flushTo(firewall);
				lw.setDoNotOverwrite(firewall);
				lw.write(log);
				savedTransId = -1;
			}
			log.freeData();
			if (random(10) == 1) {
				if (savedTransId == -1) {
					savedTransId = i;
					savedLog.setAddress(log.getAddress());
				} else {
					lw.flushTo(savedLog.getAddress());
					lw.read(savedLog);
					assertTrue(savedLog.getTransID() == savedTransId);
					ld = savedLog.getData().next();
					assertTrue(ld.getData().getInt(3) == savedTransId);
					savedLog.setTransID(0);
				}
			}
		}
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	private RedoLogWriter lw;
	private RandomAccessFile raf;
}
