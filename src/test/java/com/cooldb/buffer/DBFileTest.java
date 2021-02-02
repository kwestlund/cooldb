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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertTrue;

public class DBFileTest {

	@Before
	public void setUp() throws IOException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();
		raf = new RandomAccessFile("build/tmp/cooldb/test.db", "rw");
		FileChannel channel = raf.getChannel();
		channel.truncate(0);
		dbf = new DBFile(raf);
		bb = ByteBuffer.allocate(dbf.getPageSize());
	}

	@After
	public void tearDown() throws IOException {
		// remove the file
		raf.close();
		String fileName = "build/tmp/cooldb/test.db";
		File file = new File(fileName);
		file.delete();

		dbf = null;
		bb = null;
		raf = null;

		System.gc();
	}

	/**
	 * Extend the file by 100 pages 10 times
	 */
	@Test
	public void testExtend() {
		assertTrue(dbf.size() == 0);
		for (int i = 0; i < 10; i++) {
			dbf.extend(10);
			assertTrue(dbf.size() == (i + 1) * 10);
		}
	}

	/**
	 * Test page fetch and flush functions.
	 */
	@Test
	public void testFetchFlush() {
		// extend the file first
		testExtend();
		fetchFlush(33);
	}

	/**
	 * Try the DBFile force method.
	 */
	@Test
	public void testForce() {
		dbf.force();
		testFetchFlush();
		dbf.force();
	}

	/**
	 * Create a 5 gig file and test
	 */
	@Test
	public void testVLDB() {
		// Create a 10gig data file
		assertTrue(dbf.size() == 0);
		long bytes = (long) 5000 * (long) 1024 * 1024;
		int npages = (int) (bytes / dbf.getPageSize());
		dbf.extend(npages);
		assertTrue(dbf.size() == npages);
		assertTrue(dbf.capacity() == bytes);

		// Read/Write the last page in the file
		fetchFlush(npages - 1);
	}

	/**
	 * Fetch and flush a specific page.
	 */
	private void fetchFlush(int pid) {
		// Fetch page pid, modify it, fetch it again and verify the
		// modification is not there,
		// then modify it again, flush it, clear it, fetch it and verify the
		// modification is there.
		assertTrue(bb.getInt(33) == 0);
		dbf.fetch(pid, bb);
		assertTrue(bb.getInt(33) == 0);
		bb.putInt(33, 33);
		assertTrue(bb.getInt(33) == 33);
		dbf.fetch(pid, bb);
		assertTrue(bb.getInt(33) == 0);
		bb.putInt(33, 33);
		assertTrue(bb.getInt(33) == 33);
		dbf.flush(pid, bb, false);
		bb.putInt(33, 0);
		assertTrue(bb.getInt(33) == 0);
		dbf.fetch(pid, bb);
		assertTrue(bb.getInt(33) == 33);
	}

	private DBFile dbf;
	private ByteBuffer bb;
	private RandomAccessFile raf;
}
