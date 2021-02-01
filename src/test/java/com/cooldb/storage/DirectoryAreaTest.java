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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Arrays;

import com.cooldb.buffer.FilePage;
import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Core;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class DirectoryAreaTest {

	@Before
	public void setUp() throws DatabaseException, InterruptedException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();

		core = new Core(new File("build/tmp/cooldb"));
		core.createDatabase(true);

		pageBuffer = core.getTransactionManager().pinNew(
				new FilePage((short) 0, 10));
		int rowBase = RowPage.BASE + 16; // 16 bytes for 2 8-byte lock
		// entries
		dirArea = new DirectoryArea();
		dirArea.setPageBuffer(pageBuffer);
		dirArea.setBounds(rowBase, pageBuffer.capacity() - rowBase);

		PageBuffer pageBuffer2 = core.getTransactionManager().pinNew(
				new FilePage((short) 0, 11));
		dirArea2 = new DirectoryArea();
		dirArea2.setPageBuffer(pageBuffer2);
		dirArea2.setBounds(rowBase, pageBuffer2.capacity() - rowBase);
	}

	@After
	public void tearDown() {
		pageBuffer = null;
		dirArea = null;
		core.destroyDatabase();
		core = null;

		System.gc();
	}

	/**
	 * Set the directory value for as many entries as possible, then read and
	 * verify them.
	 */
	@Test
	public void testSetGet() {
		int n = (dirArea.getCapacity() - 2) / 2;
		for (int i = 0; i < n; i++) {
			assertTrue(dirArea.getCount() == i);
			dirArea.set(i, (short) i);
			dirArea.setCount((short) (i + 1));
		}
		for (int i = 0; i < n; i++)
			assertTrue(dirArea.get(i) == (short) i);
	}

	/**
	 * Push as many entries onto the directory and validate them.
	 */
	@Test
	public void testPush() {
		// push entries of varying sizes onto the page
		int maxSize = dirArea.getUnusedSpace() - 2;
		for (int size = 1; size <= maxSize; size++) {
			dirArea.setCount((short) 0);
			int n = (dirArea.getCapacity() - 2) / (size + 2);
			assertTrue(n > 0);
			byte[] data = new byte[size];
			Arrays.fill(data, (byte) 255);

			push(n, size, data);
			assertFalse(dirArea.canHold(size));
			validate(n, size, data);
		}
	}

	/**
	 * Test insert, remove, replace: push maximum - 1 entries onto the page,
	 * insert 1 and validate the page, then remove 1 from the page and validate
	 * again, then insert another and validate, then replace some and validate.
	 */
	@Test
	public void testInsert() {
		// push entries of varying sizes onto the page
		int maxSize = dirArea.getUnusedSpace() - 2;
		for (int size = 1; size <= maxSize; size++) {
			dirArea.setCount((short) 0);
			int n = (dirArea.getCapacity() - 2) / (size + 2);
			assertTrue(n > 0);
			byte[] data = new byte[size];
			Arrays.fill(data, (byte) 255);

			// leave room for the insert
			--n;
			push(n, size, data);
			validate(n, size, data);

			insertRemoveAt(0, n, size, data);
			insertRemoveAt(n, n, size, data);

			for (int j = 0; j < 20; j++) {
				insertRemoveAt(random(n), n, size, data);
			}

			for (int j = 0; j < 20; j++) {
				replaceAt(random(n), n, size, data);
			}
		}
	}

	/**
	 * Insert several entries into one directory, then copy several to another
	 * directory area, then truncate the original and validate the results on
	 * both pages.
	 */
	@Test
	public void testCopyAndTruncate() {
		// push entries of varying sizes onto the page
		int maxSize = dirArea.getUnusedSpace() - 2;
		for (int size = 1; size <= maxSize; size++) {
			dirArea.setCount((short) 0);
			dirArea2.setCount((short) 0);
			int n = (dirArea.getCapacity() - 2) / (size + 2);
			assertTrue(n > 0);
			byte[] data = new byte[size];
			Arrays.fill(data, (byte) 255);
			push(n, size, data);
			validate(n, size, data);

			// split the page in 2
			int from = n / 2;
			dirArea2.copyRange(dirArea, from, n);
			dirArea.setCount(from);

			assertTrue(dirArea.getCount() == from);
			assertTrue(dirArea2.getCount() == n - from);

			validate(dirArea2, n - from, size, data);
		}
	}

	/**
	 * TODO: Test compact
	 */
	@Test
	public void testCompact() {
	}

	private void replaceAt(int i, int n, int size, byte[] data) {
		// replace
		dirArea.replaceAt(i, (short) size);
		write(size, data, i);
		validate(n, size, data);
	}

	private void insertRemoveAt(int i, int n, int size, byte[] data) {
		// insert
		dirArea.insertAt(i, (short) size);
		assertFalse(dirArea.canHold(size));
		++n;
		write(size, data, i);
		validate(n, size, data);

		// remove
		dirArea.removeAt(i);
		assertTrue(dirArea.canHold(size));
		--n;
		validate(n, size, data);
	}

	private void validate(int n, int size, byte[] data) {
		validate(dirArea, n, size, data);
	}

	private void validate(DirectoryArea dirArea, int n, int size, byte[] data) {
		// validate ceil, floor, sizeAt
		for (int i = 0; i < n; i++) {
			assertTrue(dirArea.sizeAt(i) == size);
			assertTrue(dirArea.ceil(i) == (i + 1) * size);
			assertTrue(dirArea.floor(i) == i * size);
		}

		// validate values
		for (int i = 0; i < n; i++) {
			int loc = dirArea.loc(i);
			pageBuffer.get(loc, data, 0, size);
			assertTrue(data[0] == data[size - 1]);
		}
	}

	private void push(int n, int size, byte[] data) {
		for (int i = 0; i < n; i++) {
			assertTrue(dirArea.getCount() == i);
			assertTrue(dirArea.canHold(size));
			dirArea.push(size);
			write(size, data, i);
		}
	}

	private void write(int size, byte[] data, int i) {
		// write a value into the location
		int loc = dirArea.loc(i);
		data[0] = (byte) random(255);
		data[size - 1] = data[0];
		pageBuffer.put(loc, data, 0, size);
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	Core core;
	PageBuffer pageBuffer;
	DirectoryArea dirArea;
	PageBuffer pageBuffer2;
	DirectoryArea dirArea2;
}
