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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class FileManagerTest {

	@Before
	public void setUp() throws IOException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();
		fm = new FileManagerImpl();
		bb = ByteBuffer.allocate(fm.getPageSize());

		// add 4 files to the FileManager
		files = new RandomAccessFile[10];
		for (int i = 0; i < 10; i++) {
			String fileName = System.getProperty("user.home") + "/test/test.db"
					+ i;
			files[i] = new RandomAccessFile(fileName, "rw");
			FileChannel channel = files[i].getChannel();
			channel.truncate(0);
			fm.addFile((short) i, new DBFile(files[i]));
		}

		// extend the files by 100 pages each
		for (int i = 0; i < 10; i++) {
			fm.extend((short) i, 100);
		}
	}

	@After
	public void tearDown() throws IOException {
		// remove the files
		for (int i = 0; i < 10; i++) {
			files[i].close();
			String fileName = System.getProperty("user.home") + "/test/test.db"
					+ i;
			File file = new File(fileName);
			file.delete();
		}

		fm = null;
		bb = null;
		files = null;

		System.gc();
	}

	/**
	 * Extend the file by 100 pages 10 times
	 */
	@Test
	public void testFileManager() {
		FilePage page = new FilePage();
		for (int i = 0; i < 10; i++) {
			page.setFileId((short) i);
			page.setPageId(33);

			// Fetch page 33, modify it, fetch it again and verify the
			// modification is not there,
			// then modify it again, flush it, clear it, fetch it and verify
			// the modification is there.
			bb.putInt(33, 0);
			assertTrue(bb.getInt(33) == 0);
			fm.fetch(page, bb);
			assertTrue(bb.getInt(33) == 0);
			bb.putInt(33, 33);
			assertTrue(bb.getInt(33) == 33);
			fm.fetch(page, bb);
			assertTrue(bb.getInt(33) == 0);
			bb.putInt(33, 33);
			assertTrue(bb.getInt(33) == 33);
			fm.flush(page, bb, false);
			bb.putInt(33, 0);
			assertTrue(bb.getInt(33) == 0);
			fm.fetch(page, bb);
			assertTrue(bb.getInt(33) == 33);
		}

		for (int i = 0; i < 10; i++) {
			fm.force((short) i);
		}
	}

	private FileManager fm;
	private ByteBuffer bb;
	private RandomAccessFile[] files;
}
