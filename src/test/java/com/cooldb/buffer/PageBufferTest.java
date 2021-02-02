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

public class PageBufferTest {

	@Before
	public void setUp() throws IOException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();

		fm = new FileManagerImpl();
		page = new FilePage();

		// Create a buffer pool with 5 buffers
		bp = new BufferPoolImpl(fm);
		bp.ensureCapacity(5);
		bp.start();

		// add 4 files to the FileManager
		files = new RandomAccessFile[4];
		for (int i = 0; i < 4; i++) {
			String fileName = "build/tmp/cooldb/test/test.db" + i;
			files[i] = new RandomAccessFile(fileName, "rw");
			FileChannel channel = files[i].getChannel();
			channel.truncate(0);
			fm.addFile((short) i, new DBFile(files[i]));
		}

		// extend the files by 25 pages each
		ByteBuffer bb = ByteBuffer.allocate(fm.getPageSize());
		for (int i = 0; i < 4; i++) {
			fm.extend((short) i, 25);

			// write an integer identifier on each page
			page.setFileId((short) i);
			for (int p = 0; p < 25; p++) {
				page.setPageId(p);
				fm.fetch(page, bb);
				bb.putInt(222, i);
				bb.putInt(333, p);
				fm.flush(page, bb, false);
			}
		}
	}

	@After
	public void tearDown() throws IOException {
		bp.stop();

		// remove the files
		for (int i = 0; i < 4; i++) {
			files[i].close();
			String fileName = "build/tmp/cooldb/test/test.db"
					+ i;
			File file = new File(fileName);
			file.delete();
		}

		bp = null;
		fm = null;
		page = null;
		bb = null;
		files = null;

		System.gc();
	}

	/**
	 * Test the PageBuffer.
	 *
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testSimpleAccess() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);
		page.setPageId(3);

		bb = pin(page, BufferPool.Mode.EXCLUSIVE);

		bb.putInt(1000, 1234);
		bb.putInt(456);
		bb.position(2000);
		bb.putInt(789);

		bb.position(1000);
		assertTrue(bb.getInt() == 1234);
		assertTrue(bb.getInt() == 456);
		assertTrue(bb.getInt(2000) == 789);

		bp.unPin(bb, BufferPool.Affinity.HATED);
	}

	/**
	 * Exercise the PageBuffer.
	 *
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testExercise() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);
		page.setPageId(3);

		int s = 0;
		byte[] byteArrayVal = { (byte) 3, (byte) 2, (byte) 99 };
		byte[] byteArrayBuf = new byte[3];
		ByteBuffer byteBufferVal = ByteBuffer.allocate(3);
		byteBufferVal.put((byte) 7);
		byteBufferVal.put((byte) 111);
		byteBufferVal.put((byte) 13);

		for (int j = 0; j < 10000; j++) {
			int offset = 0;
			byte byteVal = 0;
			short shortVal = 0;
			int intVal = 0;
			long longVal = 0;
			float floatVal = 0;
			double doubleVal = 0;
			char charVal = '0';

			bb = bp.pin(page, BufferPool.Mode.EXCLUSIVE);
			int nwrites = random(1000) + 1;
			for (int i = 0; i < nwrites; i++) {
				offset = random(fm.getPageSize() - 8);

				byteVal = (byte) random(Byte.MAX_VALUE);
				shortVal = (short) random(Short.MAX_VALUE);
				intVal = random(Short.MAX_VALUE);
				longVal = random(Short.MAX_VALUE);
				floatVal = random(Short.MAX_VALUE);
				doubleVal = random(Short.MAX_VALUE);
				charVal = 'z';

				s = random(9);
				switch (s) {
				case 0:
					if (random(100) < 50) {
						bb.position(offset);
						bb.put(byteVal);
					} else
						bb.put(offset, byteVal);
					break;
				case 1:
					if (random(100) < 50) {
						bb.position(offset);
						bb.putShort(shortVal);
					} else
						bb.putShort(offset, shortVal);
					break;
				case 2:
					if (random(100) < 50) {
						bb.position(offset);
						bb.putInt(intVal);
					} else
						bb.putInt(offset, intVal);
					break;
				case 3:
					if (random(100) < 50) {
						bb.position(offset);
						bb.putLong(longVal);
					} else
						bb.putLong(offset, longVal);
					break;
				case 4:
					if (random(100) < 50) {
						bb.position(offset);
						bb.putFloat(floatVal);
					} else
						bb.putFloat(offset, floatVal);
					break;
				case 5:
					if (random(100) < 50) {
						bb.position(offset);
						bb.putDouble(doubleVal);
					} else
						bb.putDouble(offset, doubleVal);
					break;
				case 6:
					if (random(100) < 50) {
						bb.position(offset);
						bb.putChar(charVal);
					} else
						bb.putChar(offset, charVal);
					break;
				case 7:
					bb.position(offset);
					bb.put(byteArrayVal, 0, byteArrayVal.length);
					break;
				case 8:
					bb.position(offset);
					byteBufferVal.rewind();
					bb.put(byteBufferVal);
					break;
				}
			}

			switch (s) {
			case 0:
				if (random(100) < 50) {
					bb.position(offset);
					assertTrue(bb.get() == byteVal);
				} else
					assertTrue(bb.get(offset) == byteVal);
				break;
			case 1:
				if (random(100) < 50) {
					bb.position(offset);
					assertTrue(bb.getShort() == shortVal);
				} else
					assertTrue(bb.getShort(offset) == shortVal);
				break;
			case 2:
				if (random(100) < 50) {
					bb.position(offset);
					assertTrue(bb.getInt() == intVal);
				} else
					assertTrue(bb.getInt(offset) == intVal);
				break;
			case 3:
				if (random(100) < 50) {
					bb.position(offset);
					assertTrue(bb.getLong() == longVal);
				} else
					assertTrue(bb.getLong(offset) == longVal);
				break;
			case 4:
				if (random(100) < 50) {
					bb.position(offset);
					assertTrue(bb.getFloat() == floatVal);
				} else
					assertTrue(bb.getFloat(offset) == floatVal);
				break;
			case 5:
				if (random(100) < 50) {
					bb.position(offset);
					assertTrue(bb.getDouble() == doubleVal);
				} else
					assertTrue(bb.getDouble(offset) == doubleVal);
				break;
			case 6:
				if (random(100) < 50) {
					bb.position(offset);
					assertTrue(bb.getChar() == charVal);
				} else
					assertTrue(bb.getChar(offset) == charVal);
				break;
			case 7:
				bb.position(offset);
				bb.get(byteArrayBuf);
				for (int b = 0; b < byteArrayBuf.length; b++)
					assertTrue(byteArrayBuf[b] == byteArrayVal[b]);
				break;
			case 8:
				bb.position(offset);
				bb.get(byteArrayBuf);
				for (int b = 0; b < byteArrayBuf.length; b++)
					assertTrue(byteArrayBuf[b] == byteBufferVal.get(b));
				break;
			}

			bp.unPin(bb, BufferPool.Affinity.HATED);
		}
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	private PageBuffer pin(FilePage page, BufferPool.Mode mode)
			throws BufferNotFound, InterruptedException {
		PageBuffer bb = bp.pin(page, mode);
		assertTrue(bb.getInt(222) == page.getFileId());
		assertTrue(bb.getInt(333) == page.getPageId());
		return bb;
	}

	private BufferPoolImpl bp;
	private FileManager fm;
	private FilePage page;
	private PageBuffer bb;
	private RandomAccessFile[] files;
}
