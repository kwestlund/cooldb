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
import java.nio.ReadOnlyBufferException;
import java.nio.channels.FileChannel;

import static org.junit.Assert.assertTrue;

public class BufferPoolTest {

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
			String fileName = "build/tmp/cooldb/test.db" + i;
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
			String fileName = "test/test.db" + i;
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
	 * Make sure a simple pin/unpin works
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testSimplePin() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);

		for (int i = 0; i < 25; i++) {
			if (i <= bp.capacity)
				assertTrue(bp.allocated == i);
			else
				assertTrue(bp.allocated == bp.capacity);
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			bp.unPin(bb, BufferPool.Affinity.HATED);
		}
	}

	/**
	 * Make sure we cannot overwrite a read-only buffer.
	 * @throws Exception
	 */
	@Test
	public void testReadOnlyBuffer() throws Exception {
		page.setFileId((short) 1);

		page.setPageId(1);
		bb = pin(page, BufferPool.Mode.SHARED);
		try {
			bb.putInt(33, 33);
			throw new Exception("Overwrote read-only buffer");
		} catch (ReadOnlyBufferException robe) {
		}
		bp.unPin(bb, BufferPool.Affinity.HATED);
	}

	/**
	 * Make sure we cannot pin a buffer while it is already pinned EXCLUSIVE.
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testTryPin() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);

		page.setPageId(1);
		bb = pin(page, BufferPool.Mode.SHARED);
		PageBuffer bb2 = bp.tryPin(page, BufferPool.Mode.SHARED);
		assertTrue(bb2 != null);
		assertTrue(bp.tryPin(page, BufferPool.Mode.EXCLUSIVE) == null);
		bp.unPin(bb2, BufferPool.Affinity.HATED);
		assertTrue(bp.tryPin(page, BufferPool.Mode.EXCLUSIVE) == null);
		bp.unPin(bb, BufferPool.Affinity.HATED);
		bb = bp.tryPin(page, BufferPool.Mode.EXCLUSIVE);
		assertTrue(bb != null);
		assertTrue(bp.tryPin(page, BufferPool.Mode.SHARED) == null);
		bp.unPin(bb, BufferPool.Affinity.HATED);
		bb = bp.tryPin(page, BufferPool.Mode.SHARED);
		assertTrue(bb != null);
		bp.unPin(bb, BufferPool.Affinity.HATED);
	}

	/**
	 * Make sure we that if we pin more buffers than we have, new buffers are
	 * automatically allocated.
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testCapacity() throws BufferNotFound, InterruptedException {
		PageBuffer[] bufs = new PageBuffer[bp.capacity + 1];
		page.setFileId((short) 1);

		int i;
		for (i = 0; i < bp.capacity; i++) {
			page.setPageId(i);
			bufs[i] = pin(page, BufferPool.Mode.SHARED);
		}

		page.setPageId(i);
		bufs[i] = pin(page, BufferPool.Mode.SHARED);

		assertTrue(bp.capacity == i + 1);

		for (i = 0; i < bp.capacity; i++) {
			page.setPageId(i);
			bp.unPin(bufs[i], BufferPool.Affinity.HATED);
		}
	}

	/**
	 * Make sure we catch mismatched pin/unpin pairs
	 */
	@Test
	public void testMismatchedPin() {
		bp.unPin(bb, BufferPool.Affinity.HATED);
	}

	/**
	 * Test writes.
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testBufferWrites() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);

		for (int i = 0; i < 25; i++) {
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.EXCLUSIVE);
			bb.putInt(i);
			bp.unPinDirty(bb, BufferPool.Affinity.HATED, i);
		}

		for (int i = 0; i < 25; i++) {
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			assertTrue(bb.getInt() == i);
			bp.unPin(bb, BufferPool.Affinity.HATED);
		}
	}

	/**
	 * Test replacement of HATED pages. These should be replaced before all
	 * others.
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testReplaceHated() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);

		// make sure all buffers have been allocated, all LIKED except one
		// HATED
		for (int i = 0; i < bp.capacity; i++) {
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			if (i == 2)
				bp.unPin(bb, BufferPool.Affinity.HATED);
			else
				bp.unPin(bb, BufferPool.Affinity.LIKED);
		}

		// now scan the file using the single HATED buffer
		for (int i = 0; i < 25; i++) {
			assertPagesCached();
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			bp.unPin(bb, BufferPool.Affinity.HATED);
		}
	}

	private void assertPagesCached() {
		for (int i = 0; i < bp.capacity; i++) {
			page.setPageId(i);
			if (i != 2)
				assertTrue(bp.isCached(page));
		}
	}

	/**
	 * Test replacement of LIKED pages. These should be replaced before all
	 * LOVED pages.
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testReplaceLiked() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);

		// make sure all buffers have been allocated, all LOVED except one
		// LIKED
		for (int i = 0; i < bp.capacity; i++) {
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			if (i == 2)
				bp.unPin(bb, BufferPool.Affinity.LIKED);
			else
				bp.unPin(bb, BufferPool.Affinity.LOVED);
		}

		// now scan the file using the single LIKED buffer
		for (int i = 0; i < 25; i++) {
			assertPagesCached();
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			bp.unPin(bb, BufferPool.Affinity.LIKED);
		}
	}

	/**
	 * Test replacement of LOVED pages by HATED pages (all pages must be LOVED
	 * for this to happen).
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testReplaceLoved() throws BufferNotFound, InterruptedException {
		page.setFileId((short) 1);

		// make sure all buffers are LOVED
		for (int i = 0; i < bp.capacity; i++) {
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			bp.unPin(bb, BufferPool.Affinity.LOVED);
		}

		// now scan the file replacing LOVED pages with HATED ones
		// this will age the buffers in turn, reversing their order, so that
		// the last
		// one becomes the first and only one to be replaced (once HATED, it
		// will continue
		// to be replaced as the only HATED page among LOVED pages)
		for (int i = 0; i < 25; i++) {
			page.setPageId(i);
			bb = pin(page, BufferPool.Mode.SHARED);
			bp.unPin(bb, BufferPool.Affinity.HATED);
		}

		// verify cache
		for (int i = 0; i < bp.capacity; i++) {
			page.setPageId(i);
			BufferPoolImpl.Slot slot = bp.pageMap.get(page);

			// the last LOVED page should be gone
			if (i == bp.capacity - 1)
				assertTrue(slot == null);
			else {
				// all others should still be cached
				assertTrue(slot != null);
				assertTrue(slot.getAffinity() == BufferPool.Affinity.LOVED);
			}
		}
		// and the last HATED page read (24) should be cached in place of
		// the LOVED one
		page.setPageId(24);
		BufferPoolImpl.Slot slot = bp.pageMap.get(page);
		assertTrue(slot != null);
		assertTrue(slot.getAffinity() == BufferPool.Affinity.HATED);
	}

	/**
	 * Exercise all possible combinations of pin/unpin.
	 *
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testExercise() throws BufferNotFound, InterruptedException {
		// Ensure a buffer pool capacity of 80 buffers
		bp.ensureCapacity(80);

		// filepage: files 1-4, pages 1-25
		// pin Mode: SHARED, EXCLUSIVE
		// unpin Affinity: HATED, LIKED, LOVED; with and without endLSN
		for (int i = 0; i < 1000000; i++) {
			page.setFileId((short) random(4));
			page.setPageId(random(25));
			BufferPool.Mode mode = random(2) == 0 ? BufferPool.Mode.SHARED
					: BufferPool.Mode.EXCLUSIVE;
			BufferPool.Affinity affinity = null;
			int a = random(3);
			switch (a) {
			case 0:
				affinity = BufferPool.Affinity.HATED;
				break;
			case 1:
				affinity = BufferPool.Affinity.LIKED;
				break;
			case 2:
				affinity = BufferPool.Affinity.LOVED;
				break;
			}
			bb = pin(page, mode);
			bp.unPinDirty(bb, affinity, i);

			// checkpoint
			if (i % 100000 == 0)
				bp.checkPoint();
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
