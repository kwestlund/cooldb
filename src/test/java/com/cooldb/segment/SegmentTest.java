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

package com.cooldb.segment;

import com.cooldb.buffer.FilePage;
import com.cooldb.transaction.TransactionManager;
import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.BufferNotFound;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.PageBuffer;
import com.cooldb.core.Core;
import com.cooldb.log.LogExhaustedException;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionCancelledException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Comparator;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class SegmentTest {

	@Before
	public void setUp() throws DatabaseException {
		System.gc();
		File file = new File("build/tmp/cooldb");
		file.mkdirs();

		core = new Core(new File("build/tmp/cooldb"));
		core.createDatabase(true);
	}

	@After
	public void tearDown() {
		core.destroyDatabase();
		core = null;

		System.gc();
	}

	/**
	 * Stop and start the database a few times.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testCore() throws DatabaseException, InterruptedException {
		for (int i = 0; i < 50; i++) {
			core.stopDatabase();
			core.startDatabase();
		}
		// test soft shutdown
		TransactionManager tm = core.getTransactionManager();
		Transaction trans = tm.beginTransaction();
		assertFalse(core.stopDatabase(1000));
		tm.commitTransaction(trans);
		assertTrue(core.stopDatabase(1000));
	}

	/**
	 * Exercise the Binary-search Page. Insert various FilePage objects,
	 * validate by checking the count and ordering in the vector, then perform
	 * binary searches and verify results, then remove various pages and
	 * continue to validate contents.
	 *
	 * @throws InterruptedException
	 * @throws BufferNotFound
	 */
	@Test
	public void testBSearchArea() throws BufferNotFound, InterruptedException {
		TransactionManager tm = core.getTransactionManager();
		FilePage page = new FilePage((short) 0, 3);
		PageBuffer pb = tm.pinNew(page);
		BSearchArea bpage = new BSearchArea(0, pb.capacity(), FilePage.sizeOf());
		bpage.setPageBuffer(pb);

		// test 'put'
		int i = 0;
		for (i = 0; i < 1000; i++) {
			page.setPageId(i);
			int count = bpage.getCount();
			assertTrue(count == i);
			bpage.put(i, page);
		}

		// test bSearch
		BinarySearch.Search search = new BinarySearch.Search(page,
				new FilePage());
		for (i = 0; i < 1000; i++) {
			int p = random(1000);
			page.setPageId(p);

			assertTrue(BinarySearch.bSearch(search, bpage));
			bpage.get(search.index, page);
			assertTrue(search.index == p);
			assertTrue(search.index == page.getPageId());
		}

		// test insert
		page.setPageId(333);
		bpage.insert(333, page);
		int count = bpage.getCount();
		assertTrue(count == 1001);

		for (i = 0; i < 1001; i++) {
			bpage.get(i, page);
			if (i <= 333)
				assertTrue(i == page.getPageId());
			else
				assertTrue(i == page.getPageId() + 1);
		}

		// test remove
		bpage.remove(333);
		count = bpage.getCount();
		assertTrue(count == 1000);

		for (i = 0; i < 1000; i++) {
			bpage.get(i, page);
			assertTrue(i == page.getPageId());
		}

		// test capacity
		do {
			page.setPageId(i);
			bpage.put(i++, page);
		} while (bpage.getAvailable() > 0);

		assertTrue(bpage.getCount() == (short) ((pb.capacity() - BSearchArea.ENTRY_BASE) / FilePage
				.sizeOf()));

		tm.unPin(pb, BufferPool.Affinity.HATED);
	}

	/**
	 * Create a SegmentManager then insert a segment, select the segment and
	 * validate the attributes are as expected, then change one of the
	 * attributes and update the segment, select and validate again, then remove
	 * the segment, select and confirm its removal. Repeat randomly to exercise.
	 *
	 * @throws InterruptedException
	 * @throws DatabaseException
	 */
	@Test
	public void testSegmentManager() throws InterruptedException,
			DatabaseException {
		TransactionManager tm = core.getTransactionManager();
		FilePage FREE_EXTENTS_ID = new FilePage((short) 0, 0);
		FilePage USED_EXTENTS_ID = new FilePage((short) 0, 1);
		FilePage SEGMENT_MANAGER_ID = new FilePage((short) 0, 2);
		FilePage DATASET_MANAGER_ID = new FilePage((short) 0, 3);
		FilePage TABLE_MANAGER_ID = new FilePage((short) 0, 4);
		FilePage TREE_MANAGER_ID = new FilePage((short) 0, 5);
		FilePage SEQUENCE_MANAGER_ID = new FilePage((short) 0, 6);
		Segment smSegment = new Segment(SEGMENT_MANAGER_ID);
		Segment freeSegment = new Segment(FREE_EXTENTS_ID);
		Segment usedSegment = new Segment(USED_EXTENTS_ID);
		Segment datasetSegment = new Segment(DATASET_MANAGER_ID);
		Segment tablesSegment = new Segment(TABLE_MANAGER_ID);
		Segment treeSegment = new Segment(TREE_MANAGER_ID);
		Segment seqSegment = new Segment(SEQUENCE_MANAGER_ID);
		SegmentManager sm = new SegmentManager(smSegment, core);

		Transaction trans = tm.beginTransaction();

		for (int i = 0; i < 10000; i++) {
			FilePage page = new FilePage((short) 0, random(181));
			Segment segment = new Segment(page, 10, 10, 2);

			if (segment.equals(smSegment) || segment.equals(freeSegment)
					|| segment.equals(usedSegment)
					|| segment.equals(datasetSegment)
					|| segment.equals(tablesSegment)
					|| segment.equals(treeSegment)
					|| segment.equals(seqSegment))
				continue;

			Segment lookup = new Segment(page);
			segment.setSegmentId(page);
			lookup.setSegmentId(page);

			if (!sm.select(lookup)) {
				sm.insert(trans, segment);
				assertTrue(sm.select(lookup));
				assertTrue(lookup.getNextSize() == 10);
			} else {
				assertTrue(sm.select(lookup));
				assertTrue(lookup.getNextSize() == 20);
			}
			assertTrue(lookup.getInitialSize() == 10);
			assertTrue(lookup.getGrowthRate() == 2);
			segment.setNextSize(20);
			sm.update(trans, segment);
			assertTrue(sm.select(lookup));
			assertTrue(lookup.getNextSize() == 20);

			// test removal in only 5% of cases
			if (random(100) < 5) {
				sm.remove(trans, segment);
				assertFalse(sm.select(segment));
			}
		}

		tm.commitTransaction(trans);
	}

	/**
	 * Create a FreeExtentMethod then insert a extent, select the extent and
	 * validate the attributes are as expected, then change one of the
	 * attributes and update the extent, select and validate again, then remove
	 * the extent, select and confirm its removal. Repeat randomly to exercise.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 * @throws TransactionCancelledException
	 */
	@Test
	public void testFreeExtentMethod() throws BufferNotFound,
			LogExhaustedException, InterruptedException,
			TransactionCancelledException {
		TransactionManager tm = core.getTransactionManager();
		FilePage emPage = new FilePage((short) 0, 33);
		Segment emSegment = new Segment(emPage);
		ExtentMethod em = new FreeExtentMethod(emSegment, core);

		Transaction trans = tm.beginTransaction();

		// Test findExtent
		Extent extent = new Extent(emPage, 100);
		em.insertExtent(trans, extent);
		extent.setSize(0);
		assertTrue(em.findExtent(50, extent));
		assertTrue(extent.getSize() == 100);
		assertTrue(em.findExtent(100, extent));
		assertTrue(extent.getSize() == 100);
		assertFalse(em.findExtent(101, extent));
		extent.setStart(emPage);
		extent.setSize(100);
		em.removeExtent(trans, extent);
		assertFalse(em.findExtent(50, extent));

		// Test coalesce upper
		extent.setStart(emPage);
		extent.setSize(100);
		em.insertExtent(trans, extent);
		extent.setPageId(extent.getEndPageId());
		em.insertExtent(trans, extent);
		assertTrue(em.findExtent(200, extent));
		assertTrue(extent.equals(emPage));

		// Test coalesce lower
		extent.setPageId(extent.getPageId() - 3);
		extent.setSize(3);
		em.insertExtent(trans, extent);
		assertTrue(em.findExtent(203, extent));
		assertTrue(extent.getPageId() == emPage.getPageId() - 3);

		// Test split, coalesce both sides
		extent.setPageId(extent.getPageId() + 10);
		extent.setSize(10);
		em.removeExtent(trans, extent);
		assertTrue(em.findExtent(183, extent));
		assertFalse(em.findExtent(184, extent));
		extent.setPageId(emPage.getPageId() + 7);
		extent.setSize(10);
		em.insertExtent(trans, extent);
		assertTrue(em.findExtent(203, extent));

		tm.commitTransaction(trans);
	}

	/**
	 * Create a UsedExtentMethod then insert a SegmentExtent, select the extent
	 * and validate the attributes are as expected, then change one of the
	 * attributes and update the extent, select and validate again, then remove
	 * the extent, select and confirm its removal. Repeat randomly to exercise.
	 *
	 * @throws InterruptedException
	 * @throws LogExhaustedException
	 * @throws BufferNotFound
	 * @throws TransactionCancelledException
	 */
	@SuppressWarnings("unchecked")
	@Test
	public void testUsedExtentMethod() throws BufferNotFound,
			LogExhaustedException, InterruptedException,
			TransactionCancelledException {
		TransactionManager tm = core.getTransactionManager();
		FilePage emPage = new FilePage((short) 0, 33);
		Segment emSegment = new Segment(emPage);
		ExtentMethod em = new UsedExtentMethod(emSegment, core);

		Transaction trans = tm.beginTransaction();

		// Test findExtent
		SegmentExtent proto1 = new SegmentExtent(emPage,
				new Extent(emPage, 100));
		SegmentExtent extent = (SegmentExtent) proto1.copy();
		SegmentExtent emPageExtent = (SegmentExtent) extent.copy();
		FilePage anotherPage = new FilePage((short) 1, 33);
		Segment anotherSegment = new Segment(anotherPage);
		SegmentExtent proto2 = new SegmentExtent(anotherPage, new Extent(
				anotherPage, 100));
		SegmentExtent anotherExtent = (SegmentExtent) proto2.copy();

		em.insertExtent(trans, extent);
		em.insertExtent(trans, anotherExtent);
		extent.setSize(0);
		assertTrue(em.findExtent(50, extent));
		assertTrue(extent.getSize() == 100);
		assertTrue(em.findExtent(100, extent));
		assertTrue(extent.getSize() == 100);
		assertFalse(em.findExtent(101, extent));
		extent.setStart(emPage);
		extent.setSize(100);
		em.removeExtent(trans, extent);
		assertTrue(em.findExtent(50, extent));
		em.removeExtent(trans, extent);
		assertFalse(em.findExtent(50, extent));

		// Test coalesce upper
		extent = (SegmentExtent) proto1.copy();
		extent.setStart(emPage);
		extent.setSize(100);
		em.insertExtent(trans, extent);
		em.insertExtent(trans, anotherExtent);
		extent.setPageId(extent.getEndPageId());
		em.insertExtent(trans, extent);
		assertTrue(em.findExtent(200, extent));
		assertTrue(extent.equals(emPageExtent));
		assertFalse(em.findExtent(250, extent));

		// Test coalesce lower
		extent.setPageId(extent.getPageId() - 3);
		extent.setSize(3);
		em.insertExtent(trans, extent);
		assertTrue(em.findExtent(203, extent));
		assertTrue(extent.getPageId() == emPage.getPageId() - 3);

		// Test split, coalesce both sides
		extent.setPageId(extent.getPageId() + 10);
		extent.setSize(10);
		em.removeExtent(trans, extent);
		assertTrue(em.findExtent(183, extent));
		assertFalse(em.findExtent(184, extent));
		extent.setPageId(emPage.getPageId() + 7);
		extent.setSize(10);
		em.insertExtent(trans, extent);
		assertTrue(em.findExtent(203, extent));

		em.removeExtent(trans, extent);
		assertTrue(em.findExtent(100, extent));
		assertTrue(extent.equals(anotherExtent));
		em.removeExtent(trans, anotherExtent);
		assertFalse(em.findExtent(1, extent));

		// Exercise a bit
		extent = (SegmentExtent) proto1.copy();
		anotherExtent = (SegmentExtent) proto2.copy();

		em.insertExtent(trans, extent);

		for (int i = 0; i < 10; i++) {
			// insert non-contiguous 100 page extents, starting at pageId =
			// 200
			anotherExtent.setPageId(200 + i * 100 + i);

			em.insertExtent(trans, anotherExtent);
		}

		assertFalse(em.findExtent(101, extent));

		for (int i = 0; i < 9; i++) {
			// fill in the gaps to force coalesce
			anotherExtent.setPageId(300 + i * 100 + i);
			anotherExtent.setSize(1);

			em.insertExtent(trans, anotherExtent);
		}

		assertTrue(em.findExtent(1000, extent));

		for (int i = 0; i < 9; i++) {
			// remove the gaps to force splitting
			anotherExtent.setPageId(300 + i * 100 + i);
			anotherExtent.setSize(1);

			em.removeExtent(trans, anotherExtent);
		}

		assertFalse(em.findExtent(101, extent));

		// Test find by filter
		Comparator comp = new SpaceManager.SegmentComparator(anotherSegment);
		while (em.findExtent(extent, comp)) {
			em.removeExtent(trans, extent);
		}

		assertTrue(em.findExtent(100, extent));
		assertTrue(extent.getSegmentId().equals(emPage));
		em.removeExtent(trans, extent);
		assertFalse(em.findExtent(1, extent));

		tm.commitTransaction(trans);
	}

	/**
	 * Create several FreeExtentMethod methods, then make sure they can be found
	 * using the 'get' method, then update some attributes, then stop and
	 * restart the SegmentFactory to clear its cache, then use 'get' to make
	 * sure the changed attributes persisted.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testSegmentFactory() throws DatabaseException,
			InterruptedException {
		TransactionManager tm = core.getTransactionManager();
		SegmentFactory sf = core.getSegmentFactory();
		FreeExtentMethod fems[] = new FreeExtentMethod[100];
		FreeExtentMethod fems2[] = new FreeExtentMethod[100];

		Transaction trans = tm.beginTransaction();

		for (int i = 0; i < fems.length; i++) {
			FilePage page = new FilePage((short) 0, 100 + i);
			Segment segment = new Segment(page);

			fems[i] = (FreeExtentMethod) sf.createSegmentMethod(trans, segment,
					FreeExtentMethod.class);
		}

		for (int i = 0; i < fems.length; i++) {
			FilePage page = new FilePage((short) 0, 100 + i);
			FreeExtentMethod fem = (FreeExtentMethod) sf.getSegmentMethod(page);
			assertTrue(fem != null && fems[i] == fem);
			assertTrue(fem.getSegment().getNextSize() == 4);

			Segment segment = fems[i].getSegment();
			segment.setNextSize(10 + i);
			sf.updateSegment(trans, segment);
		}

		FilePage SEGMENT_MANAGER_ID = new FilePage((short) 0, 2);
		Segment segmentManagerSegment = new Segment(SEGMENT_MANAGER_ID);
		sf.stop();
		sf.start(segmentManagerSegment);

		for (int i = 0; i < fems.length; i++) {
			fems2[i] = (FreeExtentMethod) sf.getSegmentMethod(fems[i]
					.getSegment().getSegmentId());
			assertTrue(fems2[i] != fems[i]);
			assertTrue(fems2[i].getSegment().equals(fems[i].getSegment()));
			assertTrue(fems2[i].getSegment().getNextSize() == 10 + i);
		}

		for (int i = 0; i < fems.length; i++) {
			sf.removeSegmentMethod(trans, fems[i]);
		}

		for (int i = 0; i < fems.length; i++) {
			fems2[i] = (FreeExtentMethod) sf.getSegmentMethod(fems[i]
					.getSegment().getSegmentId());
			assertTrue(fems2[i] == null);
		}

		tm.commitTransaction(trans);
	}

	/**
	 * Allocate extents to a segment, then free them.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testSpaceManager() throws DatabaseException,
			InterruptedException {
		TransactionManager tm = core.getTransactionManager();
		SpaceManager sm = core.getSpaceManager();
		FilePage page = new FilePage((short) 0, 100);
		Segment segment = new Segment(page);

		Transaction trans = tm.beginTransaction();

		int nextSize = segment.getNextSize();

		sm.allocateNextExtent(trans, segment);
		assertFalse(segment.getNewExtent().isNull());
		assertTrue(segment.getNewExtent().getSize() == segment.getInitialSize());

		nextSize = segment.getNextSize();

		sm.allocateNextExtent(trans, segment);
		assertTrue(segment.getNewExtent().getSize() == nextSize);

		nextSize = segment.getNextSize();
		int nextNextSize = (int) (segment.getGrowthRate() * nextSize);

		sm.allocateNextExtent(trans, segment);
		assertTrue(segment.getNewExtent().getSize() == nextSize);
		assertTrue(segment.getNextSize() == nextNextSize);

		// cause file system allocation
		segment.setNextSize(1024);
		segment.setGrowthRate(2);
		sm.allocateNextExtent(trans, segment);

		assertTrue(segment.getNewExtent().getSize() == 1024);
		assertTrue(segment.getNextSize() == 2048);

		// free the segment
		sm.freeExtents(trans, segment);

		tm.commitTransaction(trans);
	}

	/**
	 * Allocate extents to a segment, then free them, then test failure scenario
	 * 1.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testFailure1() throws DatabaseException, InterruptedException {
		TransactionManager tm = core.getTransactionManager();
		SpaceManager sm = core.getSpaceManager();
		FilePage page = new FilePage((short) 0, 100);
		Segment segment = new Segment(page);

		Transaction trans = tm.beginTransaction();

		int nextSize = segment.getNextSize();

		sm.allocateNextExtent(trans, segment);
		assertFalse(segment.getNewExtent().isNull());
		assertTrue(segment.getNewExtent().getSize() == segment.getInitialSize());

		nextSize = segment.getNextSize();

		sm.allocateNextExtent(trans, segment);
		assertTrue(segment.getNewExtent().getSize() == nextSize);

		nextSize = segment.getNextSize();
		int nextNextSize = (int) (segment.getGrowthRate() * nextSize);

		sm.allocateNextExtent(trans, segment);
		assertTrue(segment.getNewExtent().getSize() == nextSize);
		assertTrue(segment.getNextSize() == nextNextSize);

		// cause file system allocation failure 1
		segment.setNextSize(1024);
		segment.setGrowthRate(2);
		try {
			SpaceManager.testFailure1 = true;
			sm.allocateNextExtent(trans, segment);
		} catch (Exception e) {
			// restart
			core.stopDatabase();
			core.startDatabase();

			tm = core.getTransactionManager();
			sm = core.getSpaceManager();
			trans = tm.beginTransaction();

			SpaceManager.testFailure1 = false;
			sm.allocateNextExtent(trans, segment);
		}

		assertTrue(segment.getNewExtent().getSize() == 1024);
		assertTrue(segment.getNextSize() == 2048);

		// free the segment
		sm.freeExtents(trans, segment);

		tm.commitTransaction(trans);
	}

	/**
	 * Allocate extents to a segment, then free them, then test failure scenario
	 * 2.
	 *
	 * @throws DatabaseException
	 * @throws InterruptedException
	 */
	@Test
	public void testFailure2() throws DatabaseException, InterruptedException {
		TransactionManager tm = core.getTransactionManager();
		SpaceManager sm = core.getSpaceManager();
		FilePage page = new FilePage((short) 0, 100);
		Segment segment = new Segment(page);

		Transaction trans = tm.beginTransaction();

		int nextSize = segment.getNextSize();

		sm.allocateNextExtent(trans, segment);
		assertFalse(segment.getNewExtent().isNull());
		assertTrue(segment.getNewExtent().getSize() == segment.getInitialSize());

		nextSize = segment.getNextSize();

		sm.allocateNextExtent(trans, segment);
		assertTrue(segment.getNewExtent().getSize() == nextSize);

		nextSize = segment.getNextSize();
		int nextNextSize = (int) (segment.getGrowthRate() * nextSize);

		sm.allocateNextExtent(trans, segment);
		assertTrue(segment.getNewExtent().getSize() == nextSize);
		assertTrue(segment.getNextSize() == nextNextSize);

		// cause file system allocation failure 1
		segment.setNextSize(1024);
		segment.setGrowthRate(2);
		try {
			SpaceManager.testFailure2 = true;
			sm.allocateNextExtent(trans, segment);
		} catch (Exception e) {
			// restart
			core.stopDatabase();
			core.startDatabase();

			tm = core.getTransactionManager();
			sm = core.getSpaceManager();
			trans = tm.beginTransaction();

			// induce recovery by causing another extension
			SpaceManager.testFailure2 = false;
			segment.setNextSize(1200);
			sm.allocateNextExtent(trans, segment);
		}

		assertTrue(segment.getNewExtent().getSize() == 1200);
		assertTrue(segment.getNextSize() == 2400);

		// induce one more extension
		segment.setNextSize(2500);
		sm.allocateNextExtent(trans, segment);

		// free the segment
		sm.freeExtents(trans, segment);

		tm.commitTransaction(trans);
	}

	private int random(int n) {
		return (int) (Math.random() * n);
	}

	Core core;
}
