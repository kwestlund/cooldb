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

package com.cooldb.api.impl;

import com.cooldb.api.*;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class APITest {

    CoolDB coolDB;
    Database db;
    String results = "";

    @Before
    public void setUp() throws DatabaseException {
        System.gc();
        File file = new File("build/tmp/cooldb");
        file.mkdirs();
        coolDB = CoolDB.getInstance();
        db = coolDB.getDatabase(file);
        db.replaceDatabase();
    }

    @After
    public void tearDown() {
        coolDB.destroyDatabase(db.getPath());
        db = null;
        coolDB = null;
        System.gc();
    }

    /**
     * Create a number of tables.
     *
     * @throws Exception
     */
    @Test
    public void testCreateTable() throws Exception {
        Session session = db.createSession();
        Table table;
        try {
            table = session.createTable("ken", "sSbiIldaAx");
            throw new Exception("Was expecting an exception.");
        } catch (DatabaseException e) {
            table = session.createTable("ken", "sSbiIldaA");
            Assert.assertTrue(table != null);
        }

        Assert.assertTrue(session.getTable("kenx") == null);
        table = session.getTable("ken");
        Assert.assertTrue(table != null);

        Row row = null;
        RID rid = null;
        RID rid33 = null;
        ;
        for (int i = 0; i < 100; i++) {
            table = session.createTable("SomeAverageTableName" + i,
                                        "IIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIIII");
            row = table.allocateRow();
            row.setInt(3, i);
            rid = table.insert(row);
            if (i == 33)
                rid33 = rid;
        }

        // Now get one of those tables and validate
        Assert.assertTrue(rid33 != null);
        table = session.getTable("SomeAverageTableName33");
        Assert.assertTrue(table != null);
        row = table.allocateRow();
        table.fetch(rid33, row);
        Assert.assertTrue(row.getInt(3) == 33);
    }

    /**
     * Create a table with several columns, one of each type, then insert,
     * update, and delete rows.
     *
     * @throws DatabaseException
     */
    @Test
    public void testTable() throws DatabaseException {
        Session session = db.createSession();
        Table table = session.createTable("ken", "sSbiIldaA");

        // test insert
        Row row = table.allocateRow();
        Row row2 = table.allocateRow();

        row.setString(0, "hello");
        row.setString(1, " world!");
        row.setByte(2, Byte.MAX_VALUE);
        row.setShort(3, Short.MAX_VALUE);
        row.setInt(4, Integer.MAX_VALUE);
        row.setLong(5, Long.MAX_VALUE);
        row.setDouble(6, Double.MAX_VALUE);
        row.setBytes(7, new byte[]{(byte) 1, (byte) 2, (byte) 3});
        row.setBytes(8, new byte[]{(byte) 4, (byte) 5, (byte) 6, (byte) 7,
                (byte) 8, (byte) 9});

        RID rowid = table.insert(row);

        table.fetch(rowid, row2);
        Assert.assertTrue(row.equals(row2));

        // test update
        row.setString(0, "xxxxxxxxxxxxxxxxxx");
        row.setString(1, "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy");
        row.setByte(2, (byte) 2);
        row.setShort(3, (short) 3);
        row.setInt(4, 4);
        row.setLong(5, 5);
        row.setDouble(6, 6);
        row.setBytes(7, new byte[]{(byte) 7});
        row.setBytes(8, new byte[]{(byte) 8});

        table.update(rowid, row);

        table.fetch(rowid, row2);
        Assert.assertTrue(row.equals(row2));

        // test delete
        table.remove(rowid);

        Assert.assertFalse(table.fetch(rowid, row2));
        Assert.assertTrue(row.equals(row2));
    }

    /**
     * Create a sequence, then generate a bunch of numbers, make sure they are
     * as expected, then create a second session and generate a bunch of
     * numbers, then rollback the first session, then generate some more numbers
     * in the second session and validate. Restart the database, generate some
     * more numbers and verify persistence.
     *
     * @throws DatabaseException
     */
    @Test
    public void testSequence() throws DatabaseException {
        Session session = db.createSession();
        Sequence sequence = session.createSequence("ken");

        for (int i = 0; i < 333; i++)
            Assert.assertTrue(sequence.next() == i);

        for (int i = 333; i < 1333; i++)
            Assert.assertTrue(sequence.next() == i);

        session.rollback();

        for (int i = 1333; i < 2000; i++)
            Assert.assertTrue(sequence.next() == i);

        // hard stop, restart
        db.stopDatabase();
        db.startDatabase();

        session = db.createSession();
        sequence = session.getSequence("ken");

        for (int i = 2000; i < 2050; i++)
            Assert.assertTrue(sequence.next() == i);

        // hard stop, restart
        db.stopDatabase();
        db.startDatabase();

        session = db.createSession();
        sequence = session.getSequence("ken");

        // these tests assume a cache size of 100

        for (int i = 2100; i < 2201; i++)
            Assert.assertTrue(sequence.next() == i);

        // hard stop, restart
        db.stopDatabase();
        db.startDatabase();

        session = db.createSession();
        sequence = session.getSequence("ken");

        for (int i = 2300; i < 3000; i++)
            Assert.assertTrue(sequence.next() == i);
    }

    /**
     * Create a table, insert several rows, then open a cursor to scan, update,
     * and delete the rows.
     *
     * @throws Exception
     */
    @Test
    public void testTableCursor() throws Exception {
        Session session = db.createSession();
        Table table = session.createTable("ken", "sSbiIldaA");
        Table table2 = session.createTable("ken1", "sSbiIldaA");

        // test insert
        Row row = table.allocateRow();
        Row row2 = table2.allocateRow();

        row.setString(0, "hello");
        row.setString(1, " world!");
        row.setByte(2, Byte.MAX_VALUE);
        row.setShort(3, Short.MAX_VALUE);
        row.setInt(4, Integer.MAX_VALUE);
        row.setLong(5, Long.MAX_VALUE);
        row.setDouble(6, Double.MAX_VALUE);
        row.setBytes(7, new byte[]{(byte) 1, (byte) 2, (byte) 3});
        row.setBytes(8, new byte[]{(byte) 4, (byte) 5, (byte) 6, (byte) 7,
                (byte) 8, (byte) 9});

        try {
            table2.insert(row);
            throw new Exception("Was expecting an error");
        } catch (DatabaseException e) {
            table2.insert(row2);
        }

        for (int i = 0; i < 10000; i++) {
            row.setInt(4, i);
            table.insert(row);
        }

        session.commit();

        Cursor cursor = table.allocateCursor();
        cursor.open();
        try {
            cursor.removeCurrent();
            throw new Exception("Was expecting an error");
        } catch (DatabaseException e) {
        }

        try {
            cursor.updateCurrent(row);
            throw new Exception("Was expecting an error");
        } catch (DatabaseException e) {
        }

        for (int i = 0; i < 10000; i++) {
            cursor.fetchNext(row);
            Assert.assertTrue(row.getInt(4) == i);
        }
        cursor.rewind();
        for (int i = 0; i < 10000; i++) {
            cursor.fetchNext(row);
            row.setInt(4, 10001);
            cursor.updateCurrent(row);
        }
        cursor.rewind();
        for (int i = 0; i < 10000; i++) {
            cursor.fetchNext(row);
            Assert.assertTrue(row.getInt(4) == i);
        }
        cursor.open();
        for (int i = 0; i < 10000; i++) {
            cursor.fetchNext(row);
            Assert.assertTrue(row.getInt(4) == 10001);
        }
        cursor.rewind();
        for (int i = 0; i < 10000; i++) {
            cursor.fetchNext(row);
            cursor.removeCurrent();
        }
        cursor.rewind();
        for (int i = 0; i < 10000; i++) {
            cursor.fetchNext(row);
            Assert.assertTrue(row.getInt(4) == 10001);
        }
        cursor.open();
        Assert.assertFalse(cursor.fetchNext(row));

        session.commit();
    }

    /**
     * Create a table, insert several rows, then open a cursor to scan using a
     * comparator.
     *
     * @throws DatabaseException
     */
    @Test
    public void testComparator() throws DatabaseException {
        Session session = db.createSession();
        Table table = session.createTable("ken", "sSbiIldaA");

        Row row = table.allocateRow();

        row.setString(0, "hello");
        row.setString(1, " world!");
        row.setByte(2, Byte.MAX_VALUE);
        row.setShort(3, Short.MAX_VALUE);
        row.setInt(4, Integer.MAX_VALUE);
        row.setLong(5, Long.MAX_VALUE);
        row.setDouble(6, Double.MAX_VALUE);
        row.setBytes(7, new byte[]{(byte) 1, (byte) 2, (byte) 3});
        row.setBytes(8, new byte[]{(byte) 4, (byte) 5, (byte) 6, (byte) 7,
                (byte) 8, (byte) 9});

        for (int i = 0; i < 10000; i++) {
            row.setInt(4, i);
            table.insert(row);
        }

        session.commit();

        Cursor cursor = table.allocateCursor();
        Row row2 = table.allocateRow();
        Assert.assertTrue(cursor.fetchNext(row2, new RowComparator(1234)));

        row.setInt(4, 1234);
        Assert.assertTrue(row.equals(row2));

        session.commit();
    }

    /**
     * Create a table with several columns, than create an index on that table.
     *
     * @throws Exception
     */
    @Test
    public void testCreateIndex() throws Exception {
        Session session = db.createSession();
        Table table = session.createTable("ken", "sSbiIldaA");
        byte[] keyMap = new byte[]{4};
        Index index = table.createIndex(keyMap, true);

        db.stopDatabase();
        db.startDatabase();

        session = db.createSession();
        table = session.getTable("ken");
        index = table.getIndex(keyMap);

        // test insert
        Row row = table.allocateRow();
        Row row2 = table.allocateRow();

        row.setString(0, "hello");
        row.setString(1, " world!");
        row.setByte(2, Byte.MAX_VALUE);
        row.setShort(3, Short.MAX_VALUE);
        row.setInt(4, Integer.MAX_VALUE);
        row.setLong(5, Long.MAX_VALUE);
        row.setDouble(6, Double.MAX_VALUE);
        row.setBytes(7, new byte[]{(byte) 1, (byte) 2, (byte) 3});
        row.setBytes(8, new byte[]{(byte) 4, (byte) 5, (byte) 6, (byte) 7,
                (byte) 8, (byte) 9});

        int rows = 10000;
        for (int i = 0; i < rows; i++) {
            row.setInt(4, i);
            table.insert(row);
        }
        // Test uniqueness contraint
        try {
            table.insert(row);
            throw new Exception("Was expecting an exception.");
        } catch (DatabaseException dbe) {
        }

        // Now scan the table using the index
        Cursor ic = index.allocateCursor();
        scan(ic, rows, row, row2);

        // Drop the index, then re-create the index on table with
        // pre-existing rows, then
        // scan again to validate the index is populated.
        table.dropIndex(keyMap);
        index = table.createIndex(keyMap, true);
        ic = index.allocateCursor();
        scan(ic, rows, row, row2);

        deleteAll(ic, rows, row, row2);
        table.dropIndex(keyMap);
        session.dropTable(table.getName());
    }

    /**
     * Create a table with several columns of various types, then create various
     * indexes on that table with different combinations of columns as keys and
     * test the various index functions with each different key type.
     *
     * @throws Exception
     */
    @Test
    public void testKeyTypes() throws Exception {
        for (int j = 0; j < 2; j++) {
            Session session = db.createSession();
            Table table = session.createTable("ken", "sSbiIldaA");
            byte[] keyMap = new byte[]{4, 0, 1, 2, 3, 5, 6, 7, 8};
            Index index = table.createIndex(keyMap, true);
            String[] strings = new String[]{"hello", "world", "ken",
                    "westlund", "liam robert westlund & erin rose westlund"};

            db.stopDatabase();
            db.startDatabase();

            session = db.createSession();
            table = session.getTable("ken");
            index = table.getIndex(keyMap);

            // test insert
            Row row = table.allocateRow();
            Row row2 = table.allocateRow();

            row.setString(0, "hello");
            row.setString(1, " world!");
            row.setByte(2, Byte.MAX_VALUE);
            row.setShort(3, Short.MAX_VALUE);
            row.setInt(4, Integer.MAX_VALUE);
            row.setLong(5, Long.MAX_VALUE);
            row.setDouble(6, Double.MAX_VALUE);
            row.setBytes(7, new byte[]{(byte) 1, (byte) 2, (byte) 3});
            row.setBytes(8, new byte[]{(byte) 4, (byte) 5, (byte) 6,
                    (byte) 7, (byte) 8, (byte) 9});

            int rows = 10000;

            for (int k = 0; k < 2; k++) {
                for (int i = 0; i < rows; i++) {
                    row.setString(0, strings[random(4)]);
                    row.setString(1, strings[random(4)]);
                    row.setNull(2, true);
                    row.setShort(3, (short) i);
                    row.setInt(4, i);
                    row.setShort(5, (short) i);
                    row.setDouble(6, .5 + i);
                    table.insert(row);
                }
                // Test uniqueness contraint
                try {
                    table.insert(row);
                    throw new Exception("Was expecting an exception.");
                } catch (DatabaseException dbe) {
                }

                // Now scan the table using the index
                Cursor ic = index.allocateCursor();
                scanTypes(ic, rows, row, row2);

                // Drop the index, then re-create the index on table with
                // pre-existing rows, then
                // scan again to validate the index is populated.
                table.dropIndex(keyMap);
                index = table.createIndex(keyMap, true);
                ic = index.allocateCursor();
                scanTypes(ic, rows, row, row2);

                deleteAll(ic, rows, row, row2);
            }
            table.dropIndex(keyMap);
            session.dropTable(table.getName());
            session.commit();
        }
    }

    /**
     * Create a table with several columns of various types, then create an
     * index with non-unique key and test the various index functions.
     *
     * @throws Exception
     */
    @Test
    public void testNonUniqueKey() throws Exception {
        Session session = db.createSession();
        Table table = session.createTable("ken", "sSbiIldaA");
        byte[] keyMap = new byte[]{0, 2};
        Index index = table.createIndex(keyMap, false);
        String[] strings = new String[]{"hello", "world", "ken", "westlund",
                "liam robert westlund & erin rose westlund"};
        String[] sortedStrings = new String[]{"hello", "ken",
                "liam robert westlund & erin rose westlund", "westlund",
                "world"};
        int[] sortedIndex = new int[]{0, 2, 4, 3, 1};

        db.stopDatabase();
        db.startDatabase();

        session = db.createSession();
        table = session.getTable("ken");
        index = table.getIndex(keyMap);

        // test insert
        Row row = table.allocateRow();
        Row row2 = table.allocateRow();

        row.setString(0, "hello");
        row.setString(1, " world!");
        row.setByte(2, Byte.MAX_VALUE);
        row.setShort(3, Short.MAX_VALUE);
        row.setInt(4, Integer.MAX_VALUE);
        row.setLong(5, Long.MAX_VALUE);
        row.setDouble(6, Double.MAX_VALUE);
        row.setBytes(7, new byte[]{(byte) 1, (byte) 2, (byte) 3});
        row.setBytes(8, new byte[]{(byte) 4, (byte) 5, (byte) 6, (byte) 7,
                (byte) 8, (byte) 9});

        int rows = 10000;
        for (int i = 0; i < rows; i++) {
            row.setString(0, strings[i % 5]);
            row.setString(1, strings[i % 5]);
            row.setByte(2, (byte) (i % 5));
            row.setShort(3, (short) i);
            row.setInt(4, i);
            row.setShort(5, (short) i);
            row.setDouble(6, .5 + i);
            table.insert(row);
        }

        // Now scan the table using the index
        Cursor ic = index.allocateCursor();
        scanNonUnique(ic, rows, row, row2, strings, sortedStrings, sortedIndex);

        // Drop the index, then re-create the index on table with
        // pre-existing rows, then
        // scan again to validate the index is populated.
        table.dropIndex(keyMap);
        index = table.createIndex(keyMap, false);
        ic = index.allocateCursor();
        scanNonUnique(ic, rows, row, row2, strings, sortedStrings, sortedIndex);

        deleteAll(ic, rows, row, row2);
        table.dropIndex(keyMap);
        session.dropTable(table.getName());
    }

    private void scan(Cursor ic, int rows, Row row, Row row2)
            throws DatabaseException {
        ic.open();
        ic.forward();
        int i = rows;
        while (ic.fetchPrev(row2)) {
            row.setInt(4, --i);
            Assert.assertTrue(row.equals(row2));
        }
        Assert.assertTrue(i == 0);

        ic.rewind();
        while (ic.fetchNext(row2)) {
            row.setInt(4, i++);
            Assert.assertTrue(row.equals(row2));
        }
        Assert.assertTrue(i == rows);
    }

    private void scanTypes(Cursor ic, int rows, Row row, Row row2)
            throws DatabaseException {
        ic.open();
        ic.forward();
        int i = rows;
        while (ic.fetchPrev(row2)) {
            --i;
            Assert.assertTrue(row2.isNull(2));
            Assert.assertTrue(row2.getInt(3) == i);
            Assert.assertTrue(row2.getInt(4) == i);
            Assert.assertTrue(row2.getInt(5) == i);
            Assert.assertTrue(row2.getDouble(6) == .5 + i);
            Assert.assertTrue(Arrays.equals(row2.getBytes(7), row.getBytes(7)));
            Assert.assertTrue(Arrays.equals(row2.getBytes(8), row.getBytes(8)));
        }
        Assert.assertTrue(i == 0);
        ic.rewind();
        while (ic.fetchNext(row2)) {
            Assert.assertTrue(row2.isNull(2));
            Assert.assertTrue(row2.getInt(3) == i);
            Assert.assertTrue(row2.getInt(4) == i);
            Assert.assertTrue(row2.getInt(5) == i);
            Assert.assertTrue(row2.getDouble(6) == .5 + i);
            Assert.assertTrue(Arrays.equals(row2.getBytes(7), row.getBytes(7)));
            Assert.assertTrue(Arrays.equals(row2.getBytes(8), row.getBytes(8)));
            ++i;
        }
        Assert.assertTrue(i == rows);
    }

    private void scanNonUnique(Cursor ic, int rows, Row row, Row row2,
                               String[] strings, String[] sortedStrings, int[] sortedIndex)
            throws DatabaseException {
        ic.open();
        ic.rewind();
        int i = 0;
        while (ic.fetchNext(row2)) {
            int j = i * 5 / rows;
            int k = row2.getByte(2);
            String s = row2.getString(0);
            Assert.assertTrue(k == sortedIndex[j]);
            Assert.assertTrue(s.equals(strings[sortedIndex[j]]));
            ++i;
        }
        Assert.assertTrue(i == rows);
    }

    private void deleteAll(Cursor ic, int rows, Row row, Row row2)
            throws DatabaseException {
        ic.open();
        ic.rewind();
        while (ic.fetchNext(row2))
            ic.removeCurrent();
    }

    /**
     * This example creates a database at "test" and a table named "cool", then
     * inserts 100,000 rows setting the first column of each row to the row
     * sequence number.
     *
     * @throws DatabaseException
     */
    @Test
    public void testExample1() throws DatabaseException {
        File file = new File("build/tmp/cooldb/temp");
        file.mkdirs();

        // Create and start a new database
        Database db = coolDB.getDatabase(new File("build/tmp/cooldb/temp"));
        db.replaceDatabase();

        // Create a session
        Session session = db.createSession();

        // Create a table with 2 colums, one of type integer(I) and one of
        // type small string(s)
        Table table = session.createTable("cool", "Is");

        // Allocate a Row for use in modifying the table
        Row row = table.allocateRow();

        // Insert 100,000 rows
        row.setString(1, "hello world");
        for (int i = 0; i < 100000; i++) {
            row.setInt(0, i);
            table.insert(row);
        }

        // Commit the transaction
        session.commit();

        // Stop the database
        db.stopDatabase();
    }

    /**
     * This example starts the database created in Example 1 at "test", deletes
     * all rows from the table "cool" except for the single row whose first
     * column is equal to 12345, and updates that row's second column with a new
     * string value. Finally it prints the contents of the "cool" table.
     *
     * @throws DatabaseException
     */
    @Test
    public void testExample2() throws DatabaseException {
        // Start the database
        File file = new File("build/tmp/cooldb/temp");
        file.mkdirs();

        Database db = coolDB.getDatabase(new File("build/tmp/cooldb/temp"));
        db.startDatabase();

        // Create a session
        Session session = db.createSession();

        // Get the table
        Table table = session.getTable("cool");

        // Allocate a TableCursor and a Row for use in modifying the table
        Cursor cursor = table.allocateCursor();
        Row row = table.allocateRow();

        // Update the row where column 1 equals 12345
        while (cursor.fetchNext(row)) {
            if (row.getInt(0) == 12345) {
                row.setString(1, "the quick brown fox jumps over the lazy dog");
                cursor.updateCurrent(row);
            } else
                cursor.removeCurrent();
        }

        // Commit the transaction
        session.commit();

        // Display the contents of the table
        cursor.open();
        while (cursor.fetchNext(row))
            System.out.println("Column1=" + row.getString(0) + " Column2="
                                       + row.getString(1));

        // Commit the transaction
        session.commit();

        // Stop the database
        db.stopDatabase();
    }

    /**
     * This example starts the database modified in Example 2 and creates a
     * unique index on the first column of table "cool", inserts 10000 records,
     * then uses the index to retrieve and print the row whose first column =
     * 12345.
     *
     * @throws DatabaseException
     */
    @Test
    public void testExample3() throws DatabaseException {
        // Start the database
        File file = new File("build/tmp/cooldb/temp");
        file.mkdirs();

        Database db = coolDB.getDatabase(new File("build/tmp/cooldb/temp"));
        db.startDatabase();

        // Create a session
        Session session = db.createSession();

        // Get the table
        Table table = session.getTable("cool");

        // Create the index
        byte[] keyMap = new byte[]{0}; // the column at pos 0 in the
        // table is indexed
        Index index = table.createIndex(keyMap, true);

        // Allocate a Row for use in modifying the table
        Row row = table.allocateRow();

        // Insert 10,000 rows
        row.setString(1, "hello world");
        for (int i = 0; i < 10000; i++) {
            row.setInt(0, i);
            table.insert(row);
        }

        // Lookup the row with key = 12345
        Key key = index.allocateKey();
        key.setInt(0, 12345);
        index.fetch(key, row);

        // Print the row
        System.out.println("Column1=" + row.getString(0) + " Column2="
                                   + row.getString(1));

        // Commit the transaction
        session.commit();

        // Stop the database
        db.stopDatabase();
    }

    /**
     * This test runs a set of insert and delete operations, then restarts to
     * cause a purge to happen on the keys deleted.
     *
     * @throws DatabaseException
     */
    @Test
    public void testRestartCompact() throws DatabaseException {
        TestDatum[] testData = new TestDatum[]{
                new TestDatum(true, 13947, "E1.DatabaseWriter"),
                new TestDatum(true, 14043, "E1.Expert"),
                new TestDatum(true, 14133, "E2.BasketServer"),
                new TestDatum(true, 14288, "IMSalesTrader"),
                new TestDatum(true, 14416, "P.BIG"),
                new TestDatum(false, 14288, "IMSalesTrader"),
                new TestDatum(true, 15407, "IMSalesTrader"),
                new TestDatum(false, 15407, "IMSalesTrader"),
                new TestDatum(true, 16943, "IMSalesTrader"),
                new TestDatum(false, 16943, "IMSalesTrader"),
                new TestDatum(false, 14043, "E1.Expert"),
                new TestDatum(false, 14133, "E2.BasketServer"),
                new TestDatum(false, 13947, "E1.DatabaseWriter"),
                new TestDatum(false, 14416, "P.BIG"),
                new TestDatum(true, 25836, "P.BIG"),
                new TestDatum(true, 25862, "E1.Expert"),
                new TestDatum(true, 25896, "E2.BasketServer"),
                new TestDatum(true, 25939, "E1.DatabaseWriter"),
                new TestDatum(true, 25979, "IMSalesTrader"),
                new TestDatum(false, 25979, "IMSalesTrader"),
                new TestDatum(true, 26879, "IMSalesTrader"),
                new TestDatum(false, 26879, "IMSalesTrader"),
                new TestDatum(false, 25836, "P.BIG"),
                new TestDatum(false, 25862, "E1.Expert"),
                new TestDatum(false, 25896, "E2.BasketServer"),
                new TestDatum(false, 25939, "E1.DatabaseWriter"),
                new TestDatum(true, 1104, "P.BIG"),
                new TestDatum(true, 1139, "E1.Expert"),
                new TestDatum(true, 1162, "E2.BasketServer"),
                new TestDatum(true, 1182, "E1.DatabaseWriter"),
                new TestDatum(true, 1212, "IMSalesTrader"),
                new TestDatum(false, 1139, "E1.Expert"),
                new TestDatum(false, 1162, "E2.BasketServer"),
                new TestDatum(false, 1212, "IMSalesTrader"),
                new TestDatum(true, 2396, "IMSalesTrader"),
                new TestDatum(false, 2396, "IMSalesTrader"),
                new TestDatum(false, 7498, "E1.Expert"),
                new TestDatum(false, 1104, "P.BIG"),
                new TestDatum(false, 1182, "E1.DatabaseWriter"),
                new TestDatum(true, 10660, "P.BIG"),
                new TestDatum(true, 10692, "E1.Expert"),
                new TestDatum(true, 10725, "E2.BasketServer"),
                new TestDatum(true, 10762, "E1.DatabaseWriter"),
                new TestDatum(true, 10824, "IMSalesTrader"),
                new TestDatum(false, 10824, "IMSalesTrader"),
                new TestDatum(true, 3799, "IMSalesTrader"),
                new TestDatum(false, 3799, "IMSalesTrader"),
                new TestDatum(false, 10660, "P.BIG"),
                new TestDatum(false, 10692, "E1.Expert"),
                new TestDatum(false, 10725, "E2.BasketServer"),
                new TestDatum(false, 10762, "E1.DatabaseWriter"),
                new TestDatum(true, 26613, "E1.DatabaseWriter"),
                new TestDatum(true, 26709, "E1.Expert"),
                new TestDatum(true, 26811, "E2.BasketServer"),
                new TestDatum(true, 26950, "P.BIG"),
                new TestDatum(true, 27079, "IMSalesTrader"),
                new TestDatum(false, 27079, "IMSalesTrader"),
                new TestDatum(true, 27496, "IMSalesTrader"),
                new TestDatum(false, 27496, "IMSalesTrader"),
                new TestDatum(true, 29060, "IMSalesTrader"),
                new TestDatum(false, 29060, "IMSalesTrader"),
                new TestDatum(false, 26613, "E1.DatabaseWriter"),
                new TestDatum(false, 26709, "E1.Expert"),
                new TestDatum(false, 26811, "E2.BasketServer"),
                new TestDatum(false, 26950, "P.BIG"),
                new TestDatum(true, 31400, "P.BIG"),
                new TestDatum(true, 31434, "E1.Expert"),
                new TestDatum(true, 31464, "E2.BasketServer"),
                new TestDatum(true, 31503, "E1.DatabaseWriter"),
                new TestDatum(true, 31529, "IMSalesTrader"),
                new TestDatum(false, 31529, "IMSalesTrader"),
                new TestDatum(false, 31434, "E1.Expert"),
                new TestDatum(true, 23242, "E1.Expert"),
                new TestDatum(false, 23242, "E1.Expert"),
                new TestDatum(true, 27458, "E1.Expert"),
                new TestDatum(false, 27458, "E1.Expert"),
                new TestDatum(true, 27665, "E1.Expert"),
                new TestDatum(false, 27665, "E1.Expert"),
                new TestDatum(true, 30210, "E1.Expert"),
                new TestDatum(false, 31400, "P.BIG"),
                new TestDatum(false, 31464, "E2.BasketServer"),
                new TestDatum(false, 31503, "E1.DatabaseWriter"),
                new TestDatum(false, 30210, "E1.Expert"),
                new TestDatum(true, 32252, "P.BIG"),
                new TestDatum(true, 32300, "E1.Expert"),
                new TestDatum(true, 32329, "E2.BasketServer"),
                new TestDatum(true, 32348, "E1.DatabaseWriter"),
                new TestDatum(true, 32371, "IMSalesTrader"),
                new TestDatum(false, 32371, "IMSalesTrader"),
                new TestDatum(false, 32329, "E2.BasketServer"),
                new TestDatum(true, 5022, "E2.BasketServer"),
                new TestDatum(false, 5022, "E2.BasketServer"),
                new TestDatum(true, 10991, "E2.BasketServer"),
                new TestDatum(false, 10991, "E2.BasketServer"),
                new TestDatum(true, 13288, "E2.BasketServer"),
                new TestDatum(true, 16419, "IMSalesTrader"),
                new TestDatum(false, 16419, "IMSalesTrader"),
                new TestDatum(true, 17085, "IMSalesTrader"),
                new TestDatum(false, 17085, "IMSalesTrader"),
                new TestDatum(false, 32300, "E1.Expert"),
                new TestDatum(true, 18133, "E1.Expert"),
                new TestDatum(true, 20148, "IMSalesTrader"),
                new TestDatum(false, 20148, "IMSalesTrader"),
                new TestDatum(true, 3531, "IMSalesTrader"),
                new TestDatum(false, 3531, "IMSalesTrader"),
                new TestDatum(false, 18133, "E1.Expert"),
                new TestDatum(true, 3738, "E1.Expert"),
                new TestDatum(false, 32252, "P.BIG"),
                new TestDatum(false, 32348, "E1.DatabaseWriter"),
                new TestDatum(false, 13288, "E2.BasketServer"),
                new TestDatum(false, 3738, "E1.Expert"),
                new TestDatum(true, 11662, "P.BIG"),
                new TestDatum(true, 11682, "E1.Expert"),
                new TestDatum(true, 11714, "E2.BasketServer"),
                new TestDatum(true, 11776, "E1.DatabaseWriter"),
                new TestDatum(true, 11819, "IMSalesTrader"),
                new TestDatum(false, 11819, "IMSalesTrader"),
                new TestDatum(false, 11662, "P.BIG"),
                new TestDatum(false, 11682, "E1.Expert"),
                new TestDatum(false, 11714, "E2.BasketServer"),
                new TestDatum(false, 11776, "E1.DatabaseWriter"),
                new TestDatum(true, 31964, "P.BIG"),
                new TestDatum(true, 31972, "E1.Expert"),
                new TestDatum(true, 32005, "E2.BasketServer"),
                new TestDatum(true, 32041, "E1.DatabaseWriter"),
                new TestDatum(true, 32077, "IMSalesTrader"),
                new TestDatum(false, 32077, "IMSalesTrader"),
                new TestDatum(false, 31964, "P.BIG"),
                new TestDatum(false, 31972, "E1.Expert"),
                new TestDatum(false, 32005, "E2.BasketServer"),
                new TestDatum(false, 32041, "E1.DatabaseWriter"),
                new TestDatum(true, 15778, "P.BIG"),
                new TestDatum(true, 15783, "E1.Expert"),
                new TestDatum(true, 15792, "E2.BasketServer"),
                new TestDatum(true, 15799, "E1.DatabaseWriter"),
                new TestDatum(true, 15812, "IMSalesTrader"),
                new TestDatum(false, 15812, "IMSalesTrader"),
                new TestDatum(false, 15792, "E2.BasketServer"),
                new TestDatum(true, 17155, "E2.BasketServer"),
                new TestDatum(false, 17155, "E2.BasketServer"),
                new TestDatum(true, 10453, "E2.BasketServer"),
                new TestDatum(false, 15778, "P.BIG"),
                new TestDatum(false, 15783, "E1.Expert"),
                new TestDatum(false, 15799, "E1.DatabaseWriter"),
                new TestDatum(false, 10453, "E2.BasketServer"),
                new TestDatum(true, 4033, "P.BIG"),
                new TestDatum(true, 4056, "E1.Expert"),
                new TestDatum(true, 4088, "E2.BasketServer"),
                new TestDatum(true, 4121, "E1.DatabaseWriter"),
                new TestDatum(true, 4162, "IMSalesTrader"),
                new TestDatum(false, 4162, "IMSalesTrader"),
                new TestDatum(false, 4121, "E1.DatabaseWriter"),
                new TestDatum(false, 4056, "E1.Expert"),
                new TestDatum(false, 4088, "E2.BasketServer"),
                new TestDatum(false, 4033, "P.BIG"),
                new TestDatum(true, 12787, "E1.Expert"),
                new TestDatum(true, 14997, "E1.DatabaseWriter")};

        runTestData(testData);
    }

    /**
     * This test runs a set of insert and delete operations, then restarts to
     * cause a purge to happen on the keys deleted.
     *
     * @throws DatabaseException
     */
    @Test
    public void testData() throws DatabaseException {
        TestDatum[] testData = new TestDatum[]{
                new TestDatum(false, 17995, "Stout"),
                new TestDatum(false, 18104, "Stout"),
                new TestDatum(true, 18301, "P.BIG"),
                new TestDatum(true, 18408, "IMSalesTrader"),
                new TestDatum(true, 18518, "E1.DatabaseWriter"),
                new TestDatum(true, 18618, "E1.Expert"),
                new TestDatum(true, 18761, "E2.BasketServer"),
                new TestDatum(true, 19124, "Stout", true, 2),
                new TestDatum(true, 21969, "Stout"),
                new TestDatum(true, 22138, "E1.DatabaseWriter"),
                new TestDatum(true, 22244, "E1.Expert"),
                new TestDatum(true, 22365, "E2.BasketServer"),
                new TestDatum(true, 22537, "P.BIG"),
                new TestDatum(true, 22672, "IMSalesTrader"),
                new TestDatum(false, 21969, "Stout"),
                new TestDatum(false, 22138, "E1.DatabaseWriter"),
                new TestDatum(false, 22244, "E1.Expert"),
                new TestDatum(false, 22365, "E2.BasketServer"),
                new TestDatum(false, 22672, "IMSalesTrader"),
                new TestDatum(false, 22537, "P.BIG", true, 1),
                new TestDatum(true, 11426, "Stout"),
                new TestDatum(true, 13974, "E1.Expert"),
                new TestDatum(true, 13979, "E1.DatabaseWriter"),
                new TestDatum(true, 13986, "E2.BasketServer"),
                new TestDatum(true, 14020, "P.BIG"),
                new TestDatum(true, 14044, "IMSalesTrader"),
                new TestDatum(false, 13974, "E1.Expert"),
                new TestDatum(false, 14044, "IMSalesTrader"),
                new TestDatum(false, 13979, "E1.DatabaseWriter"),
                new TestDatum(false, 13986, "E2.BasketServer"),
                new TestDatum(false, 14020, "P.BIG"),
                new TestDatum(true, 2831, "E1.DatabaseWriter"),
                new TestDatum(true, 2915, "E1.Expert"),
                new TestDatum(true, 3053, "E2.BasketServer"),
                new TestDatum(true, 3227, "P.BIG"),
                new TestDatum(true, 3424, "IMSalesTrader"),
                new TestDatum(false, 3424, "IMSalesTrader"),
                new TestDatum(false, 2831, "E1.DatabaseWriter"),
                new TestDatum(false, 2915, "E1.Expert"),
                new TestDatum(false, 3053, "E2.BasketServer"),
                new TestDatum(false, 3227, "P.BIG"),
                new TestDatum(true, 7094, "E1.DatabaseWriter"),
                new TestDatum(true, 7209, "E2.BasketServer"),
                new TestDatum(true, 7322, "E1.Expert"),
                new TestDatum(true, 7466, "IMSalesTrader"),
                new TestDatum(true, 7571, "P.BIG"),
                new TestDatum(false, 7466, "IMSalesTrader"),
                new TestDatum(true, 21237, "IMSalesTrader"),
                new TestDatum(false, 21237, "IMSalesTrader"),
                new TestDatum(true, 23632, "IMSalesTrader"),
                new TestDatum(false, 7094, "E1.DatabaseWriter"),
                new TestDatum(false, 7209, "E2.BasketServer"),
                new TestDatum(false, 7322, "E1.Expert"),
                new TestDatum(false, 23632, "IMSalesTrader"),
                new TestDatum(false, 7571, "P.BIG"),
                new TestDatum(true, 26055, "E1.DatabaseWriter"),
                new TestDatum(true, 26149, "E1.Expert"),
                new TestDatum(true, 26269, "E2.BasketServer"),
                new TestDatum(true, 26435, "IMSalesTrader"),
                new TestDatum(true, 26542, "P.BIG", true, 1),
                new TestDatum(false, 26055, "E1.DatabaseWriter"),
                new TestDatum(false, 26269, "E2.BasketServer"),
                new TestDatum(false, 26149, "E1.Expert"),
                new TestDatum(false, 26435, "IMSalesTrader"),
                new TestDatum(false, 26542, "P.BIG"),
                new TestDatum(true, 21907, "E1.DatabaseWriter"),
                new TestDatum(true, 22038, "E2.BasketServer"),
                new TestDatum(true, 22157, "E1.Expert"),
                new TestDatum(true, 22309, "P.BIG"),
                new TestDatum(false, 21907, "E1.DatabaseWriter"),
                new TestDatum(false, 22038, "E2.BasketServer"),
                new TestDatum(false, 22157, "E1.Expert"),
                new TestDatum(false, 22309, "P.BIG"),
                new TestDatum(true, 2000, "E1.Expert"),
                new TestDatum(true, 2001, "E1.DatabaseWriter"),
                new TestDatum(true, 2004, "E2.BasketServer"),
                new TestDatum(true, 2013, "P.BIG"),
                new TestDatum(true, 2023, "IMSalesTrader"),
                new TestDatum(false, 2000, "E1.Expert"),
                new TestDatum(false, 2001, "E1.DatabaseWriter"),
                new TestDatum(false, 2004, "E2.BasketServer"),
                new TestDatum(false, 2013, "P.BIG"),
                new TestDatum(false, 2023, "IMSalesTrader"),
                new TestDatum(true, 29505, "E1.Expert"),
                new TestDatum(true, 29507, "E1.DatabaseWriter"),
                new TestDatum(true, 29526, "E2.BasketServer"),
                new TestDatum(true, 29556, "P.BIG"),
                new TestDatum(true, 29709, "IMSalesTrader"),
                new TestDatum(false, 29505, "E1.Expert"),
                new TestDatum(false, 29507, "E1.DatabaseWriter"),
                new TestDatum(false, 29526, "E2.BasketServer"),
                new TestDatum(false, 29556, "P.BIG"),
                new TestDatum(false, 29709, "IMSalesTrader"),
                new TestDatum(true, 19466, "E1.Expert"),
                new TestDatum(true, 19467, "E1.DatabaseWriter"),
                new TestDatum(true, 19470, "E2.BasketServer"),
                new TestDatum(true, 19482, "P.BIG"),
                new TestDatum(true, 19489, "IMSalesTrader", true, 1),
                new TestDatum(false, 19466, "E1.Expert"),
                new TestDatum(false, 19467, "E1.DatabaseWriter"),
                new TestDatum(false, 19470, "E2.BasketServer"),
                new TestDatum(false, 19489, "IMSalesTrader"),
                new TestDatum(false, 19482, "P.BIG"),
                new TestDatum(true, 28218, "E1.DatabaseWriter"),
                new TestDatum(true, 28300, "E1.Expert"),
                new TestDatum(true, 28406, "E2.BasketServer"),
                new TestDatum(true, 28611, "P.BIG"),
                new TestDatum(false, 28218, "E1.DatabaseWriter"),
                new TestDatum(false, 28300, "E1.Expert"),
                new TestDatum(false, 28406, "E2.BasketServer"),
                new TestDatum(false, 28611, "P.BIG"),
                new TestDatum(true, 29571, "E1.DatabaseWriter"),
                new TestDatum(true, 29686, "E2.BasketServer"),
                new TestDatum(true, 29808, "E1.Expert"),
                new TestDatum(true, 29972, "P.BIG"),
                new TestDatum(false, 29571, "E1.DatabaseWriter"),
                new TestDatum(false, 29686, "E2.BasketServer"),
                new TestDatum(false, 29808, "E1.Expert"),
                new TestDatum(false, 29972, "P.BIG"),
                new TestDatum(true, 31139, "E1.DatabaseWriter"),
                new TestDatum(true, 31226, "E1.Expert"),
                new TestDatum(true, 31361, "E2.BasketServer"),
                new TestDatum(true, 31512, "IMSalesTrader"),
                new TestDatum(true, 31643, "P.BIG"),
                new TestDatum(false, 31226, "E1.Expert"),
                new TestDatum(true, 3120, "E1.Expert", true, 1),
                new TestDatum(false, 28218, "E1.DatabaseWriter"),
                new TestDatum(false, 28611, "P.BIG"),
                new TestDatum(false, 31361, "E2.BasketServer"),
                new TestDatum(false, 31512, "IMSalesTrader"),
                new TestDatum(false, 3120, "E1.Expert"),
                new TestDatum(true, 13547, "E1.DatabaseWriter"),
                new TestDatum(true, 13551, "E1.Expert"),
                new TestDatum(true, 13559, "P.BIG"),
                new TestDatum(true, 13562, "E2.BasketServer"),
                new TestDatum(true, 13570, "IMSalesTrader")};

        runTestData(testData);
    }

    private void runTestData(TestDatum[] testData) throws DatabaseException {
        File file = new File("build/tmp/cooldb/temp");
        file.mkdirs();

        // Create and start a new database
        Database db = coolDB.getDatabase(new File("build/tmp/cooldb/temp"));
        db.replaceDatabase();

        // Create a session
        Session session = db.createSession();

        // Create a table with 2 colums, one of type integer(I) and one of
        // type small string(s)
        Table table = session.createTable("data", "Is");
        Index index = table.createIndex(new byte[]{0}, true);

        // Apply the test data repeatedly
        for (int i = 0; i < testData.length; i++) {
            if (testData[i].insert) {
                insertDatum(table, testData[i], session);
            } else {
                removeDatum(table, testData[i], session, index);
            }
            if (testData[i].restart) {
                for (int r = 0; r < testData[i].restartCount; r++) {
//					System.out.println("Restarting Database:");
//					System.out.println("\tIndex Before Restart:");
//					fetch(index);

                    // index.print();
                    db.stopDatabase();
                    db.startDatabase();

                    session = db.createSession();
                    table = session.getTable("data");
                    index = table.getIndex(new byte[]{0});
//					System.out.println("\tIndex After Restart:");
//					fetch(index);
                }
            }
        }

        // Commit the transaction
        session.commit();

        // Stop the database
        db.stopDatabase();
    }

    private void fetch(Index index) throws DatabaseException {
        ((IndexImpl) index).print("\t");

        IndexCursor cursor = index.allocateCursor();

        cursor.setInclusive(true, true);
        cursor.lowerKey().setMinValue();
        cursor.upperKey().setMaxValue();
        cursor.open();

        Row row = cursor.allocateRow();
        System.out.println("\tFetching Rows:");
        while (cursor.fetchNext(row)) {
            System.out.println("\t\t" + row.toString());
        }

        cursor.close();
    }

    private void insertDatum(Table table, TestDatum datum, Session session)
            throws DatabaseException {
        Row row = table.allocateRow();

        row.setInt(0, datum.a);
        row.setString(1, datum.b);

        table.insert(row);

        session.commit();
    }

    private void removeDatum(Table table, TestDatum datum, Session session,
                             Index index) throws DatabaseException {
        IndexCursor cursor = index.allocateCursor();

        cursor.setInclusive(true, true);
        cursor.lowerKey().setInt(0, datum.a);
        cursor.upperKey().setInt(0, datum.a);

        cursor.open();

        Row row = cursor.allocateRow();
        while (cursor.fetchNext(row))
            table.remove(cursor.getCurrentRID());

        cursor.close();

        session.commit();
    }

    /**
     * In one thread, create a session that inserts only even numbers within a
     * range into a table. In second thread, create another session that inserts
     * only odd numbers within the same range. In both sessions, update a
     * summary table with each insert that reflects the total of some column of
     * all records inserted by both threads. In a third thread, repeatedly sum
     * up all records and verify that the calculated sum equals the value in the
     * summary table record.
     *
     * @throws InterruptedException
     * @throws DatabaseException
     */
    @Test
    public void testIsolation() throws InterruptedException, DatabaseException {
        // Create the tables
        Session session = db.createSession();
        @SuppressWarnings("unused")
        Table det = session.createTable("det", "l");
        Table sum = session.createTable("sum", "l");

        // Create a unique index on the detail table
        det.createIndex(new byte[]{0}, true);

        // Insert the summary row
        Row sumrow = sum.allocateRow();
        sumrow.setInt(0, 0);
        RID sumrid = sum.insert(sumrow);

        session.commit();

        int range = 1000000;
        Inserter i1 = new Inserter(range, sumrid, false);
        Inserter i2 = new Inserter(range, sumrid, true);
        Selecter s1 = new Selecter(sumrid, i1, i2);

        // Let them run to completion
        i1.waitUntilDone();
        i2.waitUntilDone();
        s1.waitUntilDone();

        Assert.assertTrue(!(i1.failed || i2.failed || s1.failed));

        // Stop the database
        db.stopDatabase();
    }

    @Test
    public void testIsolation2() throws Exception {
        // Create the tables
        Session session1 = db.createSession();
        Table det1 = session1.createTable("det", "l");
        Table sum1 = session1.createTable("sum", "l");
        det1.createIndex(new byte[]{0}, true);
        Row sumrow1 = sum1.allocateRow();
        Row detrow1 = det1.allocateRow();
        sumrow1.setInt(0, 0);
        RID sumrid = sum1.insert(sumrow1);
        session1.commit();

        Session session2 = db.createSession();
        Table det2 = session2.getTable("det");
        Table sum2 = session2.getTable("sum");
        Row sumrow2 = sum2.allocateRow();
        Row detrow2 = det2.allocateRow();
        session2.commit();

        Session session3 = db.createSession();
        Table det3 = session3.getTable("det");
        Table sum3 = session3.getTable("sum");
        Row sumrow3 = sum3.allocateRow();
        Row detrow3 = det3.allocateRow();
        Cursor tc3 = det3.allocateCursor();
        session3.commit();

        long total = 0;
        long sum = 0;
        int n = 0;
        int inc = 10000;
        for (int c = 0; c < 9; c++) {
            for (int d = c; d < 9; d++) {
                for (int e = d; e < 9; e++) {
                    for (int f = 0; f < 10; f++) {
                        ++n;

                        // T2: insert num
                        if (f == 0) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        if (c == 0)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 0) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 0)
                            session3.commit();

                        // T2: insert num
                        if (f == 1) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        // T1: insert num
                        detrow1.setInt(0, n);
                        det1.insert(detrow1);

                        // T2: insert num
                        if (f == 2) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        if (c == 1)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 1) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 1)
                            session3.commit();

                        // T2: insert num
                        if (f == 3) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        // T1: fetch for update of the summary row
                        sum1.fetchForUpdate(sumrid, sumrow1);

                        // T2: insert num
                        if (f == 4) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        if (c == 2)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 2) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 2)
                            session3.commit();

                        // T2: insert num
                        if (f == 5) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        if (c == 3)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 3) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 3)
                            session3.commit();

                        // T2: insert num
                        if (f == 6) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        // T1: update the summary row
                        sumrow1.setLong(0, sumrow1.getLong(0) + n);
                        sum1.update(sumrid, sumrow1);

                        // T2: insert num
                        if (f == 7) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        if (c == 4)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 4) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 4)
                            session3.commit();

                        // T2: insert num
                        if (f == 8) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        // T1: commit
                        session1.commit();

                        // T2: insert num
                        if (f == 9) {
                            detrow2.setInt(0, n + inc);
                            det2.insert(detrow2);
                        }

                        if (c == 5)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 5) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 5)
                            session3.commit();

                        // T2: fetch for update of the summary row
                        sum2.fetchForUpdate(sumrid, sumrow2);

                        if (c == 6)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 6) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 6)
                            session3.commit();

                        // T2: update the summary row
                        sumrow2.setLong(0, sumrow2.getLong(0) + n + inc);
                        sum2.update(sumrid, sumrow2);

                        if (c == 7)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 7) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 7)
                            session3.commit();

                        // T2: commit
                        session2.commit();

                        if (c == 8)
                            total = calcTotal(session3, tc3, detrow3);
                        if (d == 8) {
                            sum = getSum(session3, sum3, sumrow3, sumrid);
                            results = "Sum: " + sum + " Det total: " + total
                                    + " run(" + c + "," + d + ")";
                        }
                        if (e == 8)
                            session3.commit();

                        // System.out.println("Results: " + results);

                        if (total != sum) {
                            System.out.println("Failed! run(" + c + "," + d
                                                       + ")");
                            return;
                        }
                    }
                }
            }
        }

        // Stop the database
        db.stopDatabase();
    }

    @Test
    public void testSerializable() throws Exception {

    }

    private void checkTotals(Session session, Cursor tc, Row detrow,
                             Table sum, Row sumrow, RID sumrid) throws Exception {
        long num = getSum(session, sum, sumrow, sumrid);
        long total = calcTotal(session, tc, detrow);

        results = results + "Sum: " + num + " Det total: " + total + "\n";

        if (num != total) {
            total = calcTotal(session, tc, detrow);
            num = getSum(session, sum, sumrow, sumrid);
            results = results + "Sum2: " + num + " Det total: " + total + "\n";
            throw new Exception("Totals do not match");
        }

        session.commit();
    }

    private long calcTotal(Session session, Cursor tc, Row detrow)
            throws Exception {
        tc.open();
        long total = 0;
        while (tc.fetchNext(detrow))
            total += detrow.getLong(0);
        return total;
    }

    private long getSum(Session session, Table sum, Row sumrow, RID sumrid)
            throws Exception {
        sum.fetch(sumrid, sumrow);
        return sumrow.getLong(0);
    }

    private int random(int n) {
        return (int) (Math.random() * n);
    }

    class RowComparator implements Filter {
        int id;

        RowComparator(int id) {
            this.id = id;
        }

        public boolean passes(Object obj) {
            Row row = (Row) obj;
            try {
                if (row.getInt(4) == id)
                    return true;
            } catch (Exception e) {
            }
            return false;
        }
    }

    class TestDatum {
        boolean insert;
        int a;
        String b;
        boolean restart;
        int restartCount;
        TestDatum(boolean insert, int a, String b) {
            this(insert, a, b, false, 0);
        }
        TestDatum(boolean insert, int a, String b, boolean restart,
                  int restartCount) {
            this.insert = insert;
            this.a = a;
            this.b = b;
            this.restart = restart;
            this.restartCount = restartCount;
        }
    }

    private class TestThread extends Thread {
        boolean failed;
        boolean done;

        TestThread() {
            super();
        }

        synchronized void done() {
            done = true;
            notifyAll();
        }

        synchronized void waitUntilDone() throws InterruptedException {
            while (!done) {
                wait();
            }
        }
    }

    private class Inserter extends TestThread {
        private final boolean odd;
        private RID sumrid;

        Inserter(int range, RID sumrid, boolean odd) {
            super();
            this.sumrid = sumrid;
            this.odd = odd;
            start();
        }

        public void run() {
            try {
                Session session = db.createSession();
                Table det = session.getTable("det");
                Table sum = session.getTable("sum");
                Row detrow = det.allocateRow();
                Row sumrow = sum.allocateRow();

                for (int i = odd ? 1 : 0; i < 10000; i += 2) {

                    // insert the number
                    detrow.setInt(0, i);
                    det.insert(detrow);

                    // update the summary row
                    sum.fetchForUpdate(sumrid, sumrow);
                    sumrow.setLong(0, sumrow.getLong(0) + i);
                    sum.update(sumrid, sumrow);

                    session.commit();
                }
            } catch (Exception e) {
                System.err.println(e.getMessage());
                failed = true;
            }

            done();
        }
    }

    private class Selecter extends TestThread {
        private RID sumrid;
        private Inserter i1;
        private Inserter i2;
        private Session session;
        private Table det;
        private Table sum;
        private Row detrow;
        private Row sumrow;
        private Cursor tc;
        Selecter(RID sumrid, Inserter i1, Inserter i2) {
            super();
            this.sumrid = sumrid;
            this.i1 = i1;
            this.i2 = i2;
            start();
        }

        public void run() {
            try {
                session = db.createSession();
                det = session.getTable("det");
                sum = session.getTable("sum");
                detrow = det.allocateRow();
                sumrow = sum.allocateRow();
                tc = det.allocateCursor();

                session.commit();

                while (!(i1.done && i2.done)) {
                    checkTotals(session, tc, detrow, sum, sumrow, sumrid);
                }

                checkTotals(session, tc, detrow, sum, sumrow, sumrid);
            } catch (Exception e) {
                failed = true;
            }

            System.out.println("Results:\n" + results);

            done();
        }
    }
}
