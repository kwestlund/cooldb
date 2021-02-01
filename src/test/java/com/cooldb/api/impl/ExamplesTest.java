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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;

public class ExamplesTest {

    @BeforeAll
    static void setUp() {
        CoolDB coolDB = CoolDB.getInstance();
        coolDB.destroyDatabase(new File("build/tmp/cooldb"));
    }

    @AfterAll
    static void tearDown() {
        CoolDB coolDB = CoolDB.getInstance();
        coolDB.destroyDatabase(new File("build/tmp/cooldb"));
    }

    /**
     * This example creates a database at "build/tmp/cooldb" and a table named "cool", then inserts 100,000 rows
     * setting the first column of each row to the row sequence number.
     */
    @Test
    public void testExample1() throws Exception {
        // First you'll need a CoolDB object:
        CoolDB coolDB = CoolDB.getInstance();

        // With coolDB, you can create and start a new database
        Database db = coolDB.getDatabase(new File("build/tmp/cooldb"));
        db.createDatabase();

        // Then create a session to provide you with a transaction context
        Session session = db.createSession();

        // Create a table with 2 columns, one of type integer(I) and one of type small string(s)
        Table table = session.createTable("cool", "Is");

        // Allocate a reusable Row for use in modifying the table
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
     * This example starts the database created in Example 1 at "build/tmp/cooldb", deletes all rows from the table
     * "cool" except for the single row whose first column is equal to 12345, and updates that row's second column
     * with a  new string value. Then it prints the contents of the "cool" table.
     */
    @Test
    public void testExample2() throws Exception {
        // First you'll need a CoolDB object:
        CoolDB coolDB = CoolDB.getInstance();

        // With coolDB, you can create and start a new database
        Database db = coolDB.getDatabase(new File("build/tmp/cooldb"));
        db.startDatabase();

        // Then create a session to provide you with a transaction context
        Session session = db.createSession();

        // Get the table
        Table table = session.getTable("cool");

        // Allocate a TableCursor and a Row for use in modifying the table
        TableCursor cursor = table.allocateCursor();
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
        while (cursor.fetchNext(row)) {
            System.out.println("Column1=" + row.getString(0) + " Column2=" + row.getString(1));
        }

        // Stop the database
        db.stopDatabase();
    }

    /**
     * This example starts the database modified in Example 2 and creates a unique index on the first column of table
     * "cool", inserts 10000 records, then uses the index to retrieve and print the row whose first column = 12345.
     */
    @Test
    public void testExample3() throws Exception {
        // First you'll need a CoolDB object:
        CoolDB coolDB = CoolDB.getInstance();

        // With coolDB, you can create and start a new database
        Database db = coolDB.getDatabase(new File("build/tmp/cooldb"));
        db.startDatabase();

        // Create a session
        Session session = db.createSession();

        // Get the table
        Table table = session.getTable("cool");

        // Create the index
        byte[] keyMap = new byte[]{0}; // the column at pos 0 in the table is indexed
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
        System.out.println("Column1=" + row.getString(0) + " Column2=" + row.getString(1));

        // Commit the transaction
        session.commit();

        // Stop the database
        db.stopDatabase();
    }
}
