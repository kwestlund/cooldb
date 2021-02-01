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

package com.cooldb.api;

import com.cooldb.api.impl.DatabaseImpl;

import java.io.File;
import java.util.HashMap;

/**
 * CoolDB provides a thread-safe entry point to one or more cooldb databases. There can be only one
 * instance of CoolDB, so it is a singleton class.  Use {@link #getInstance()} to get the
 * single instance.
 * <p>
 * Call the method {@link #getDatabase} with the file system
 * path of the desired database to obtain a specific Database instance.
 */
public class CoolDB {
    private static final CoolDB instance = new CoolDB();
    final HashMap<File, Database> dbs = new HashMap<>();

    private CoolDB() {}

    /**
     * Gets the singleton instance of CoolDB
     *
     * @return the singleton instance of CoolDB
     */
    public static CoolDB getInstance() {
        return instance;
    }

    /**
     * Gets the Database instance associated with the specified
     * file system path.  If no such path exists, one will be created.
     *
     * @param path Specifies the directory containing the database files.
     * @return the Database instance associated with the given path
     */
    public synchronized Database getDatabase(File path) {
        if (dbs.containsKey(path))
            return dbs.get(path);
        Database database = new DatabaseImpl(path);
        dbs.put(path, database);
        return database;
    }

    /**
     * Destroys the Database instance associated with the specified
     * file system path.
     *
     * @param path Specifies the directory containing the database files.
     */
    public synchronized void destroyDatabase(File path) {
        DatabaseImpl database = (DatabaseImpl) getDatabase(path);
        database.destroyDatabase();
        dbs.remove(database.getPath());
    }
}
