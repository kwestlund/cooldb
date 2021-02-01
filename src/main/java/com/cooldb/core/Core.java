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

package com.cooldb.core;

import com.cooldb.access.TreeManager;
import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.*;
import com.cooldb.log.*;
import com.cooldb.recovery.*;
import com.cooldb.segment.Extent;
import com.cooldb.segment.Segment;
import com.cooldb.segment.SegmentFactory;
import com.cooldb.segment.SpaceManager;
import com.cooldb.storage.Dataset;
import com.cooldb.storage.DatasetManager;
import com.cooldb.transaction.*;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.RandomAccessFile;

/**
 * Core is used to reference a specific database identified by its File system
 * path.
 * <p>
 * Core provides methods to create, start, stop, and to subsequently make use of
 * core database services encapsulated by the Core instance and dedicated to the
 * specific database.
 */
public class Core implements RecoveryContext {

    private static final int KEY_FILE = 0;
    private static final int SYS_FILE = 1;
    private static final int REDO_FILE = 2;
    private static final int UNDO_FILE = 3;
    private static final int DEFAULT_BUFFER_POOL_SIZE = 128;
    // Core space-management pages
    private static final FilePage FREE_EXTENTS_ID = new FilePage((short) 0, 0);
    private static final FilePage USED_EXTENTS_ID = new FilePage((short) 0, 1);
    private static final FilePage SEGMENT_MANAGER_ID = new FilePage((short) 0,
                                                                    2);
    private static final int NEXT_FREE_PAGE_ID = 3;
    // Pre-allocated extents
    private static final Extent DATASET_MANAGER_EXTENT = new Extent(
            new FilePage((short) 0, NEXT_FREE_PAGE_ID), 1);
    private static final Extent TABLE_MANAGER_EXTENT = new Extent(new FilePage(
            (short) 0, NEXT_FREE_PAGE_ID + 1), 1);
    private static final Extent TREE_MANAGER_EXTENT = new Extent(new FilePage(
            (short) 0, NEXT_FREE_PAGE_ID + 2), 1);
    private static final Extent SEQUENCE_MANAGER_EXTENT = new Extent(
            new FilePage((short) 0, NEXT_FREE_PAGE_ID + 3), 1);
    private static final Segment SEGMENT_MANAGER_SEGMENT = new Segment(
            SEGMENT_MANAGER_ID);
    // Location of the database
    private final File path;
    // Core files
    private final File[] files = new File[4];
    private final int[] size = new int[4];
    private final DBFile[] dbf = new DBFile[4];
    // Core services
    private FileManager fileManager;
    private BufferPool bufferPool;
    private RedoLogWriter redoLogWriter;
    private UndoLogWriter undoLogWriter;
    private SystemKey systemKey;

    // RecoveryContext implementation
    private CheckpointWriter checkpointWriter;
    private LogManager logManager;
    private TransactionLogger transactionLogger;
    private TransactionPool transactionPool;
    private TransactionManager transactionManager;
    private SegmentFactory segmentFactory;
    private SpaceManager spaceManager;
    private DatasetManager datasetManager;
    private TableManager tableManager;
    private TreeManager treeManager;
    private SequenceManager sequenceManager;
    private SortManager sortManager;
    private int bufferPoolSize = DEFAULT_BUFFER_POOL_SIZE;

    public Core(File path) {
        this.path = path;

        // create the file pointers
        files[REDO_FILE] = new File(path, "redo.log");
        files[UNDO_FILE] = new File(path, "undo.log");
        files[SYS_FILE] = new File(path, "sys.db");
        files[KEY_FILE] = new File(path, "sys.key");

        // set default file sizes
        size[REDO_FILE] = DEFAULT_BUFFER_POOL_SIZE;
        size[UNDO_FILE] = DEFAULT_BUFFER_POOL_SIZE;
        size[SYS_FILE] = DEFAULT_BUFFER_POOL_SIZE;
        size[KEY_FILE] = 1;
    }

    /**
     * Return true if the database exists.
     */
    public synchronized boolean databaseExists() {
        return files[KEY_FILE].exists();
    }

    /**
     * Return true if the database is in use by another process.
     */
    public synchronized boolean databaseInUse() {
        try {
            if (fileManager == null) {
                if (databaseExists()) {
                    openFiles();
                    closeFiles();
                }
            }
        } catch (DatabaseException ce) {
            return true;
        } catch (FileNotFoundException ignored) {
        }

        return false;
    }

    /**
     * Sets the number of heap memory pages to allocate to the buffer pool.
     */
    public synchronized void setBufferPoolSize(int pages) {
        bufferPoolSize = pages;
        if (bufferPool != null)
            bufferPool.ensureCapacity(pages);
    }

    /**
     * Create and open a new database in the file system directory specified
     * when the Core object was created.
     */
    public synchronized void createDatabase() throws DatabaseException {
        createDatabase(false);
    }

    /**
     * Create and open a new database with an option to overwrite any existing
     * database.
     */
    public synchronized void createDatabase(int pages) throws DatabaseException {
        createDatabase(false, pages);
    }

    /**
     * Create and open a new database with an option to overwrite any existing
     * database.
     */
    public synchronized void createDatabase(boolean overwrite)
            throws DatabaseException {
        createDatabase(overwrite, 0);
    }

    /**
     * Create and open a new database with an option to overwrite any existing
     * database. The system file will have the initial number of pages
     * specified.
     */
    public synchronized void createDatabase(boolean overwrite, int pages)
            throws DatabaseException {
        try {
            if (databaseExists()) {
                if (!overwrite)
                    throw new DatabaseException(
                            "Create database failed because it exists already: "
                                    + path);
            } else if (!path.exists()) {
                //noinspection ResultOfMethodCallIgnored
                path.mkdirs();
            }

            // make sure the instance is not already running
            stopDatabase();

            if (pages > DEFAULT_BUFFER_POOL_SIZE)
                size[SYS_FILE] = pages;

            // create the core files
            createFiles();
            closeFiles();

            // open the database, starting core services
            openDatabase();

            systemKey.create(size[SYS_FILE]);
            undoLogWriter.create();

            Transaction trans = transactionManager.beginTransaction();

            // core space management services
            segmentFactory.create(trans, SEGMENT_MANAGER_SEGMENT);

            // System extent, located after catalog pages
            Extent sysExtent = new Extent((short) 0, NEXT_FREE_PAGE_ID,
                                          dbf[SYS_FILE].size() - NEXT_FREE_PAGE_ID);

            spaceManager.create(trans, dbf[SYS_FILE], FREE_EXTENTS_ID,
                                USED_EXTENTS_ID, sysExtent);

            // table meta-data
            Segment segment;

            segment = spaceManager.createSegment(trans, DATASET_MANAGER_EXTENT);
            datasetManager = (DatasetManager) segmentFactory
                    .createSegmentMethod(trans, segment, DatasetManager.class);

            segment = spaceManager.createSegment(trans, TABLE_MANAGER_EXTENT);
            tableManager = new TableManager((Dataset) segmentFactory
                    .createSegmentMethod(trans, segment, Dataset.class),
                                            transactionManager, segmentFactory, spaceManager);

            segment = spaceManager.createSegment(trans, TREE_MANAGER_EXTENT);
            treeManager = (TreeManager) segmentFactory.createSegmentMethod(
                    trans, segment, TreeManager.class);

            segment = spaceManager
                    .createSegment(trans, SEQUENCE_MANAGER_EXTENT);
            sequenceManager = new SequenceManager((Dataset) segmentFactory
                    .createSegmentMethod(trans, segment, Dataset.class),
                                                  transactionManager, segmentFactory, spaceManager);

            transactionManager.commitTransaction(trans);

            // take an initial checkpoint
            checkpointWriter.syncCheckPoint();

            startDatabase();
        } catch (Exception e) {
            stopDatabase();

            throw new DatabaseException("Failed to create database: " + path, e);
        }
    }

    /**
     * Start the core services and perform restart recovery.
     */
    public synchronized void startDatabase() throws DatabaseException {
        try {
            if (!databaseExists())
                throw new DatabaseException("Database does not exist: " + path);

            // Make sure the database is opened
            openDatabase();

            // Perform restart recovery
            RecoveryManager recoveryManager = new RecoveryManager(systemKey,
                                                                  logManager, transactionPool, transactionLogger,
                                                                  bufferPool,
                                                                  checkpointWriter, this);
            recoveryManager.recover();

            // Start the checkpoint writer
            checkpointWriter.start();

            Transaction trans = transactionManager.beginTransaction();

            // Remove leftover temporary segments
            sortManager.recover(trans);

            transactionManager.commitTransaction(trans);
        } catch (Exception e) {
            throw new DatabaseException("Failed to start database: " + path, e);
        }
    }

    /**
     * Performs a soft shutdown of the database by waiting for upto
     * <code>timeout</code> milliseconds for ongoing transactions to finish and
     * then by taking a final checkpoint prior to stopping core services.
     *
     * @param timeout the maximum time to wait for a soft shutdown
     * @return true if successful, false if soft shutdown fails
     */
    public boolean stopDatabase(long timeout) throws DatabaseException {
        try {
            if (transactionManager != null
                    && !transactionManager.quiesce(timeout))
                return false;

            // take a final checkpoint
            synchronized (this) {
                if (checkpointWriter != null) {
                    checkpointWriter.syncCheckPoint();
                    checkpointWriter.stop();
                    checkpointWriter = null;
                }
            }
        } catch (Exception e) {
            throw new DatabaseException(e);
        }

        stopDatabase();

        return true;
    }

    /**
     * Performs a hard stop of all core database services without first waiting
     * for ongoing transactions to finish and without taking a final checkpoint.
     */
    public synchronized void stopDatabase() {
        if (sortManager != null)
            sortManager.stop();
        if (spaceManager != null)
            spaceManager.stop();
        if (checkpointWriter != null)
            checkpointWriter.stop();
        if (segmentFactory != null)
            segmentFactory.stop();
        if (bufferPool != null)
            bufferPool.stop();
        if (undoLogWriter != null)
            undoLogWriter.stop();
        if (redoLogWriter != null)
            redoLogWriter.stop();
        if (systemKey != null)
            systemKey.stop();
        if (fileManager != null)
            fileManager.clear();

        closeFiles();

        fileManager = null;
        bufferPool = null;
        redoLogWriter = null;
        undoLogWriter = null;
        systemKey = null;
        checkpointWriter = null;
        logManager = null;
        transactionLogger = null;
        transactionPool = null;
        transactionManager = null;
        segmentFactory = null;
        spaceManager = null;
        datasetManager = null;
        tableManager = null;
        treeManager = null;
        sequenceManager = null;
        sortManager = null;
    }

    /**
     * Destroys the database, shutting down all core services and removing the
     * core files from the file system.
     */
    public synchronized void destroyDatabase() {
        stopDatabase();

        // delete the files
        deleteFiles();
    }

    /**
     * Returns the database page size in bytes.
     */
    public synchronized int getPageSize() {
        return FileManager.DEFAULT_PAGE_SIZE;
    }

    /**
     * Returns the core TransactionManager service.
     */
    public synchronized TransactionManager getTransactionManager() {
        return transactionManager;
    }

    /**
     * Returns the core transaction DeadlockDetector.
     */
    public DeadlockDetector getDeadlockDetector() {
        return getTransactionManager().getDeadlockDetector();
    }

    /**
     * Returns the core SegmentFactory service.
     */
    public synchronized SegmentFactory getSegmentFactory() {
        return segmentFactory;
    }

    /**
     * Returns the core SpaceManager service.
     */
    public synchronized SpaceManager getSpaceManager() {
        return spaceManager;
    }

    /**
     * Returns the core DatasetManager service.
     */
    public synchronized DatasetManager getDatasetManager() {
        return datasetManager;
    }

    public synchronized TableManager getTableManager() {
        return tableManager;
    }

    public synchronized TreeManager getTreeManager() {
        return treeManager;
    }

    public synchronized SequenceManager getSequenceManager() {
        return sequenceManager;
    }

    public synchronized SortManager getSortManager() {
        return sortManager;
    }

    /**
     * Initialize the core services.
     */
    void openDatabase() throws DatabaseException {
        if (fileManager != null)
            return;

        try {
            // Open the core files
            openFiles();

            // Create the file manager
            fileManager = new FileManagerImpl();

            // Add the sys db file to the FileManager
            fileManager.addFile((short) 0, dbf[SYS_FILE]);

            // Create the system key
            systemKey = new SystemKey(dbf[KEY_FILE]);

            // Create the log writers and their manager
            redoLogWriter = new RedoLogWriter(dbf[REDO_FILE]);
            undoLogWriter = new UndoLogWriter(dbf[UNDO_FILE]);
            undoLogWriter.start();
            logManager = new LogManager(redoLogWriter, undoLogWriter);

            // Create the transaction pool
            transactionPool = new TransactionPool(logManager, 1);

            // Create the buffer pool
            bufferPool = new BufferPoolImpl(fileManager, logManager,
                                            transactionPool);
            bufferPool.ensureCapacity(bufferPoolSize);
            bufferPool.start();

            // Create the checkpoint writer
            checkpointWriter = new CheckpointWriter(
                    dbf[REDO_FILE].capacity() / 4);
            checkpointWriter.init(systemKey, logManager, bufferPool,
                                  transactionPool);

            // Create the transaction logger and manager
            transactionLogger = new TransactionLogger(logManager,
                                                      checkpointWriter);
            transactionManager = new TransactionManager(transactionPool,
                                                        transactionLogger, bufferPool);

            // Create the segment factory
            segmentFactory = new SegmentFactory(systemKey, this);
            segmentFactory.start(SEGMENT_MANAGER_SEGMENT);

            transactionManager.setVersionController(new MVCC(logManager,
                                                             segmentFactory));

            // Create the space, dataset, table, tree, and sequence managers
            spaceManager = new SpaceManager(segmentFactory, transactionManager,
                                            systemKey);
            spaceManager.start(dbf[SYS_FILE], FREE_EXTENTS_ID, USED_EXTENTS_ID);

            datasetManager = (DatasetManager) segmentFactory
                    .getSegmentMethod(DATASET_MANAGER_EXTENT);

            tableManager = new TableManager((Dataset) segmentFactory
                    .getSegmentMethod(TABLE_MANAGER_EXTENT),
                                            transactionManager, segmentFactory, spaceManager);

            treeManager = (TreeManager) segmentFactory
                    .getSegmentMethod(TREE_MANAGER_EXTENT);

            sequenceManager = new SequenceManager((Dataset) segmentFactory
                    .getSegmentMethod(SEQUENCE_MANAGER_EXTENT),
                                                  transactionManager, segmentFactory, spaceManager);

            sortManager = new SortManager(this);
            sortManager.start();
        } catch (Exception e) {
            throw new DatabaseException("Failed to open database: " + path, e);
        }
    }

    public void redo(RedoLog log) throws RedoException {
        segmentFactory.redo(log);
    }

    public void undo(UndoLog log, Transaction trans) throws RollbackException {
        segmentFactory.undo(log, trans);
    }

    public void undo(UndoLog log, PageBuffer pb) throws RollbackException {
        segmentFactory.undo(log, pb);
    }

    public void didRedoPass() throws RedoException {
        try {
            // reload all SegmentMethod instances to ensure that they
            // incorporate
            // the page-oriented updates made during the recovery redo pass
            segmentFactory.stop();
            segmentFactory.start(SEGMENT_MANAGER_SEGMENT);

            // also re-initialize all services that depend on the segmentFactory
            spaceManager.stop();

            spaceManager.start(dbf[SYS_FILE], FREE_EXTENTS_ID, USED_EXTENTS_ID);

            datasetManager = (DatasetManager) segmentFactory
                    .getSegmentMethod(DATASET_MANAGER_EXTENT);

            tableManager = new TableManager((Dataset) segmentFactory
                    .getSegmentMethod(TABLE_MANAGER_EXTENT),
                                            transactionManager, segmentFactory, spaceManager);

            treeManager = (TreeManager) segmentFactory
                    .getSegmentMethod(TREE_MANAGER_EXTENT);

            sequenceManager = new SequenceManager((Dataset) segmentFactory
                    .getSegmentMethod(SEQUENCE_MANAGER_EXTENT),
                                                  transactionManager, segmentFactory, spaceManager);
        } catch (Exception e) {
            throw new RedoException(
                    "Failed during restart recovery at end of redo pass.");
        }
    }

    private void createFiles() throws FileNotFoundException, DatabaseException {
        dbf[REDO_FILE] = createFile(files[REDO_FILE], size[REDO_FILE]);
        dbf[UNDO_FILE] = createFile(files[UNDO_FILE], size[UNDO_FILE]);
        dbf[SYS_FILE] = createFile(files[SYS_FILE], size[SYS_FILE]);
        dbf[KEY_FILE] = createFile(files[KEY_FILE], size[KEY_FILE]);
    }

    private void openFiles() throws FileNotFoundException, DatabaseException {
        dbf[REDO_FILE] = openFile(files[REDO_FILE]);
        dbf[UNDO_FILE] = openFile(files[UNDO_FILE]);
        dbf[SYS_FILE] = openFile(files[SYS_FILE]);
        dbf[KEY_FILE] = openFile(files[KEY_FILE]);
    }

    private void closeFiles() {
        if (dbf[REDO_FILE] != null) {
            dbf[REDO_FILE].close();
        }
        if (dbf[UNDO_FILE] != null) {
            dbf[UNDO_FILE].close();
        }
        if (dbf[SYS_FILE] != null) {
            dbf[SYS_FILE].close();
        }
        if (dbf[KEY_FILE] != null) {
            dbf[KEY_FILE].close();
        }

        dbf[REDO_FILE] = null;
        dbf[UNDO_FILE] = null;
        dbf[SYS_FILE] = null;
        dbf[KEY_FILE] = null;
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private void deleteFiles() {
        if (files[REDO_FILE] != null) {
            files[REDO_FILE].delete();
        }
        if (files[UNDO_FILE] != null) {
            files[UNDO_FILE].delete();
        }
        if (files[SYS_FILE] != null) {
            files[SYS_FILE].delete();
        }
        if (files[KEY_FILE] != null) {
            files[KEY_FILE].delete();
        }
    }

    private DBFile createFile(File file, int pages)
            throws FileNotFoundException, DatabaseException {
        DBFile dbf = openFile(file);

        dbf.truncate(0);
        dbf.extend(pages);

        return dbf;
    }

    private DBFile openFile(File file) throws FileNotFoundException,
            DatabaseException {
        DBFile dbf = new DBFile(new RandomAccessFile(file, "rw"));

        if (!dbf.tryLock())
            throw new DatabaseException("Database in use.");

        return dbf;
    }
}
