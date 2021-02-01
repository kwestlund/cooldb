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

package com.cooldb.recovery;

import com.cooldb.log.*;
import com.cooldb.transaction.TransactionPool;
import com.cooldb.transaction.TransactionState;
import com.cooldb.buffer.BufferNotFound;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.DPEntry;
import com.cooldb.log.*;

import java.nio.ByteBuffer;

/**
 * CheckpointWriter asynchronously takes a checkpoint of the database in order
 * to reclaim log space and shorten recovery restart time. The checkpoint is
 * triggered when the amount of data written to the log that would need to be
 * redone during restart recovery exceeds the checkPointInterval parameter.
 * <p>
 * CheckpointWriter causes the BufferPool to write to stable storage as many
 * dirty pages as it can without blocking on any active transactions. Any
 * remaining dirty pages are noted in the log checkpoint record and their
 * collective minimum recovery log-sequence-number together with the
 * BeginCheckpoint log-sequence-number determine the starting point for redo
 * recovery and also the point before which all other log records can be erased
 * safely from the redo log.
 * <p>
 * CheckpointWriter also notes all uncommitted transactions in the log
 * checkpoint record and their collective maximum undo-next-log-sequence-number
 * may be used to help determine the starting point for undo recovery. Also,
 * their collective minimum commit log-sequence-number is the point prior to
 * which records can be erased safely from the undo log in order to reclaim undo
 * log space.
 * <p>
 * Finally, CheckpointWriter forces the EndCheckpoint log record to stable
 * storage and records the address of the BeginCheckpoint log record as well as
 * the next transaction identifier in the SystemKey, and forces it to stable
 * storage. The checkpoint is not complete until the information in the
 * SystemKey is forced out.
 */

public class CheckpointWriter implements Runnable {

    /**
     * Minimum redo log space requirement needed for the BeginCheckpoint record
     */
    public static final long REDO_MINIMUM = 1024;
    private final long checkPointInterval;
    private SystemKey systemKey;
    private LogManager logManager;
    private BufferPool bufferPool;
    private TransactionPool transactionPool;
    private Thread thread;
    private boolean shutdown;
    private final boolean[] checkPointInProgress = {false};

    public CheckpointWriter(long checkPointInterval) {
        this.checkPointInterval = checkPointInterval;
    }

    /**
     * Initialize the CheckpointWriter without starting it.
     */
    public void init(SystemKey systemKey, LogManager logManager,
                     BufferPool bufferPool, TransactionPool transactionPool) {
        this.systemKey = systemKey;
        this.logManager = logManager;
        this.bufferPool = bufferPool;
        this.transactionPool = transactionPool;
    }

    /**
     * Start the CheckpointWriter thread.
     */
    public void start() {
        if (thread == null) {
            thread = new Thread(this, "Cooldb - CheckpointWriter");
            thread.start();
        }
    }

    /**
     * Stop the CheckpointWriter thread.
     */
    public synchronized void stop() {
        if (thread != null) {
            shutdown = true;
            synchronized (checkPointInProgress) {
                checkPointInProgress.notify();
            }

            try {
                while (thread != null)
                    wait();
            } catch (Exception ignored) {
            }
        }
    }

    /**
     * Signal the CheckpointWriter to take a checkpoint asynchronously if
     * certain conditions are met. The checkpoint will be taken if the total
     * size of all redo log records written since the last determined minimum
     * recovery LSN point or since the last checkpoint was taken is greater than
     * the checkPointInterval.
     * <p>
     * The CheckpointWriter must have been started in order for this to have any
     * effect.
     */
    public void checkPoint() throws LogExhaustedException {
        // Asynchronous checkpoint to reclaim log space
        if (logManager.getEndOfLog() - logManager.getStartOfLog() > checkPointInterval)
            asyncCheckPoint();

        // Asynchronous checkpoint to minimize recovery time
        if (logManager.getEndOfLog() - systemKey.getMaster() > checkPointInterval)
            asyncCheckPoint();
    }

    // Runnable implementation
    public void run() {
        while (!shutdown) {
            try {
                synchronized (checkPointInProgress) {
                    while (!(shutdown || checkPointInProgress[0]))
                        checkPointInProgress.wait();
                }

                if (!shutdown)
                    syncCheckPoint();
            } catch (InterruptedException ie) {
                break;
            } catch (LogExhaustedException lee) {
                // TODO: handle LogExhaustedException
            } catch (Exception e) {
                // TODO: handle other Exceptions
            } finally {
                synchronized (checkPointInProgress) {
                    checkPointInProgress[0] = false;
                }
            }
        }

        // help gc
        systemKey = null;
        logManager = null;
        bufferPool = null;
        transactionPool = null;
        synchronized (this) {
            thread = null;
            notifyAll();
        }
    }

    /**
     * syncCheckpoint takes a checkpoint of the database in order to reclaim log
     * space and shorten recovery restart time.
     * <p>
     * syncCheckpoint writes a BeginCheckpoint log record to the redo log then
     * tells the BufferPool to write to stable storage as many dirty pages as it
     * can without blocking on any active transactions. Following the BufferPool
     * checkpoint, any remaining dirty pages are noted in the EndCheckpoint log
     * record and their collective minimum recovery log-sequence-number (recLSN)
     * is used to garbage-collect the redo log as all log records prior to the
     * minimum recLSN are no longer needed.
     * <p>
     * syncCheckpoint also notes in the EndCheckpoint log record the states of
     * all transactions in the TransactionPool and their collective minimum
     * commit LSN is used to garbage-collect the undo log as all log records
     * prior to the minimum commit LSN are no longer needed.
     * <p>
     * Finally, syncCheckpoint forces the EndCheckpoint log record to stable
     * storage and records the address of the BeginCheckpoint log record as well
     * as the next transaction identifier in the SystemKey, and forces it to
     * stable storage. The checkpoint is not complete until the information in
     * the SystemKey is forced out.
     */
    public synchronized void syncCheckPoint() throws LogExhaustedException,
            BufferNotFound, InterruptedException {
        // Write begin checkpoint log record
        RedoLog log = new BeginCheckpointLog();
        logManager.writeRedo(log);
        systemKey.setMaster(log.getAddress());

        // Checkpoint the buffer pool, calculate the new minimum recovery LSN,
        // and inform the redo log writer to garbage-collect all log records
        // written prior to the new minimum recovery LSN.

        long minRecLSN = log.getAddress();
        DPEntry[] dirtyPages = bufferPool.checkPoint();
        for (DPEntry dirtyPage : dirtyPages) minRecLSN = Math.min(minRecLSN, dirtyPage.getRecLSN());

        // Flush to the minRecLSN
        logManager.moveFirewallTo(minRecLSN);

        // Construct end checkpoint log record with list of dirty pages
        log = new EndCheckpointLog();
        addToCheckpoint(log, dirtyPages);

        // Get the state of all currently active transactions
        TransactionState[] activeTransactions = transactionPool
                .getActiveTransactions();

        // Add active transactions to the EndCheckpoint log
        addToCheckpoint(log, activeTransactions);

        // Write the end checkpoint record to the redo log
        logManager.writeRedo(log);

        // Flush both logs to stable storage
        logManager.flushTo(log.getAddress());
        // logManager.force();

        // Save begin checkpoint LSN in 'master' record
        systemKey.setNextTransId(transactionPool.getNextTransId());
        systemKey.setCommitLSN(transactionPool.calcCommitLSN());
        systemKey.flush();

        // Garbage collect undo log space
        logManager.setMinUndo(transactionPool.calcMinCommitLSN());
    }

    private void asyncCheckPoint() {
        synchronized (checkPointInProgress) {
            if (!checkPointInProgress[0]) {
                checkPointInProgress[0] = true;
                checkPointInProgress.notify();
            }
        }
    }

    private void addToCheckpoint(Log log, DBObject[] obj) {
        int size = 0;
        for (DBObject dbObject : obj) size += dbObject.storeSize();
        ByteBuffer bb = log.allocate((byte) 0, size).getData();
        for (DBObject dbObject : obj) dbObject.writeTo(bb);
    }
}
