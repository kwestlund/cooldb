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

/**
 * DatabaseWriter asynchronously writes modified page buffers to the file system
 * whenever the number of dirty page buffers exceeds a threshold. DatabaseWriter
 * sorts pages by their physical location in the file system so that writes are
 * physically clustered for efficiency.
 */

public class DatabaseWriter implements Runnable {

    private final boolean[] checkPointInProgress = {false};
    private BufferPool bufferPool;
    private Thread thread;
    private boolean shutdown;

    public DatabaseWriter(BufferPool bufferPool) {
        this.bufferPool = bufferPool;
    }

    /**
     * Start the DatabaseWriter thread.
     */
    public void start() {
        thread = new Thread(this, "Cooldb - DatabaseWriter");
        thread.start();
    }

    /**
     * Stop the DatabaseWriter thread.
     */
    public synchronized void stop() {
        shutdown = true;
        synchronized (checkPointInProgress) {
            checkPointInProgress.notify();
        }

        try {
            while (thread != null)
                wait();
        } catch (Exception ignored) {
        }

        thread = null;
        bufferPool = null;
    }

    /**
     * Signal the DatabaseWriter thread to take a checkpoint of the BufferPool.
     */
    public void takeCheckpoint() {
        synchronized (checkPointInProgress) {
            if (!checkPointInProgress[0]) {
                checkPointInProgress[0] = true;
                checkPointInProgress.notify();
            }
        }
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
            } finally {
                synchronized (checkPointInProgress) {
                    checkPointInProgress[0] = false;
                }
            }
        }

        // help gc
        bufferPool = null;

        synchronized (this) {
            thread = null;
            notifyAll();
        }
    }

    public synchronized void syncCheckPoint() {
        bufferPool.checkPoint();
    }
}
