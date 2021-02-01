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

package com.cooldb.storage;

import com.cooldb.buffer.FilePage;
import com.cooldb.api.DatabaseException;
import com.cooldb.buffer.BufferPool;
import com.cooldb.buffer.DBObject;
import com.cooldb.core.Core;
import com.cooldb.segment.Segment;
import com.cooldb.transaction.Transaction;

/**
 * TempDataset extends Dataset and overrides the insert method with one
 * optimized for single-transaction bulk-inserts into a temporary table.
 * TempDataset avoids writing redo/undo logs for each insert and it keeps each
 * new page pinned until filled to minimize page-pinning overhead.
 * <p>
 * TempDataset requires the beginInsert and endInsert methods to bracket
 * bulk-inserts.
 */

public class TempDataset extends Dataset {
    public TempDataset(Segment segment, Core core) throws DatabaseException {
        super(segment, core);
        descriptor.setLoadMin((byte) 0);
        descriptor.setLoadMax((byte) 100);
    }

    public void beginInsert(Transaction trans) throws DatabaseException {
        allocateNextPage(trans);
    }

    @Override
    public Rowid insert(Transaction trans, DBObject obj) throws DatabaseException {
        // Calculate tuple size
        short tsize = (short) (obj.storeSize() + RowHeader.getOverhead());

        if (!datasetPage.canHold(tsize, descriptor.getLoadMax())) {
            allocateNextPage(trans);
        }

        // Assign the to-be-inserted object a row identifier
        Rowid rowid = new Rowid(datasetPage.getPage(), datasetPage.prepareSlot(
                trans, tsize));

        // Write the object into the prepared slot without logging
        datasetPage.insert(trans, obj, rowid, tsize, false);

        // Alert attachments
        for (Attachment attachment : attachments) attachment.didInsert(trans, obj, rowid);

        return rowid;
    }

    public void endInsert(Transaction trans) throws DatabaseException {
        datasetPage.unPin(BufferPool.Affinity.HATED);
    }

    // SpaceDelegate implementation
    @Override
    public void didAllocatePage(Transaction trans, FilePage page)
            throws DatabaseException {
        // Set the previous page's nextPage pointer to the new page and unPin
        // the previous page
        if (datasetPage == null) {
            datasetPage = new DatasetPage(core.getTransactionManager(),
                                          allocPageBroker());
        } else {
            datasetPage.setNextPage(page);
            datasetPage.unPin(BufferPool.Affinity.HATED);
        }

        // initialize the new page pinned for temporary writing without logging
        datasetPage.writePin(page, true);
        datasetPage.create(FilePage.NULL, descriptor.getLastPage(),
                           FilePage.NULL);

        // update the dataset descriptor in-memory only
        descriptor.setFreePage(page);
        descriptor.setLastPage(page);
    }
}
