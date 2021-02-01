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

package com.cooldb.access;

import com.cooldb.api.DatabaseException;
import com.cooldb.api.Filter;
import com.cooldb.api.Key;
import com.cooldb.buffer.FilePage;
import com.cooldb.segment.PageBroker;
import com.cooldb.transaction.Transaction;
import com.cooldb.transaction.TransactionManager;

public class BTreeNode extends AbstractNode {
    private Entry leftLeafEntry;
    private Entry rightLeafEntry;
    private Entry leftBranchEntry;
    private Entry rightBranchEntry;

    public BTreeNode(AbstractTree tree, TransactionManager transactionManager,
                     PageBroker pageBroker) {
        super(tree, transactionManager, pageBroker);
    }

    public int compare(Entry e1, Entry e2) {
        BTreePredicate p1 = (BTreePredicate) e1.getPredicate();
        BTreePredicate p2 = (BTreePredicate) e2.getPredicate();

        return p1.getStartKey().compareTo(p2.getStartKey());
    }

    public boolean consistent(Entry entry, Predicate query) {
        BTreePredicate p = (BTreePredicate) entry.getPredicate();
        BTreePredicate q = (BTreePredicate) query;
        Key pk = p.getStartKey();

        return testKey(q.getStartOp(), pk, q.getStartKey())
                && testKey(q.getEndOp(), pk, q.getEndKey());
    }

    public boolean passes(Entry entry, Filter filter) {
        if (filter != null) {
            BTreePredicate p = (BTreePredicate) entry.getPredicate();
            return filter.passes(p.getStartKey());
        }
        return true;
    }

    public Entry pickSplit(Node newNode, int level) throws DatabaseException {
        BTreeNode rightNode = (BTreeNode) newNode;
        Entry entry = newEntry(level + 1);
        int count = getCount();
        int split = count / 2;

        Entry re = getRightEntry(level);
        Entry le = getLeftEntry(level);
        Key rsep = ((BTreePredicate) re.getPredicate()).getStartKey();
        Key lsep = ((BTreePredicate) le.getPredicate()).getStartKey();

        // read rsep value
        get(split, re);

        if (level == 0) {
            /*
             * TODO: generate a prefix separator between this and the new node
             * and apply rear-compression to retain only the necessary prefix
             * get(split - 1, le);
             * rsep.compressRear(lsep.separator(rsep));
             */
        }

        // set the chosen/generated separator in the return entry
        BTreePredicate bp = (BTreePredicate) entry.getPredicate();
        bp.setStartKey(rsep);

        // move all entries to the right of the split point and
        // including the split point from this node to the rightNode
        moveRange(rightNode, split, count);

        // record the new node's bounding predicate in the new node itself
        rightNode.insertBoundingPredicate(bp);

        // return the separator and downlink
        entry.setPointer(rightNode.getPage());

        return entry;
    }

    @Override
    Predicate allocatePredicate() throws DatabaseException {
        return ((BTree) tree).allocatePredicate();
    }

    @Override
    void initRoot(Transaction trans, FilePage oldRoot, int oldLevel)
            throws DatabaseException {
        // insert bounding predicate
        Entry e = getRightEntry(oldLevel + 1);
        BTreePredicate bp = (BTreePredicate) e.getPredicate();
        bp.getStartKey().setMinValue();

        insertBoundingPredicate(bp);

        // insert entry with min key and initial downlink pointing to the old
        // root page
        if (!oldRoot.isNull()) {
            e.setPointer(oldRoot);
            pageInsert(trans, e, oldLevel + 1, e.storeSize());
        }
    }

    private boolean testKey(int op, Key pk, Key qk) {
        switch (op) {
            case BTreePredicate.EQ:
                return pk.compareTo(qk) == 0;
            case BTreePredicate.LT:
                return pk.compareTo(qk) < 0;
            case BTreePredicate.GT:
                return pk.compareTo(qk) > 0;
            case BTreePredicate.LTE:
                return pk.compareTo(qk) <= 0;
            case BTreePredicate.GTE:
                return pk.compareTo(qk) >= 0;
            default:
                return true;
        }
    }

    private Entry getRightEntry(int level) throws DatabaseException {
        if (level == 0) {
            if (rightLeafEntry == null)
                rightLeafEntry = newEntry(level);
            return rightLeafEntry;
        } else {
            if (rightBranchEntry == null)
                rightBranchEntry = newEntry(level);
            return rightBranchEntry;
        }
    }

    private Entry getLeftEntry(int level) throws DatabaseException {
        if (level == 0) {
            if (leftLeafEntry == null)
                leftLeafEntry = newEntry(level);
            return leftLeafEntry;
        } else {
            if (leftBranchEntry == null)
                leftBranchEntry = newEntry(level);
            return leftBranchEntry;
        }
    }
}
