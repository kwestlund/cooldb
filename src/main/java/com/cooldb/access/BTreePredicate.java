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

import com.cooldb.api.Key;
import com.cooldb.buffer.DBObject;

import java.nio.ByteBuffer;

public class BTreePredicate implements GiST.Predicate {
    /**
     * Operator types
     */
    public static final int EQ = 1;
    public static final int LT = 2;
    public static final int GT = 3;
    public static final int LTE = 4;
    public static final int GTE = 5;
    int startOp;
    int endOp;
    Key startKey;
    Key endKey;

    public BTreePredicate(Key startKey, Key endKey) {
        startOp = EQ;
        this.startKey = startKey;
        this.endKey = endKey;
    }

    public BTreePredicate(BTreePredicate btp) {
        startOp = btp.startOp;
        endOp = btp.endOp;
        startKey = (Key) btp.startKey.copy();
        endKey = (Key) btp.endKey.copy();
    }

    private static String getOpSymbol(int op) {
        switch (op) {
            case EQ:
                return "=";
            case LT:
                return "<";
            case GT:
                return ">";
            case LTE:
                return "<=";
            case GTE:
                return ">=";
            default:
                return "no-op";
        }
    }

    public void assign(DBObject o) {
        BTreePredicate btp = (BTreePredicate) o;
        startOp = btp.startOp;
        endOp = btp.endOp;
        startKey.assignRow(btp.startKey);
        endKey.assignRow(btp.endKey);
    }

    public int getStartOp() {
        return startOp;
    }

    public void setStartOp(int startOp) {
        this.startOp = startOp;
    }

    public int getEndOp() {
        return endOp;
    }

    public void setEndOp(int endOp) {
        this.endOp = endOp;
    }

    public Key getStartKey() {
        return startKey;
    }

    public void setStartKey(Key startKey) {
        this.startKey.assignRow(startKey);
    }

    public Key getEndKey() {
        return endKey;
    }

    public void setEndKey(Key endKey) {
        this.endKey.assignRow(endKey);
    }

    public void setRange(Key startKey, int startOp, Key endKey, int endOp) {
        this.startKey = startKey;
        this.startOp = startOp;
        this.endKey = endKey;
        this.endOp = endOp;
    }

    // DBObject implementation
    public DBObject copy() {
        return new BTreePredicate(this);
    }

    public void writeTo(ByteBuffer bb) {
        startKey.writeTo(bb);
    }

    public void readFrom(ByteBuffer bb) {
        startKey.readFrom(bb);
    }

    public int storeSize() {
        return startKey.storeSize();
    }

    public int compareTo(Object obj) {
        BTreePredicate o = (BTreePredicate) obj;
        return startKey.compareTo(o.startKey);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof BTreePredicate && compareTo(obj) == 0;
    }

    @Override
    public int hashCode() {
        return startKey.hashCode();
    }

    public String toString() {
        String s = getOpSymbol(startOp) + startKey;
        if (endOp != 0) {
            s = s + " AND " + getOpSymbol(endOp) + endKey;
        }
        return s;
    }
}
