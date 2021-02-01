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

import com.cooldb.api.TypeException;
import com.cooldb.buffer.DBObject;
import com.cooldb.buffer.FilePage;
import com.cooldb.segment.SegmentDescriptor;

import java.nio.ByteBuffer;

public class TreeDescriptor extends SegmentDescriptor {

    private static final int MAX_KEYCOLS = 16; // Maximum number of key columns
    private final FilePage rootPage; // Root page
    private final Fixchar keyTypes; // Key column types
    private byte height; // Height of tree
    private boolean isUnique; // Is the key-mapping enough to uniquely

    public TreeDescriptor() {
        super();
        rootPage = new FilePage();
        keyTypes = new Fixchar(MAX_KEYCOLS);
    }

    public TreeDescriptor(FilePage segmentId) {
        super(segmentId);
        rootPage = new FilePage();
        keyTypes = new Fixchar(MAX_KEYCOLS);
    }

    public TreeDescriptor(TreeDescriptor treeDescriptor) {
        super(treeDescriptor);
        rootPage = new FilePage(treeDescriptor.rootPage);
        keyTypes = new Fixchar(treeDescriptor.keyTypes);
        isUnique = treeDescriptor.isUnique;
        height = treeDescriptor.height;
    }

    public void assign(TreeDescriptor treeDescriptor) {
        super.assign(treeDescriptor);
        rootPage.assign(treeDescriptor.rootPage);
        keyTypes.assign(treeDescriptor.keyTypes);
        isUnique = treeDescriptor.isUnique;
        height = treeDescriptor.height;
    }

    public DBObject copy() {
        return new TreeDescriptor(this);
    }

    public FilePage getRootPage() {
        return rootPage;
    }

    public void setRootPage(FilePage rootPage) {
        this.rootPage.assign(rootPage);
    }

    public int getHeight() {
        return height & 0xff;
    }

    public void setHeight(int height) {
        this.height = (byte) height;
    }

    public String getKeyTypes() throws TypeException {
        return keyTypes.getString();
    }

    public void setKeyTypes(String keyTypes) throws TypeException {
        this.keyTypes.setString(keyTypes);
    }

    public boolean isUnique() {
        return isUnique;
    }

    public void setUnique(boolean isUnique) {
        this.isUnique = isUnique;
    }

    // DBObject methods
    @Override
    public void writeTo(ByteBuffer bb) {
        super.writeTo(bb);
        rootPage.writeTo(bb);
        bb.put(height);
        keyTypes.writeTo(bb);
        bb.put(isUnique ? (byte) 1 : (byte) 0);
    }

    @Override
    public void readFrom(ByteBuffer bb) {
        super.readFrom(bb);
        rootPage.readFrom(bb);
        height = bb.get();
        keyTypes.readFrom(bb);
        isUnique = bb.get() == (byte) 1;
    }
    // identify each row?

    @Override
    public int storeSize() {
        return super.storeSize() + FilePage.sizeOf() + 2 + keyTypes.storeSize();
    }
}
