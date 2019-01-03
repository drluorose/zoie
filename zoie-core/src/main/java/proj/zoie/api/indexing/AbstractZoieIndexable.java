package proj.zoie.api.indexing;

/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

public abstract class AbstractZoieIndexable implements ZoieIndexable {
    public static final String DOCUMENT_ID_PAYLOAD_FIELD = "_ID";
    public static final String DOCUMENT_STORE_FIELD = "_STORE";

    @Override
    public abstract IndexingReq[] buildIndexingReqs();

    @Override
    abstract public long getUID();

    @Override
    abstract public boolean isDeleted();

    @Override
    public boolean isSkip() {
        return false;
    }

    @Override
    public boolean isStorable() {
        return false;
    }

    @Override
    public byte[] getStoreValue() {
        return null;
    }
}
