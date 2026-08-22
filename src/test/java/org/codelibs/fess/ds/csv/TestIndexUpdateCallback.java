/*
 * Copyright 2012-2025 CodeLibs Project and the Others.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */
package org.codelibs.fess.ds.csv;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;

/**
 * Collects the documents handed to the callback so tests can assert on them.
 * Mirrors what IndexUpdateCallbackImpl requires: a document without a url is rejected.
 */
public class TestIndexUpdateCallback implements IndexUpdateCallback {

    /** Documents accepted by this callback, in order. */
    public final List<Map<String, Object>> dataMapList = new ArrayList<>();

    /** When true, reject documents without a url the way IndexUpdateCallbackImpl does. */
    public boolean requireUrl = true;

    @Override
    public void store(final DataStoreParams paramMap, final Map<String, Object> dataMap) {
        if (requireUrl && dataMap.get("url") == null) {
            throw new org.codelibs.fess.exception.DataStoreException("URL field is null in dataMap.");
        }
        dataMapList.add(new HashMap<>(dataMap));
    }

    @Override
    public long getDocumentSize() {
        return dataMapList.size();
    }

    @Override
    public long getExecuteTime() {
        return 0L;
    }

    @Override
    public void commit() {
        // nothing to flush
    }
}
