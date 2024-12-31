/*
 * Copyright 2012-2024 CodeLibs Project and the Others.
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

import java.io.File;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.crawler.client.CrawlerClientFactory;
import org.codelibs.fess.ds.callback.FileListIndexUpdateCallbackImpl;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.util.ComponentUtil;

import com.orangesignal.csv.CsvConfig;

public class CsvListDataStore extends CsvDataStore {

    private static final Logger logger = LogManager.getLogger(CsvListDataStore.class);

    protected static final String TIMESTAMP_MARGIN = "timestamp_margin";

    public boolean deleteProcessedFile = true;

    public long csvFileTimestampMargin = 10 * 1000L;// 10s

    public boolean ignoreDataStoreException = true;

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    @Override
    protected boolean isCsvFile(final File parentFile, final String filename, final DataStoreParams paramMap) {
        if (super.isCsvFile(parentFile, filename, paramMap)) {
            final File file = new File(parentFile, filename);
            final long now = System.currentTimeMillis();
            return now - file.lastModified() > getTimestampMargin(paramMap);
        }
        return false;
    }

    protected long getTimestampMargin(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(TIMESTAMP_MARGIN);
        if (StringUtil.isNotBlank(value)) {
            try {
                return Long.parseLong(value);
            } catch (final NumberFormatException e) {
                logger.warn("Invalid {}.", TIMESTAMP_MARGIN, e);
            }
        }
        return csvFileTimestampMargin;
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {
        int nThreads = 1;
        if (paramMap.containsKey(Constants.NUM_OF_THREADS)) {
            try {
                nThreads = Integer.parseInt(paramMap.getAsString(Constants.NUM_OF_THREADS, "1"));
            } catch (final NumberFormatException e) {
                logger.warn("{} is not int value.", Constants.NUM_OF_THREADS, e);
            }
        }
        final CrawlerClientFactory crawlerClientFactory = ComponentUtil.getCrawlerClientFactory();
        dataConfig.initializeClientFactory(() -> crawlerClientFactory);
        try {
            final FileListIndexUpdateCallbackImpl fileListIndexUpdateCallback =
                    new FileListIndexUpdateCallbackImpl(callback, crawlerClientFactory, nThreads);
            super.storeData(dataConfig, fileListIndexUpdateCallback, paramMap, scriptMap, defaultDataMap);
            fileListIndexUpdateCallback.commit();
        } catch (final Exception e) {
            throw new DataStoreException(e);
        }
    }

    @Override
    protected void processCsv(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final CsvConfig csvConfig, final File csvFile,
            final long readInterval, final String csvFileEncoding, final boolean hasHeaderLine) {
        try {
            super.processCsv(dataConfig, callback, paramMap, scriptMap, defaultDataMap, csvConfig, csvFile, readInterval, csvFileEncoding,
                    hasHeaderLine);

            // delete csv file
            if (deleteProcessedFile && !csvFile.delete()) {
                logger.warn("Failed to delete {}", csvFile.getAbsolutePath());
            }
        } catch (final DataStoreException e) {
            if (!ignoreDataStoreException) {
                throw e;
            }
            logger.error("Failed to process {}", csvFile.getAbsolutePath(), e);
            // rename csv file, or delete it if failed
            if (!csvFile.renameTo(new File(csvFile.getParent(), csvFile.getName() + ".txt")) && !csvFile.delete()) {
                logger.warn("Failed to delete {}", csvFile.getAbsolutePath());
            }
        }
    }

}
