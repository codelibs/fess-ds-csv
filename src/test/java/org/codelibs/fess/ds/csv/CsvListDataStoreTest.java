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

import org.codelibs.fess.util.ComponentUtil;
import org.dbflute.utflute.lastadi.ContainerTestCase;

public class CsvListDataStoreTest extends ContainerTestCase {
    public CsvListDataStore dataStore;

    @Override
    protected String prepareConfigFile() {
        return "test_app.xml";
    }

    @Override
    protected boolean isSuppressTestCaseTransaction() {
        return true;
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataStore = new CsvListDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("CsvListDataStore", dataStore.getName());
    }

    public void test_getTimestampMargin_default() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(10000L, result);
    }

    public void test_getTimestampMargin_custom() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "5000");

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(5000L, result);
    }

    public void test_getTimestampMargin_invalid() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "invalid");

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(10000L, result);
    }

    public void test_isCsvFile_recent_file() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile = new java.io.File(tempDir, "recent_test.csv");

        try {
            csvFile.createNewFile();

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("timestamp_margin", "1000");

            boolean result = dataStore.isCsvFile(tempDir, csvFile.getName(), paramMap);

            assertFalse(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
        }
    }

    public void test_isCsvFile_old_file() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile = new java.io.File(tempDir, "old_test.csv");

        try {
            csvFile.createNewFile();

            csvFile.setLastModified(System.currentTimeMillis() - 15000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("timestamp_margin", "10000");

            boolean result = dataStore.isCsvFile(tempDir, csvFile.getName(), paramMap);

            assertTrue(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
        }
    }

    public void test_isCsvFile_non_csv_file() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File txtFile = new java.io.File(tempDir, "test.txt");

        try {
            txtFile.createNewFile();
            txtFile.setLastModified(System.currentTimeMillis() - 15000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

            boolean result = dataStore.isCsvFile(tempDir, txtFile.getName(), paramMap);

            assertFalse(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            txtFile.delete();
        }
    }

    public void test_deleteProcessedFile_default() {
        assertTrue(dataStore.deleteProcessedFile);
    }

    public void test_csvFileTimestampMargin_default() {
        assertEquals(10000L, dataStore.csvFileTimestampMargin);
    }

    public void test_ignoreDataStoreException_default() {
        assertTrue(dataStore.ignoreDataStoreException);
    }

    public void test_inheritance_from_CsvDataStore() {
        assertTrue(dataStore instanceof CsvDataStore);
    }

    public void test_getTimestampMargin_empty_string() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "");

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(10000L, result);
    }

    public void test_getTimestampMargin_null_value() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", null);

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(10000L, result);
    }

    public void test_isCsvFile_with_default_timestamp_margin() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile = new java.io.File(tempDir, "default_margin_test.csv");

        try {
            csvFile.createNewFile();
            csvFile.setLastModified(System.currentTimeMillis() - 15000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

            boolean result = dataStore.isCsvFile(tempDir, csvFile.getName(), paramMap);

            assertTrue(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
        }
    }

    public void test_isCsvFile_edge_case_timing() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile = new java.io.File(tempDir, "edge_timing_test.csv");

        try {
            csvFile.createNewFile();

            long margin = 5000L;
            csvFile.setLastModified(System.currentTimeMillis() - margin);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("timestamp_margin", String.valueOf(margin));

            Thread.sleep(100);

            boolean result = dataStore.isCsvFile(tempDir, csvFile.getName(), paramMap);

            assertTrue(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
        }
    }

    public void test_csvFileSuffixs_inherited() {
        assertNotNull(dataStore.csvFileSuffixs);
        assertEquals(2, dataStore.csvFileSuffixs.length);
        assertEquals(".csv", dataStore.csvFileSuffixs[0]);
        assertEquals(".tsv", dataStore.csvFileSuffixs[1]);
    }

    public void test_constants_values() {
        assertEquals(10000L, dataStore.csvFileTimestampMargin);
        assertTrue(dataStore.deleteProcessedFile);
        assertTrue(dataStore.ignoreDataStoreException);
    }

    public void test_isCsvFile_case_insensitive() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile = new java.io.File(tempDir, "CASE_TEST.CSV");

        try {
            csvFile.createNewFile();
            csvFile.setLastModified(System.currentTimeMillis() - 15000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

            boolean result = dataStore.isCsvFile(tempDir, csvFile.getName(), paramMap);

            assertTrue(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
        }
    }
}
