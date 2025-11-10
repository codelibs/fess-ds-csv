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

    public void test_getTimestampMargin_zero_value() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "0");

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(0L, result);
    }

    public void test_getTimestampMargin_large_value() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "60000");

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(60000L, result);
    }

    public void test_getTimestampMargin_negative_value() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "-1000");

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(-1000L, result);
    }

    public void test_isCsvFile_tsv_with_timestamp() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File tsvFile = new java.io.File(tempDir, "test.tsv");

        try {
            tsvFile.createNewFile();
            tsvFile.setLastModified(System.currentTimeMillis() - 20000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("timestamp_margin", "10000");

            boolean result = dataStore.isCsvFile(tempDir, tsvFile.getName(), paramMap);

            assertTrue(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            tsvFile.delete();
        }
    }

    public void test_isCsvFile_boundary_timestamp() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile = new java.io.File(tempDir, "boundary_test.csv");

        try {
            csvFile.createNewFile();
            long margin = 10000L;
            csvFile.setLastModified(System.currentTimeMillis() - margin - 1);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("timestamp_margin", String.valueOf(margin));

            boolean result = dataStore.isCsvFile(tempDir, csvFile.getName(), paramMap);

            assertTrue(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
        }
    }

    public void test_getCsvFileList_with_timestamp_filter() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "timestamp_filter_test");
        tempDir.mkdir();

        java.io.File oldCsv = new java.io.File(tempDir, "old.csv");
        java.io.File newCsv = new java.io.File(tempDir, "new.csv");

        try {
            oldCsv.createNewFile();
            oldCsv.setLastModified(System.currentTimeMillis() - 20000);

            newCsv.createNewFile();
            newCsv.setLastModified(System.currentTimeMillis() - 5000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());
            paramMap.put("timestamp_margin", "10000");

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(1, result.size());
            assertEquals(oldCsv.getName(), result.get(0).getName());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            oldCsv.delete();
            newCsv.delete();
            tempDir.delete();
        }
    }

    public void test_getCsvFileList_all_files_too_recent() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "all_recent_test");
        tempDir.mkdir();

        java.io.File csvFile1 = new java.io.File(tempDir, "file1.csv");
        java.io.File csvFile2 = new java.io.File(tempDir, "file2.csv");

        try {
            csvFile1.createNewFile();
            csvFile2.createNewFile();

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());
            paramMap.put("timestamp_margin", "10000");

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(0, result.size());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile1.delete();
            csvFile2.delete();
            tempDir.delete();
        }
    }

    public void test_getCsvFileList_mixed_old_and_new_files() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "mixed_age_test");
        tempDir.mkdir();

        java.io.File oldFile1 = new java.io.File(tempDir, "old1.csv");
        java.io.File newFile = new java.io.File(tempDir, "new.csv");
        java.io.File oldFile2 = new java.io.File(tempDir, "old2.csv");

        try {
            oldFile1.createNewFile();
            oldFile1.setLastModified(System.currentTimeMillis() - 15000);

            newFile.createNewFile();
            newFile.setLastModified(System.currentTimeMillis() - 5000);

            oldFile2.createNewFile();
            oldFile2.setLastModified(System.currentTimeMillis() - 20000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());
            paramMap.put("timestamp_margin", "10000");

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(2, result.size());
            assertTrue(result.stream().anyMatch(f -> f.getName().equals("old1.csv")));
            assertTrue(result.stream().anyMatch(f -> f.getName().equals("old2.csv")));
            assertFalse(result.stream().anyMatch(f -> f.getName().equals("new.csv")));
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            oldFile1.delete();
            newFile.delete();
            oldFile2.delete();
            tempDir.delete();
        }
    }

    public void test_getTimestampMargin_whitespace_value() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "  ");

        long result = dataStore.getTimestampMargin(paramMap);

        assertEquals(10000L, result);
    }

    public void test_isCsvFile_inherits_parent_behavior() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        java.io.File txtFile = new java.io.File(tempDir, "test.txt");
        try {
            txtFile.createNewFile();
            txtFile.setLastModified(System.currentTimeMillis() - 20000);

            boolean result = dataStore.isCsvFile(tempDir, txtFile.getName(), paramMap);

            assertFalse(result);
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            txtFile.delete();
        }
    }

    public void test_getCsvFileList_with_zero_timestamp_margin() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "zero_margin_test");
        tempDir.mkdir();

        java.io.File csvFile = new java.io.File(tempDir, "test.csv");

        try {
            csvFile.createNewFile();
            csvFile.setLastModified(System.currentTimeMillis() - 1);

            Thread.sleep(10);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());
            paramMap.put("timestamp_margin", "0");

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(1, result.size());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
            tempDir.delete();
        }
    }

    public void test_default_field_values_are_correct() {
        assertEquals(10000L, dataStore.csvFileTimestampMargin);
        assertTrue(dataStore.deleteProcessedFile);
        assertTrue(dataStore.ignoreDataStoreException);
    }

    public void test_getName_is_correct() {
        assertEquals("CsvListDataStore", dataStore.getName());
        assertNotNull(dataStore.getName());
        assertFalse(dataStore.getName().isEmpty());
    }

    public void test_inheritance_methods() {
        assertTrue(dataStore instanceof CsvDataStore);

        assertNotNull(dataStore.csvFileSuffixs);
        assertEquals(2, dataStore.csvFileSuffixs.length);
    }

    public void test_getCsvFileList_with_files_parameter() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile = new java.io.File(tempDir, "list_test.csv");

        try {
            csvFile.createNewFile();
            csvFile.setLastModified(System.currentTimeMillis() - 20000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("files", csvFile.getAbsolutePath());

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(1, result.size());
            assertEquals(csvFile.getName(), result.get(0).getName());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
        }
    }

    public void test_isCsvFile_various_file_ages() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("timestamp_margin", "10000");

        java.io.File veryOldFile = new java.io.File(tempDir, "very_old.csv");
        java.io.File mediumOldFile = new java.io.File(tempDir, "medium_old.csv");
        java.io.File justOldFile = new java.io.File(tempDir, "just_old.csv");
        java.io.File newFile = new java.io.File(tempDir, "new.csv");

        try {
            veryOldFile.createNewFile();
            veryOldFile.setLastModified(System.currentTimeMillis() - 60000);

            mediumOldFile.createNewFile();
            mediumOldFile.setLastModified(System.currentTimeMillis() - 30000);

            justOldFile.createNewFile();
            justOldFile.setLastModified(System.currentTimeMillis() - 11000);

            newFile.createNewFile();
            newFile.setLastModified(System.currentTimeMillis() - 5000);

            assertTrue(dataStore.isCsvFile(tempDir, veryOldFile.getName(), paramMap));
            assertTrue(dataStore.isCsvFile(tempDir, mediumOldFile.getName(), paramMap));
            assertTrue(dataStore.isCsvFile(tempDir, justOldFile.getName(), paramMap));
            assertFalse(dataStore.isCsvFile(tempDir, newFile.getName(), paramMap));
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            veryOldFile.delete();
            mediumOldFile.delete();
            justOldFile.delete();
            newFile.delete();
        }
    }

    public void test_field_mutation() {
        dataStore.deleteProcessedFile = false;
        assertFalse(dataStore.deleteProcessedFile);

        dataStore.deleteProcessedFile = true;
        assertTrue(dataStore.deleteProcessedFile);

        dataStore.ignoreDataStoreException = false;
        assertFalse(dataStore.ignoreDataStoreException);

        dataStore.ignoreDataStoreException = true;
        assertTrue(dataStore.ignoreDataStoreException);

        dataStore.csvFileTimestampMargin = 5000L;
        assertEquals(5000L, dataStore.csvFileTimestampMargin);

        dataStore.csvFileTimestampMargin = 10000L;
        assertEquals(10000L, dataStore.csvFileTimestampMargin);
    }

    public void test_getCsvFileList_sorting_with_timestamp_filter() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "sort_filter_test");
        tempDir.mkdir();

        java.io.File file1 = new java.io.File(tempDir, "file1.csv");
        java.io.File file2 = new java.io.File(tempDir, "file2.csv");
        java.io.File file3 = new java.io.File(tempDir, "file3.csv");

        try {
            file1.createNewFile();
            file1.setLastModified(System.currentTimeMillis() - 30000);

            file2.createNewFile();
            file2.setLastModified(System.currentTimeMillis() - 20000);

            file3.createNewFile();
            file3.setLastModified(System.currentTimeMillis() - 25000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());
            paramMap.put("timestamp_margin", "10000");

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(3, result.size());
            assertTrue(result.get(0).lastModified() <= result.get(1).lastModified());
            assertTrue(result.get(1).lastModified() <= result.get(2).lastModified());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            file1.delete();
            file2.delete();
            file3.delete();
            tempDir.delete();
        }
    }
}
