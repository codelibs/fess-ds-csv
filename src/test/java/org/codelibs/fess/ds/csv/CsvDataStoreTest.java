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

public class CsvDataStoreTest extends ContainerTestCase {
    public CsvDataStore dataStore;

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
        dataStore = new CsvDataStore();
    }

    @Override
    public void tearDown() throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown();
    }

    public void test_getName() {
        assertEquals("CsvDataStore", dataStore.getName());
    }

    public void test_getCsvFileList_with_files_param() {
        // Create temporary CSV files
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile1 = new java.io.File(tempDir, "test1.csv");
        java.io.File csvFile2 = new java.io.File(tempDir, "test2.csv");

        try {
            csvFile1.createNewFile();
            csvFile2.createNewFile();

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("files", csvFile1.getAbsolutePath() + "," + csvFile2.getAbsolutePath());

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(2, result.size());
            assertTrue(result.contains(csvFile1));
            assertTrue(result.contains(csvFile2));
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile1.delete();
            csvFile2.delete();
        }
    }

    public void test_getCsvFileList_with_directories_param() {
        // Create temporary directory with CSV files
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "csv_test_dir");
        tempDir.mkdir();

        java.io.File csvFile = new java.io.File(tempDir, "test.csv");
        java.io.File tsvFile = new java.io.File(tempDir, "test.tsv");
        java.io.File txtFile = new java.io.File(tempDir, "test.txt");

        try {
            csvFile.createNewFile();
            tsvFile.createNewFile();
            txtFile.createNewFile();

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(2, result.size());
            assertTrue(result.stream().anyMatch(f -> f.getName().equals("test.csv")));
            assertTrue(result.stream().anyMatch(f -> f.getName().equals("test.tsv")));
            assertFalse(result.stream().anyMatch(f -> f.getName().equals("test.txt")));
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
            tsvFile.delete();
            txtFile.delete();
            tempDir.delete();
        }
    }

    public void test_getCsvFileList_empty_params() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        try {
            dataStore.getCsvFileList(paramMap);
            fail("Should throw DataStoreException");
        } catch (org.codelibs.fess.exception.DataStoreException e) {
            assertTrue(e.getMessage().contains("files") && e.getMessage().contains("directories"));
        }
    }

    public void test_isCsvFile() {
        java.io.File parentDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        assertTrue(dataStore.isCsvFile(parentDir, "test.csv", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "test.tsv", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "TEST.CSV", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "test.txt", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "test.xlsx", paramMap));
    }

    public void test_getCsvFileEncoding_default() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        assertEquals("UTF-8", dataStore.getCsvFileEncoding(paramMap));
    }

    public void test_getCsvFileEncoding_custom() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("file_encoding", "Shift_JIS");

        assertEquals("Shift_JIS", dataStore.getCsvFileEncoding(paramMap));
    }

    public void test_hasHeaderLine_default() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        assertFalse(dataStore.hasHeaderLine(paramMap));
    }

    public void test_hasHeaderLine_true() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("has_header_line", "true");

        assertTrue(dataStore.hasHeaderLine(paramMap));
    }

    public void test_hasHeaderLine_false() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("has_header_line", "false");

        assertFalse(dataStore.hasHeaderLine(paramMap));
    }

    public void test_buildCsvConfig_default() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_custom_separator() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", "\\t");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\t', config.getSeparator());
    }

    public void test_buildCsvConfig_custom_quote() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_character", "'");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\'', config.getQuote());
    }

    public void test_buildCsvConfig_custom_escape() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_character", "\\");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\\', config.getEscape());
    }

    public void test_buildCsvConfig_skip_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("skip_lines", "2");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals(2, config.getSkipLines());
    }

    public void test_buildCsvConfig_ignore_empty_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_empty_lines", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isIgnoreEmptyLines());
    }

    public void test_buildCsvConfig_ignore_leading_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_leading_whitespaces", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isIgnoreLeadingWhitespaces());
    }

    public void test_buildCsvConfig_ignore_trailing_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_trailing_whitespaces", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isIgnoreTrailingWhitespaces());
    }

    public void test_getCsvFileList_with_test_resources() {
        java.io.File testResourcesDir = new java.io.File("src/test/resources");
        if (!testResourcesDir.exists()) {
            return;
        }

        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("directories", testResourcesDir.getAbsolutePath());

        java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

        assertTrue(result.size() > 0);
        assertTrue(result.stream().anyMatch(f -> f.getName().endsWith(".csv") || f.getName().endsWith(".tsv")));
    }

    public void test_buildCsvConfig_null_string() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("null_string", "NULL");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals("NULL", config.getNullString());
    }

    public void test_buildCsvConfig_break_string() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("break_string", "\\n");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals("\\n", config.getBreakString());
    }

    public void test_buildCsvConfig_quote_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_disabled", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isQuoteDisabled());
    }

    public void test_buildCsvConfig_escape_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_disabled", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isEscapeDisabled());
    }

    public void test_buildCsvConfig_ignore_line_patterns() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_line_patterns", "^#.*");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config.getIgnoreLinePatterns());
    }

    public void test_getCsvFileList_nonexistent_directory() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("directories", "/nonexistent/directory");

        java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

        assertEquals(0, result.size());
    }

    public void test_getCsvFileList_nonexistent_file() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("files", "/nonexistent/file.csv");

        java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

        assertEquals(0, result.size());
    }
}
