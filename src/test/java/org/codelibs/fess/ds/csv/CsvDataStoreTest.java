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

    public void test_buildCsvConfig_multiple_parameters() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", "\\t");
        paramMap.put("quote_character", "'");
        paramMap.put("skip_lines", "1");
        paramMap.put("ignore_empty_lines", "true");
        paramMap.put("ignore_leading_whitespaces", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\t', config.getSeparator());
        assertEquals('\'', config.getQuote());
        assertEquals(1, config.getSkipLines());
        assertTrue(config.isIgnoreEmptyLines());
        assertTrue(config.isIgnoreLeadingWhitespaces());
    }

    public void test_buildCsvConfig_invalid_skip_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("skip_lines", "not_a_number");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
        assertEquals(0, config.getSkipLines());
    }

    public void test_buildCsvConfig_invalid_separator() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", "");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_invalid_quote() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_character", "");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_invalid_escape() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_character", "");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_invalid_quote_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_disabled", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_invalid_escape_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_disabled", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_invalid_ignore_leading_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_leading_whitespaces", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_invalid_ignore_trailing_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_trailing_whitespaces", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_buildCsvConfig_invalid_ignore_empty_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_empty_lines", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_hasHeaderLine_invalid_value() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("has_header_line", "invalid_boolean");

        assertFalse(dataStore.hasHeaderLine(paramMap));
    }

    public void test_getCsvFileList_with_real_test_resources() {
        java.io.File testResourcesDir = new java.io.File("src/test/resources");
        if (!testResourcesDir.exists()) {
            return;
        }

        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("directories", testResourcesDir.getAbsolutePath());

        java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

        assertTrue(result.size() >= 3);
        boolean foundCsv = false;
        boolean foundTsv = false;
        for (java.io.File file : result) {
            if (file.getName().endsWith(".csv")) {
                foundCsv = true;
            }
            if (file.getName().endsWith(".tsv")) {
                foundTsv = true;
            }
        }
        assertTrue(foundCsv);
        assertTrue(foundTsv);
    }

    public void test_getCsvFileList_multiple_directories() {
        java.io.File tempDir1 = new java.io.File(System.getProperty("java.io.tmpdir"), "csv_test_dir1");
        java.io.File tempDir2 = new java.io.File(System.getProperty("java.io.tmpdir"), "csv_test_dir2");
        tempDir1.mkdir();
        tempDir2.mkdir();

        java.io.File csvFile1 = new java.io.File(tempDir1, "test1.csv");
        java.io.File csvFile2 = new java.io.File(tempDir2, "test2.csv");

        try {
            csvFile1.createNewFile();
            csvFile2.createNewFile();

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir1.getAbsolutePath() + "," + tempDir2.getAbsolutePath());

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(2, result.size());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile1.delete();
            csvFile2.delete();
            tempDir1.delete();
            tempDir2.delete();
        }
    }

    public void test_getCsvFileList_mixed_csv_and_tsv() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "csv_mixed_test");
        tempDir.mkdir();

        java.io.File csvFile = new java.io.File(tempDir, "data.csv");
        java.io.File tsvFile = new java.io.File(tempDir, "data.tsv");
        java.io.File txtFile = new java.io.File(tempDir, "data.txt");

        try {
            csvFile.createNewFile();
            tsvFile.createNewFile();
            txtFile.createNewFile();

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(2, result.size());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile.delete();
            tsvFile.delete();
            txtFile.delete();
            tempDir.delete();
        }
    }

    public void test_getCsvFileList_files_sorting_by_modified_time() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "csv_sort_test");
        tempDir.mkdir();

        java.io.File csvFile1 = new java.io.File(tempDir, "file1.csv");
        java.io.File csvFile2 = new java.io.File(tempDir, "file2.csv");
        java.io.File csvFile3 = new java.io.File(tempDir, "file3.csv");

        try {
            csvFile1.createNewFile();
            csvFile1.setLastModified(System.currentTimeMillis() - 3000);

            Thread.sleep(100);

            csvFile2.createNewFile();
            csvFile2.setLastModified(System.currentTimeMillis() - 2000);

            Thread.sleep(100);

            csvFile3.createNewFile();
            csvFile3.setLastModified(System.currentTimeMillis() - 1000);

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(3, result.size());
            assertTrue(result.get(0).lastModified() <= result.get(1).lastModified());
            assertTrue(result.get(1).lastModified() <= result.get(2).lastModified());
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile1.delete();
            csvFile2.delete();
            csvFile3.delete();
            tempDir.delete();
        }
    }

    public void test_isCsvFile_various_extensions() {
        java.io.File parentDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        assertTrue(dataStore.isCsvFile(parentDir, "data.csv", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "data.tsv", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "DATA.CSV", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "DATA.TSV", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "file.CSV", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "file.txt", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "file.xls", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "file.xlsx", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "file.json", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "file.xml", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "file", paramMap));
    }

    public void test_getCsvFileEncoding_various_encodings() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        paramMap.put("file_encoding", "UTF-8");
        assertEquals("UTF-8", dataStore.getCsvFileEncoding(paramMap));

        paramMap.put("file_encoding", "Shift_JIS");
        assertEquals("Shift_JIS", dataStore.getCsvFileEncoding(paramMap));

        paramMap.put("file_encoding", "Windows-31J");
        assertEquals("Windows-31J", dataStore.getCsvFileEncoding(paramMap));

        paramMap.put("file_encoding", "EUC-JP");
        assertEquals("EUC-JP", dataStore.getCsvFileEncoding(paramMap));

        paramMap.put("file_encoding", "ISO-8859-1");
        assertEquals("ISO-8859-1", dataStore.getCsvFileEncoding(paramMap));
    }

    public void test_buildCsvConfig_with_special_characters() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", "|");
        paramMap.put("quote_character", "'");
        paramMap.put("escape_character", "/");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('|', config.getSeparator());
        assertEquals('\'', config.getQuote());
        assertEquals('/', config.getEscape());
    }

    public void test_buildCsvConfig_comprehensive() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", ";");
        paramMap.put("quote_character", "'");
        paramMap.put("escape_character", "\\");
        paramMap.put("skip_lines", "3");
        paramMap.put("ignore_empty_lines", "true");
        paramMap.put("ignore_leading_whitespaces", "true");
        paramMap.put("ignore_trailing_whitespaces", "true");
        paramMap.put("quote_disabled", "false");
        paramMap.put("escape_disabled", "false");
        paramMap.put("null_string", "N/A");
        paramMap.put("break_string", "<br>");
        paramMap.put("ignore_line_patterns", "^#.*|^//.*");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals(';', config.getSeparator());
        assertEquals('\'', config.getQuote());
        assertEquals('\\', config.getEscape());
        assertEquals(3, config.getSkipLines());
        assertTrue(config.isIgnoreEmptyLines());
        assertTrue(config.isIgnoreLeadingWhitespaces());
        assertTrue(config.isIgnoreTrailingWhitespaces());
        assertFalse(config.isQuoteDisabled());
        assertFalse(config.isEscapeDisabled());
        assertEquals("N/A", config.getNullString());
        assertEquals("<br>", config.getBreakString());
        assertNotNull(config.getIgnoreLinePatterns());
    }

    public void test_csvFileSuffixs_default() {
        assertNotNull(dataStore.csvFileSuffixs);
        assertEquals(2, dataStore.csvFileSuffixs.length);
        assertEquals(".csv", dataStore.csvFileSuffixs[0]);
        assertEquals(".tsv", dataStore.csvFileSuffixs[1]);
    }

    public void test_getName_not_null() {
        assertNotNull(dataStore.getName());
        assertFalse(dataStore.getName().isEmpty());
    }

    public void test_getCsvFileList_with_multiple_files() {
        java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        java.io.File csvFile1 = new java.io.File(tempDir, "multi_test1.csv");
        java.io.File csvFile2 = new java.io.File(tempDir, "multi_test2.csv");
        java.io.File csvFile3 = new java.io.File(tempDir, "multi_test3.csv");

        try {
            csvFile1.createNewFile();
            csvFile2.createNewFile();
            csvFile3.createNewFile();

            org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("files", csvFile1.getAbsolutePath() + "," + csvFile2.getAbsolutePath() + "," + csvFile3.getAbsolutePath());

            java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(3, result.size());
            assertTrue(result.contains(csvFile1));
            assertTrue(result.contains(csvFile2));
            assertTrue(result.contains(csvFile3));
        } catch (Exception e) {
            fail("Test failed: " + e.getMessage());
        } finally {
            csvFile1.delete();
            csvFile2.delete();
            csvFile3.delete();
        }
    }

    public void test_buildCsvConfig_empty_values() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", "");
        paramMap.put("quote_character", "");
        paramMap.put("escape_character", "");
        paramMap.put("skip_lines", "");
        paramMap.put("ignore_empty_lines", "");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    public void test_hasHeaderLine_case_variations() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        paramMap.put("has_header_line", "TRUE");
        assertTrue(dataStore.hasHeaderLine(paramMap));

        paramMap.put("has_header_line", "True");
        assertTrue(dataStore.hasHeaderLine(paramMap));

        paramMap.put("has_header_line", "FALSE");
        assertFalse(dataStore.hasHeaderLine(paramMap));

        paramMap.put("has_header_line", "False");
        assertFalse(dataStore.hasHeaderLine(paramMap));
    }
}
