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

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;

import org.codelibs.fess.util.ComponentUtil;

public class CsvDataStoreTest extends UnitDsTestCase {
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
    public void setUp(TestInfo testInfo) throws Exception {
        super.setUp(testInfo);
        dataStore = new CsvDataStore();
    }

    @Override
    public void tearDown(TestInfo testInfo) throws Exception {
        ComponentUtil.setFessConfig(null);
        super.tearDown(testInfo);
    }

    @Test
    public void test_getName() {
        assertEquals("CsvDataStore", dataStore.getName());
    }

    @Test
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

    @Test
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

    @Test
    public void test_getCsvFileList_empty_params() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        try {
            dataStore.getCsvFileList(paramMap);
            fail("Should throw DataStoreException");
        } catch (org.codelibs.fess.exception.DataStoreException e) {
            assertTrue(e.getMessage().contains("files") && e.getMessage().contains("directories"));
        }
    }

    @Test
    public void test_isCsvFile() {
        java.io.File parentDir = new java.io.File(System.getProperty("java.io.tmpdir"));
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        assertTrue(dataStore.isCsvFile(parentDir, "test.csv", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "test.tsv", paramMap));
        assertTrue(dataStore.isCsvFile(parentDir, "TEST.CSV", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "test.txt", paramMap));
        assertFalse(dataStore.isCsvFile(parentDir, "test.xlsx", paramMap));
    }

    @Test
    public void test_getCsvFileEncoding_default() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        assertEquals("UTF-8", dataStore.getCsvFileEncoding(paramMap));
    }

    @Test
    public void test_getCsvFileEncoding_custom() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("file_encoding", "Shift_JIS");

        assertEquals("Shift_JIS", dataStore.getCsvFileEncoding(paramMap));
    }

    @Test
    public void test_hasHeaderLine_default() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        assertFalse(dataStore.hasHeaderLine(paramMap));
    }

    @Test
    public void test_hasHeaderLine_true() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("has_header_line", "true");

        assertTrue(dataStore.hasHeaderLine(paramMap));
    }

    @Test
    public void test_hasHeaderLine_false() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("has_header_line", "false");

        assertFalse(dataStore.hasHeaderLine(paramMap));
    }

    @Test
    public void test_buildCsvConfig_default() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_custom_separator() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", "\\t");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\t', config.getSeparator());
    }

    @Test
    public void test_buildCsvConfig_custom_quote() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_character", "'");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\'', config.getQuote());
    }

    @Test
    public void test_buildCsvConfig_custom_escape() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_character", "\\");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\\', config.getEscape());
    }

    @Test
    public void test_buildCsvConfig_skip_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("skip_lines", "2");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals(2, config.getSkipLines());
    }

    @Test
    public void test_buildCsvConfig_ignore_empty_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_empty_lines", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isIgnoreEmptyLines());
    }

    @Test
    public void test_buildCsvConfig_ignore_leading_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_leading_whitespaces", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isIgnoreLeadingWhitespaces());
    }

    @Test
    public void test_buildCsvConfig_ignore_trailing_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_trailing_whitespaces", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isIgnoreTrailingWhitespaces());
    }

    @Test
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

    @Test
    public void test_buildCsvConfig_null_string() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("null_string", "NULL");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals("NULL", config.getNullString());
    }

    @Test
    public void test_buildCsvConfig_break_string() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("break_string", "\\n");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals("\\n", config.getBreakString());
    }

    @Test
    public void test_buildCsvConfig_quote_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_disabled", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isQuoteDisabled());
    }

    @Test
    public void test_buildCsvConfig_escape_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_disabled", "true");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isEscapeDisabled());
    }

    @Test
    public void test_buildCsvConfig_ignore_line_patterns() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_line_patterns", "^#.*");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config.getIgnoreLinePatterns());
    }

    @Test
    public void test_getCsvFileList_nonexistent_directory() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("directories", "/nonexistent/directory");

        java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

        assertEquals(0, result.size());
    }

    @Test
    public void test_getCsvFileList_nonexistent_file() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("files", "/nonexistent/file.csv");

        java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

        assertEquals(0, result.size());
    }

    @Test
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

    @Test
    public void test_buildCsvConfig_invalid_skip_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("skip_lines", "not_a_number");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
        assertEquals(0, config.getSkipLines());
    }

    @Test
    public void test_buildCsvConfig_invalid_separator() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("separator_character", "");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_invalid_quote() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_character", "");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_invalid_escape() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_character", "");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_invalid_quote_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_disabled", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_invalid_escape_disabled() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_disabled", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_invalid_ignore_leading_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_leading_whitespaces", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_invalid_ignore_trailing_whitespaces() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_trailing_whitespaces", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_buildCsvConfig_invalid_ignore_empty_lines() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore_empty_lines", "not_boolean");

        com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertNotNull(config);
    }

    @Test
    public void test_hasHeaderLine_invalid_value() {
        org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("has_header_line", "invalid_boolean");

        assertFalse(dataStore.hasHeaderLine(paramMap));
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void test_csvFileSuffixs_default() {
        assertNotNull(dataStore.csvFileSuffixs);
        assertEquals(2, dataStore.csvFileSuffixs.length);
        assertEquals(".csv", dataStore.csvFileSuffixs[0]);
        assertEquals(".tsv", dataStore.csvFileSuffixs[1]);
    }

    @Test
    public void test_getName_not_null() {
        assertNotNull(dataStore.getName());
        assertFalse(dataStore.getName().isEmpty());
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void test_getCsvFileList_sorting_beyond_int_overflow_threshold() {
        // A long millisecond difference above Integer.MAX_VALUE (~24.8 days) wraps when cast to int,
        // which silently reverses the comparison. Spread the files far enough apart to hit that.
        // 30-day steps are chosen deliberately: (int) 2_592_000_000L is -1_702_967_296, so the
        // old comparator inverted every adjacent comparison and could not return sorted output.
        // (Which wrong order it produced depended on File.listFiles() order, because a
        // sign-flipping comparator also breaks transitivity.) Not every spread exposes the bug -
        // 100- and 300-day gaps happen to wrap back to the correct sign and would make this
        // test vacuous.
        final java.io.File tempDir = new java.io.File(System.getProperty("java.io.tmpdir"), "csv_overflow_sort_test_" + System.nanoTime());
        tempDir.mkdirs();

        final long day = 24L * 60L * 60L * 1000L;
        final long base = System.currentTimeMillis() - 100L * day;
        final java.io.File oldest = new java.io.File(tempDir, "oldest.csv");
        final java.io.File middle = new java.io.File(tempDir, "middle.csv");
        final java.io.File newest = new java.io.File(tempDir, "newest.csv");

        try {
            oldest.createNewFile();
            middle.createNewFile();
            newest.createNewFile();
            oldest.setLastModified(base);
            middle.setLastModified(base + 30L * day);
            newest.setLastModified(base + 60L * day);

            final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            paramMap.put("directories", tempDir.getAbsolutePath());

            final java.util.List<java.io.File> result = dataStore.getCsvFileList(paramMap);

            assertEquals(3, result.size());
            assertEquals("oldest.csv", result.get(0).getName());
            assertEquals("middle.csv", result.get(1).getName());
            assertEquals("newest.csv", result.get(2).getName());
        } catch (final java.io.IOException e) {
            fail("Failed to prepare test files: " + e.getMessage());
        } finally {
            oldest.delete();
            middle.delete();
            newest.delete();
            tempDir.delete();
        }
    }

    /** Parses the given CSV text with the config produced by buildCsvConfig and returns the rows. */
    private java.util.List<java.util.List<String>> parseWith(final org.codelibs.fess.entity.DataStoreParams paramMap, final String csv)
            throws Exception {
        final com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);
        final java.util.List<java.util.List<String>> rows = new java.util.ArrayList<>();
        try (com.orangesignal.csv.CsvReader reader = new com.orangesignal.csv.CsvReader(new java.io.StringReader(csv), config)) {
            java.util.List<String> row;
            while ((row = reader.readValues()) != null) {
                if (row.size() == 1 && row.get(0).isEmpty()) {
                    continue; // trailing empty record at EOF
                }
                rows.add(row);
            }
        }
        return rows;
    }

    @Test
    public void test_buildCsvConfig_defaults_to_rfc4180_quoting() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        final java.util.List<java.util.List<String>> rows = parseWith(paramMap, "1,\"a,b\",x\n");

        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0).size());
        assertEquals("1", rows.get(0).get(0));
        assertEquals("a,b", rows.get(0).get(1));
        assertEquals("x", rows.get(0).get(2));
    }

    @Test
    public void test_buildCsvConfig_defaults_keep_quoted_line_breaks_in_one_record() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        final java.util.List<java.util.List<String>> rows = parseWith(paramMap, "1,\"l1\nl2\",x\n");

        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0).size());
        assertEquals("l1\nl2", rows.get(0).get(1));
    }

    @Test
    public void test_buildCsvConfig_defaults_unescape_doubled_quotes() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        final java.util.List<java.util.List<String>> rows = parseWith(paramMap, "1,\"say \"\"hi\"\"\",x\n");

        assertEquals(1, rows.size());
        assertEquals("say \"hi\"", rows.get(0).get(1));
    }

    @Test
    public void test_buildCsvConfig_defaults_strip_surrounding_quotes() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        final java.util.List<java.util.List<String>> rows = parseWith(paramMap, "\"1\",\"abc\",\"x\"\n");

        assertEquals(java.util.List.of("1", "abc", "x"), rows.get(0));
    }

    @Test
    public void test_buildCsvConfig_defaults_leave_unquoted_values_untouched() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        // Values that are not quoted must parse exactly as before this change.
        assertEquals(java.util.List.of("1", "say \"hi\" now", "x"), parseWith(paramMap, "1,say \"hi\" now,x\n").get(0));
        assertEquals(java.util.List.of("1", "abc\"", "x"), parseWith(paramMap, "1,abc\",x\n").get(0));
        assertEquals(java.util.List.of("1", "C:\\path\\to", "x"), parseWith(paramMap, "1,C:\\path\\to,x\n").get(0));
        assertEquals(java.util.List.of("1", "", "x"), parseWith(paramMap, "1,,x\n").get(0));
        assertEquals(java.util.List.of("1", "山田太郎", "営業部"), parseWith(paramMap, "1,山田太郎,営業部\n").get(0));
    }

    @Test
    public void test_buildCsvConfig_explicit_quote_disabled_still_wins() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_disabled", "true");

        // With quoting explicitly disabled the quoted comma splits the field, as before.
        final java.util.List<java.util.List<String>> rows = parseWith(paramMap, "1,\"a,b\",x\n");

        assertEquals(java.util.List.of("1", "\"a", "b\"", "x"), rows.get(0));
    }

    @Test
    public void test_buildCsvConfig_quote_disabled_restores_previous_parsing_exactly() throws Exception {
        // quote_disabled=true is the documented way back to the old behaviour. Escaping must be
        // disabled along with it, or the quote character swallows the following separator and
        // produces a third parse that matches neither the old nor the RFC 4180 result.
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_disabled", "true");

        assertEquals(java.util.List.of("1", "\"say \"\"hi\"\"\"", "x"), parseWith(paramMap, "1,\"say \"\"hi\"\"\",x\n").get(0));
        assertEquals(java.util.List.of("1", "say \"hi\" now", "x"), parseWith(paramMap, "1,say \"hi\" now,x\n").get(0));
    }

    @Test
    public void test_buildCsvConfig_explicit_escape_disabled_still_wins() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_disabled", "true");

        final com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertTrue(config.isEscapeDisabled());
        assertFalse(config.isQuoteDisabled());
    }

    @Test
    public void test_buildCsvConfig_explicit_escape_character_still_wins() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("escape_character", "\\");

        final com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\\', config.getEscape());
    }

    @Test
    public void test_buildCsvConfig_escape_follows_custom_quote_character() throws Exception {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("quote_character", "'");

        final com.orangesignal.csv.CsvConfig config = dataStore.buildCsvConfig(paramMap);

        assertEquals('\'', config.getQuote());
        assertEquals('\'', config.getEscape());
    }

    @Test
    public void test_buildCsvConfig_unclosed_quote_absorbs_the_rest_of_the_file() throws Exception {
        // Enabling quoting reaches further than a single field: one unmatched quote makes the parser
        // treat everything after it as a single value, so the remaining rows never become documents
        // and nothing warns. Pinned here so the consequence stays visible.
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();

        final java.util.List<java.util.List<String>> rows = parseWith(paramMap, "1,\"unbalanced,x\n2,alice,y\n3,bob,z\n");

        assertEquals(1, rows.size());
        assertEquals(java.util.List.of("1", "\"unbalanced,x\n2,alice,y\n3,bob,z\n"), rows.get(0));
    }

    /** Registers the components processCsv needs so it can run inside this unit-test container. */
    private void registerCrawlerComponents() {
        org.codelibs.fess.util.ComponentUtil.register(new org.codelibs.fess.helper.SystemHelper(), "systemHelper");
        final org.codelibs.fess.helper.CrawlerStatsHelper crawlerStatsHelper = new org.codelibs.fess.helper.CrawlerStatsHelper();
        crawlerStatsHelper.init();
        org.codelibs.fess.util.ComponentUtil.register(crawlerStatsHelper, "crawlerStatsHelper");
        // The test container only includes convention.xml/lastaflute.xml (not Fess's fess_se.xml), so the
        // "groovy" engine convertValue() relies on is otherwise absent; register it the same way production
        // DI does (fess_se.xml + fess_se++.xml) so the documented row-filtering scripts actually evaluate.
        final org.codelibs.fess.script.ScriptEngineFactory scriptEngineFactory = new org.codelibs.fess.script.ScriptEngineFactory();
        org.codelibs.fess.util.ComponentUtil.register(scriptEngineFactory, "scriptEngineFactory");
        final org.codelibs.fess.script.groovy.GroovyEngine groovyEngine = new org.codelibs.fess.script.groovy.GroovyEngine();
        groovyEngine.init();
        groovyEngine.register();
    }

    /** Writes the given text to a temporary .csv file that the caller must delete. */
    private java.io.File writeTempCsv(final String content) throws java.io.IOException {
        final java.io.File file = java.io.File.createTempFile("fess-ds-csv-test-", ".csv");
        java.nio.file.Files.writeString(file.toPath(), content, java.nio.charset.StandardCharsets.UTF_8);
        return file;
    }

    @Test
    public void test_processCsv_indexes_each_row_with_header_names_and_cells() throws Exception {
        registerCrawlerComponents();
        final java.io.File csvFile = writeTempCsv("id,name\n1,alice\n2,bob\n");
        try {
            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            final java.util.Map<String, String> scriptMap = new java.util.LinkedHashMap<>();
            scriptMap.put("url", "\"http://example.com/\" + id");
            scriptMap.put("title", "name");
            scriptMap.put("content", "cell2");

            dataStore.processCsv(null, callback, paramMap, scriptMap, new java.util.HashMap<>(), dataStore.buildCsvConfig(paramMap),
                    csvFile, 0L, "UTF-8", true);

            assertEquals(2, callback.dataMapList.size());
            assertEquals("http://example.com/1", callback.dataMapList.get(0).get("url"));
            assertEquals("alice", callback.dataMapList.get(0).get("title"));
            assertEquals("alice", callback.dataMapList.get(0).get("content"));
            assertEquals("http://example.com/2", callback.dataMapList.get(1).get("url"));
            assertEquals("bob", callback.dataMapList.get(1).get("title"));
        } finally {
            csvFile.delete();
        }
    }

    @Test
    public void test_processCsv_skips_rows_whose_script_produced_no_url() throws Exception {
        registerCrawlerComponents();
        final java.io.File csvFile = writeTempCsv("id,in_stock\n1,true\n2,false\n3,true\n");
        try {
            final TestIndexUpdateCallback callback = new TestIndexUpdateCallback();
            final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
            final java.util.Map<String, String> scriptMap = new java.util.LinkedHashMap<>();
            // The filtering idiom the documentation recommends.
            scriptMap.put("url", "in_stock == \"true\" ? \"http://example.com/\" + id : null");
            scriptMap.put("title", "id");

            dataStore.processCsv(null, callback, paramMap, scriptMap, new java.util.HashMap<>(), dataStore.buildCsvConfig(paramMap),
                    csvFile, 0L, "UTF-8", true);

            // Rows 1 and 3 are indexed; row 2 is skipped quietly rather than recorded as a failure.
            assertEquals(2, callback.dataMapList.size());
            assertEquals("http://example.com/1", callback.dataMapList.get(0).get("url"));
            assertEquals("http://example.com/3", callback.dataMapList.get(1).get("url"));
        } finally {
            csvFile.delete();
        }
    }

    @Test
    public void test_findUnknownParamNames_reports_a_typo() {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("files", "/tmp/a.csv");
        paramMap.put("has_headerline", "true"); // typo of has_header_line

        assertEquals(java.util.List.of("has_headerline"), dataStore.findUnknownParamNames(paramMap));
    }

    @Test
    public void test_findUnknownParamNames_accepts_all_plugin_parameters() {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("files", "/tmp/a.csv");
        paramMap.put("directories", "/tmp");
        paramMap.put("file_encoding", "UTF-8");
        paramMap.put("has_header_line", "true");
        paramMap.put("separator_character", ",");
        paramMap.put("quote_character", "\"");
        paramMap.put("escape_character", "\\");
        paramMap.put("quote_disabled", "false");
        paramMap.put("escape_disabled", "false");
        paramMap.put("skip_lines", "0");
        paramMap.put("ignore_line_patterns", "^#.*");
        paramMap.put("ignore_empty_lines", "true");
        paramMap.put("ignore_trailing_whitespaces", "true");
        paramMap.put("ignore_leading_whitespaces", "true");
        paramMap.put("null_string", "NULL");
        paramMap.put("break_string", "\\n");

        assertTrue(dataStore.findUnknownParamNames(paramMap).isEmpty());
    }

    @Test
    public void test_findUnknownParamNames_accepts_framework_parameters() {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("readInterval", "100");
        paramMap.put("script_type", "groovy");
        paramMap.put("numOfThreads", "4");
        paramMap.put("delete_old_docs", "false");
        paramMap.put("keep_expires_docs", "true");
        paramMap.put("time_to_live", "60");
        // Read directly by FileListIndexUpdateCallbackImpl, reachable from CsvListDataStore.
        paramMap.put("ignore.field.names", "content,digest");

        assertTrue(dataStore.findUnknownParamNames(paramMap).isEmpty());
    }

    @Test
    public void test_findUnknownParamNames_accepts_framework_injected_session_params() {
        // DataIndexHelper always injects these two; warning about them would fire on every crawl.
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("sessionId", "20260822200000");
        paramMap.put("crawlingInfoId", "abc123");

        assertTrue(dataStore.findUnknownParamNames(paramMap).isEmpty());
    }

    @Test
    public void test_findUnknownParamNames_accepts_passthrough_prefixes() {
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("crawler.file.auth", "example");
        paramMap.put("crawler.web.proxyHost", "proxy.example.com");
        paramMap.put("field.event_type", "event_type");
        paramMap.put("event.create", "create");
        paramMap.put("config.tika.tesseract.config", "/etc/tika");
        paramMap.put("client.timeout", "5000");
        paramMap.put("info.charSet", "UTF-8");
        // Collected by prefix in DataConfig for NTLM authentication.
        paramMap.put("jcifs.smb.client.username", "user");

        assertTrue(dataStore.findUnknownParamNames(paramMap).isEmpty());
    }

    @Test
    public void test_findUnknownParamNames_accepts_camel_case_spelling() {
        // ParamMap folds snake_case and camelCase into each other, so both spellings are valid.
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("fileEncoding", "UTF-8");
        paramMap.put("hasHeaderLine", "true");

        assertTrue(dataStore.findUnknownParamNames(paramMap).isEmpty());
    }

    @Test
    public void test_findUnknownParamNames_accepts_ignore_field_names_and_jcifs_prefix_together() {
        // Both are real, valid configuration: ignore.field.names is read directly by
        // FileListIndexUpdateCallbackImpl (reachable from CsvListDataStore), and jcifs.* is collected
        // by prefix in DataConfig for NTLM authentication. Neither should trigger the unknown-param warning.
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("ignore.field.names", "content,digest");
        paramMap.put("jcifs.smb.client.username", "user");

        assertTrue(dataStore.findUnknownParamNames(paramMap).isEmpty());
    }

    @Test
    public void test_findUnknownParamNames_returns_sorted_names() {
        // DataStoreParams is backed by a HashMap, so without an explicit sort the warning text
        // would vary between runs for the same misconfiguration.
        final org.codelibs.fess.entity.DataStoreParams paramMap = new org.codelibs.fess.entity.DataStoreParams();
        paramMap.put("zeta_typo", "1");
        paramMap.put("alpha_typo", "2");
        paramMap.put("mmm_typo", "3");

        assertEquals(java.util.List.of("alpha_typo", "mmm_typo", "zeta_typo"), dataStore.findUnknownParamNames(paramMap));
    }
}
