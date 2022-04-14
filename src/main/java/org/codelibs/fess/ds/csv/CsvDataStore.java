/*
 * Copyright 2012-2022 CodeLibs Project and the Others.
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

import static org.codelibs.core.stream.StreamUtil.stream;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.commons.text.StringEscapeUtils;
import org.codelibs.core.io.CloseableUtil;
import org.codelibs.core.lang.StringUtil;
import org.codelibs.fess.Constants;
import org.codelibs.fess.app.service.FailureUrlService;
import org.codelibs.fess.crawler.exception.CrawlingAccessException;
import org.codelibs.fess.crawler.exception.MultipleCrawlingAccessException;
import org.codelibs.fess.ds.AbstractDataStore;
import org.codelibs.fess.ds.callback.IndexUpdateCallback;
import org.codelibs.fess.entity.DataStoreParams;
import org.codelibs.fess.es.config.exentity.DataConfig;
import org.codelibs.fess.exception.DataStoreCrawlingException;
import org.codelibs.fess.exception.DataStoreException;
import org.codelibs.fess.helper.CrawlerStatsHelper;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsAction;
import org.codelibs.fess.helper.CrawlerStatsHelper.StatsKeyObject;
import org.codelibs.fess.util.ComponentUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.orangesignal.csv.CsvConfig;
import com.orangesignal.csv.CsvReader;

public class CsvDataStore extends AbstractDataStore {

    private static final Logger logger = LoggerFactory.getLogger(CsvDataStore.class);

    protected static final String ESCAPE_CHARACTER_PARAM = "escape_character";

    protected static final String QUOTE_CHARACTER_PARAM = "quote_character";

    protected static final String SEPARATOR_CHARACTER_PARAM = "separator_character";

    protected static final String SKIP_LINES_PARAM = "skip_lines";

    protected static final String IGNORE_LINE_PATTERNS_PARAM = "ignore_line_patterns";

    protected static final String IGNORE_EMPTY_LINES_PARAM = "ignore_empty_lines";

    protected static final String IGNORE_TRAILING_WHITESPACES_PARAM = "ignore_trailing_whitespaces";

    protected static final String IGNORE_LEADING_WHITESPACES_PARAM = "ignore_leading_whitespaces";

    protected static final String NULL_STRING_PARAM = "null_string";

    protected static final String BREAK_STRING_PARAM = "break_string";

    protected static final String ESCAPE_DISABLED_PARAM = "escape_disabled";

    protected static final String QUOTE_DISABLED_PARAM = "quote_disabled";

    protected static final String CSV_FILE_ENCODING_PARAM = "file_encoding";

    protected static final String CSV_FILES_PARAM = "files";

    protected static final String CSV_DIRS_PARAM = "directories";

    protected static final String HAS_HEADER_LINE_PARAM = "has_header_line";

    protected static final String CELL_PREFIX = "cell";

    public String[] csvFileSuffixs = { ".csv", ".tsv" };

    @Override
    protected String getName() {
        return this.getClass().getSimpleName();
    }

    protected List<File> getCsvFileList(final DataStoreParams paramMap) {
        String value = paramMap.getAsString(CSV_FILES_PARAM);
        final List<File> fileList = new ArrayList<>();
        if (StringUtil.isBlank(value)) {
            value = paramMap.getAsString(CSV_DIRS_PARAM);
            if (StringUtil.isBlank(value)) {
                throw new DataStoreException(CSV_FILES_PARAM + " and " + CSV_DIRS_PARAM + " are blank.");
            }
            logger.info("{}={}", CSV_DIRS_PARAM, value);
            final String[] values = value.split(",");
            for (final String path : values) {
                final File dir = new File(path);
                if (dir.isDirectory()) {
                    stream(dir.listFiles()).of(stream -> stream.filter(f -> isCsvFile(f.getParentFile(), f.getName(), paramMap))
                            .sorted((f1, f2) -> (int) (f1.lastModified() - f2.lastModified())).forEach(f -> fileList.add(f)));
                } else {
                    logger.warn("{} is not a directory.", path);
                }
            }
        } else {
            logger.info("{}={}", CSV_FILES_PARAM, value);
            final String[] values = value.split(",");
            for (final String path : values) {
                final File file = new File(path);
                if (file.isFile() && isCsvFile(file.getParentFile(), file.getName(), paramMap)) {
                    fileList.add(file);
                } else {
                    logger.warn("{} is not found.", path);
                }
            }
        }
        if (fileList.isEmpty() && logger.isDebugEnabled()) {
            logger.debug("No csv files in {}", value);
        }
        return fileList;
    }

    protected boolean isCsvFile(final File parentFile, final String filename, final DataStoreParams paramMap) {
        final String name = filename.toLowerCase(Locale.ROOT);
        for (final String suffix : csvFileSuffixs) {
            if (name.endsWith(suffix)) {
                return true;
            }
        }
        return false;
    }

    protected String getCsvFileEncoding(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(CSV_FILE_ENCODING_PARAM);
        if (StringUtil.isBlank(value)) {
            return Constants.UTF_8;
        }
        return value;
    }

    protected boolean hasHeaderLine(final DataStoreParams paramMap) {
        final String value = paramMap.getAsString(HAS_HEADER_LINE_PARAM);
        if (StringUtil.isBlank(value)) {
            return false;
        }
        try {
            return Boolean.parseBoolean(value);
        } catch (final Exception e) {
            return false;
        }
    }

    @Override
    protected void storeData(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap) {

        final long readInterval = getReadInterval(paramMap);

        final List<File> csvFileList = getCsvFileList(paramMap);
        if (csvFileList.isEmpty()) {
            logger.warn("No CSV file.");
            return;
        }

        final String csvFileEncoding = getCsvFileEncoding(paramMap);
        final boolean hasHeaderLine = hasHeaderLine(paramMap);
        final CsvConfig csvConfig = buildCsvConfig(paramMap);

        for (final File csvFile : csvFileList) {
            processCsv(dataConfig, callback, paramMap, scriptMap, defaultDataMap, csvConfig, csvFile, readInterval, csvFileEncoding,
                    hasHeaderLine);
        }
    }

    protected void processCsv(final DataConfig dataConfig, final IndexUpdateCallback callback, final DataStoreParams paramMap,
            final Map<String, String> scriptMap, final Map<String, Object> defaultDataMap, final CsvConfig csvConfig, final File csvFile,
            final long readInterval, final String csvFileEncoding, final boolean hasHeaderLine) {
        logger.info("Loading {}", csvFile.getAbsolutePath());
        final CrawlerStatsHelper crawlerStatsHelper = ComponentUtil.getCrawlerStatsHelper();
        final String scriptType = getScriptType(paramMap);
        CsvReader csvReader = null;
        try {
            csvReader = new CsvReader(new BufferedReader(new InputStreamReader(new FileInputStream(csvFile), csvFileEncoding)), csvConfig);
            List<String> headerList = null;
            if (hasHeaderLine) {
                headerList = csvReader.readValues();
            }
            List<String> list;
            boolean loop = true;
            while ((list = csvReader.readValues()) != null && loop && alive) {
                final StatsKeyObject keyObj = new StatsKeyObject(csvFile.getAbsolutePath() + "#" + csvReader.getLineNumber());
                paramMap.put(Constants.CRAWLER_STATS_KEY, keyObj);
                final Map<String, Object> dataMap = new HashMap<>(defaultDataMap);
                final Map<String, Object> resultMap = new LinkedHashMap<>();
                try {
                    crawlerStatsHelper.begin(keyObj);
                    resultMap.putAll(paramMap.asMap());
                    resultMap.put("csvfile", csvFile.getAbsolutePath());
                    resultMap.put("csvfilename", csvFile.getName());
                    resultMap.put("crawlingConfig", dataConfig);
                    boolean foundValues = false;
                    for (int i = 0; i < list.size(); i++) {
                        String key = null;
                        String value = list.get(i);
                        if (value == null) {
                            value = StringUtil.EMPTY;
                        }
                        if (StringUtil.isNotBlank(value)) {
                            foundValues = true;
                        }
                        if (headerList != null && headerList.size() > i) {
                            key = headerList.get(i);
                            if (StringUtil.isNotBlank(key)) {
                                resultMap.put(key, value);
                            }
                        }
                        key = CELL_PREFIX + Integer.toString(i + 1);
                        resultMap.put(key, value);
                    }
                    if (!foundValues) {
                        logger.debug("No data in line: {}", resultMap);
                        crawlerStatsHelper.discard(keyObj);
                        continue;
                    }

                    crawlerStatsHelper.record(keyObj, StatsAction.PREPARED);

                    if (logger.isDebugEnabled()) {
                        for (final Map.Entry<String, Object> entry : resultMap.entrySet()) {
                            logger.debug("{}={}", entry.getKey(), entry.getValue());
                        }
                    }

                    final Map<String, Object> crawlingContext = new HashMap<>();
                    crawlingContext.put("doc", dataMap);
                    resultMap.put("crawlingContext", crawlingContext);
                    for (final Map.Entry<String, String> entry : scriptMap.entrySet()) {
                        final Object convertValue = convertValue(scriptType, entry.getValue(), resultMap);
                        if (convertValue != null) {
                            dataMap.put(entry.getKey(), convertValue);
                        }
                    }

                    crawlerStatsHelper.record(keyObj, StatsAction.EVALUATED);

                    if (logger.isDebugEnabled()) {
                        for (final Map.Entry<String, Object> entry : dataMap.entrySet()) {
                            logger.debug("{}={}", entry.getKey(), entry.getValue());
                        }
                    }

                    if (dataMap.get("url") instanceof String url) {
                        keyObj.setUrl(url);
                    }

                    callback.store(paramMap, dataMap);
                    crawlerStatsHelper.record(keyObj, StatsAction.FINISHED);
                } catch (final CrawlingAccessException e) {
                    logger.warn("Crawling Access Exception at : " + dataMap, e);

                    Throwable target = e;
                    if (target instanceof MultipleCrawlingAccessException) {
                        final Throwable[] causes = ((MultipleCrawlingAccessException) target).getCauses();
                        if (causes.length > 0) {
                            target = causes[causes.length - 1];
                        }
                    }

                    String errorName;
                    final Throwable cause = target.getCause();
                    if (cause != null) {
                        errorName = cause.getClass().getCanonicalName();
                    } else {
                        errorName = target.getClass().getCanonicalName();
                    }

                    String url;
                    if (target instanceof DataStoreCrawlingException) {
                        final DataStoreCrawlingException dce = (DataStoreCrawlingException) target;
                        url = dce.getUrl();
                        if (dce.aborted()) {
                            loop = false;
                        }
                    } else {
                        url = csvFile.getAbsolutePath() + ":" + csvReader.getLineNumber();
                    }
                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(dataConfig, errorName, url, target);
                    crawlerStatsHelper.record(keyObj, StatsAction.ACCESS_EXCEPTION);
                } catch (final Throwable t) {
                    logger.warn("Crawling Access Exception at : " + dataMap, t);
                    final String url = csvFile.getAbsolutePath() + ":" + csvReader.getLineNumber();
                    final FailureUrlService failureUrlService = ComponentUtil.getComponent(FailureUrlService.class);
                    failureUrlService.store(dataConfig, t.getClass().getCanonicalName(), url, t);
                    crawlerStatsHelper.record(keyObj, StatsAction.EXCEPTION);
                } finally {
                    crawlerStatsHelper.done(keyObj);
                }

                if (readInterval > 0) {
                    sleep(readInterval);
                }
            }
        } catch (final Exception e) {
            throw new DataStoreException("Failed to crawl data when reading csv file.", e);
        } finally {
            CloseableUtil.closeQuietly(csvReader);
        }
    }

    protected CsvConfig buildCsvConfig(final DataStoreParams paramMap) {
        final CsvConfig csvConfig = new CsvConfig();

        if (paramMap.containsKey(SEPARATOR_CHARACTER_PARAM)) {
            final String value = paramMap.getAsString(SEPARATOR_CHARACTER_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    csvConfig.setSeparator(StringEscapeUtils.unescapeJava(value).charAt(0));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", SEPARATOR_CHARACTER_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(QUOTE_CHARACTER_PARAM)) {
            final String value = paramMap.getAsString(QUOTE_CHARACTER_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    csvConfig.setQuote(value.charAt(0));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", QUOTE_CHARACTER_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(ESCAPE_CHARACTER_PARAM)) {
            final String value = paramMap.getAsString(ESCAPE_CHARACTER_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    csvConfig.setEscape(value.charAt(0));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", ESCAPE_CHARACTER_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(QUOTE_DISABLED_PARAM)) {
            final String value = paramMap.getAsString(QUOTE_DISABLED_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    // デフォルトでは無効となっている囲み文字を有効にします。
                    csvConfig.setQuoteDisabled(Boolean.parseBoolean(value));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", QUOTE_DISABLED_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(ESCAPE_DISABLED_PARAM)) {
            final String value = paramMap.getAsString(ESCAPE_DISABLED_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    // デフォルトでは無効となっているエスケープ文字を有効にします。
                    csvConfig.setEscapeDisabled(Boolean.parseBoolean(value));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", ESCAPE_DISABLED_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(BREAK_STRING_PARAM)) {
            final String value = paramMap.getAsString(BREAK_STRING_PARAM);
            if (StringUtil.isNotBlank(value)) {
                // 項目値中の改行を \n で置換えます。
                csvConfig.setBreakString(value);
            }
        }

        if (paramMap.containsKey(NULL_STRING_PARAM)) {
            final String value = paramMap.getAsString(NULL_STRING_PARAM);
            if (StringUtil.isNotBlank(value)) {
                // null 値扱いする文字列を指定します。
                csvConfig.setNullString(value);
            }
        }

        if (paramMap.containsKey(IGNORE_LEADING_WHITESPACES_PARAM)) {
            final String value = paramMap.getAsString(IGNORE_LEADING_WHITESPACES_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    // 項目値前のホワイトスペースを除去します。
                    csvConfig.setIgnoreLeadingWhitespaces(Boolean.parseBoolean(value));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", IGNORE_LEADING_WHITESPACES_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(IGNORE_TRAILING_WHITESPACES_PARAM)) {
            final String value = paramMap.getAsString(IGNORE_TRAILING_WHITESPACES_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    // 項目値後のホワイトスペースを除去します。
                    csvConfig.setIgnoreTrailingWhitespaces(Boolean.parseBoolean(value));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", IGNORE_TRAILING_WHITESPACES_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(IGNORE_EMPTY_LINES_PARAM)) {
            final String value = paramMap.getAsString(IGNORE_EMPTY_LINES_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    // 空行を無視するようにします。
                    csvConfig.setIgnoreEmptyLines(Boolean.parseBoolean(value));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", IGNORE_EMPTY_LINES_PARAM, e);
                }
            }
        }

        if (paramMap.containsKey(IGNORE_LINE_PATTERNS_PARAM)) {
            final String value = paramMap.getAsString(IGNORE_LINE_PATTERNS_PARAM);
            if (StringUtil.isNotBlank(value)) {
                // 正規表現による無視する行パターンを設定します。(この例では # で始まる行)
                csvConfig.setIgnoreLinePatterns(Pattern.compile(value));
            }
        }

        if (paramMap.containsKey(SKIP_LINES_PARAM)) {
            final String value = paramMap.getAsString(SKIP_LINES_PARAM);
            if (StringUtil.isNotBlank(value)) {
                try {
                    // 最初の1行目をスキップして読込みます。
                    csvConfig.setSkipLines(Integer.parseInt(value));
                } catch (final Exception e) {
                    logger.warn("Failed to load {}", SKIP_LINES_PARAM, e);
                }
            }
        }

        return csvConfig;
    }
}
