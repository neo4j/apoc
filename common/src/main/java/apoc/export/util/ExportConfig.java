/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package apoc.export.util;

import static apoc.util.Util.toBoolean;
import static java.util.Arrays.asList;

import apoc.export.cypher.formatter.CypherFormat;
import apoc.util.CompressionConfig;
import apoc.util.Util;
import java.util.*;

/**
 * @author mh
 * @since 19.01.14
 */
public class ExportConfig extends CompressionConfig {
    public static final String RELS_WITH_TYPE_KEY = "multipleRelationshipsWithType";

    public static class NodeConfig {
        public String label;
        public String id;

        public NodeConfig(Map<String, String> config) {
            config = config == null ? Collections.emptyMap() : config;
            this.label = config.get("label");
            this.id = config.get("id");
        }
    }

    public static final ExportConfig EMPTY = new ExportConfig(null);

    public static final char QUOTECHAR = '"';
    public static final String NO_QUOTES = "none";
    public static final String ALWAYS_QUOTES = "always";
    public static final String IF_NEEDED_QUOTES = "ifNeeded";

    public static final int DEFAULT_BATCH_SIZE = 20000;
    private static final int DEFAULT_UNWIND_BATCH_SIZE = 20;
    public static final String DEFAULT_DELIM = ",";
    public static final String DEFAULT_ARRAY_DELIM = ";";
    public static final String DEFAULT_QUOTES = ALWAYS_QUOTES;
    private final boolean streamStatements;
    private final boolean ifNotExists;
    private final NodeConfig source;
    private final NodeConfig target;

    private int batchSize;
    private boolean multipleRelationshipsWithType;
    private boolean saveIndexNames;
    private boolean bulkImport;
    private boolean sampling;
    private String delim;
    private String quotes;

    // If true, null values and empty string values can be differentiated by an empty value, and ""
    // Note: quotes must be ifNeeded or Always for this to work
    private Boolean differentiateNulls;
    private boolean useTypes;
    private Set<String> caption;
    private boolean writeNodeProperties;
    private boolean writeRelationshipProperties;
    private boolean nodesOfRelationships;
    private ExportFormat format;
    private CypherFormat cypherFormat;
    private final Map<String, Object> config;
    private boolean separateHeader;
    private String arrayDelim;
    private Map<String, Object> optimizations;

    public enum OptimizationType {
        NONE,
        UNWIND_BATCH,
        UNWIND_BATCH_PARAMS
    }

    private OptimizationType optimizationType;
    private int unwindBatchSize;
    private long awaitForIndexes;
    private final Map<String, Object> samplingConfig;

    public int getBatchSize() {
        return batchSize;
    }

    public boolean isBulkImport() {
        return bulkImport;
    }

    public char getDelimChar() {
        return delim.charAt(0);
    }

    public String isQuotes() {
        return quotes;
    }

    public Boolean shouldDifferentiateNulls() {
        return differentiateNulls;
    }

    public boolean useTypes() {
        return useTypes;
    }

    public ExportFormat getFormat() {
        return format;
    }

    public Set<String> getCaption() {
        return caption;
    }

    public CypherFormat getCypherFormat() {
        return cypherFormat;
    }

    public boolean isMultipleRelationshipsWithType() {
        return multipleRelationshipsWithType;
    }

    public ExportConfig(Map<String, Object> config) {
        this(config, ExportFormat.CYPHER_SHELL);
    }

    public ExportConfig(Map<String, Object> config, ExportFormat exportFormat) {
        super(config);
        config = config != null ? config : Collections.emptyMap();
        this.saveIndexNames = toBoolean(config.getOrDefault("saveIndexNames", false));
        this.delim = delim(config.getOrDefault("delim", DEFAULT_DELIM).toString());
        this.arrayDelim =
                delim(config.getOrDefault("arrayDelim", DEFAULT_ARRAY_DELIM).toString());
        this.useTypes = toBoolean(config.get("useTypes"));
        this.caption = convertCaption(config.getOrDefault("caption", asList("name", "title", "label", "id")));
        this.nodesOfRelationships = toBoolean(config.get("nodesOfRelationships"));
        this.bulkImport = toBoolean(config.get("bulkImport"));
        this.separateHeader = toBoolean(config.get("separateHeader"));
        this.format = ExportFormat.fromString((String) config.getOrDefault("format", exportFormat.getFormat()));
        this.cypherFormat = CypherFormat.fromString((String) config.getOrDefault("cypherFormat", "create"));
        this.config = config;
        this.streamStatements = toBoolean(config.get("streamStatements")) || toBoolean(config.get("stream"));
        this.writeNodeProperties = toBoolean(config.getOrDefault("writeNodeProperties", true));
        this.writeRelationshipProperties =
                toBoolean(config.getOrDefault("writeRelationshipProperties", this.writeNodeProperties));
        this.ifNotExists = toBoolean(config.get("ifNotExists"));
        exportQuotes(config);
        this.differentiateNulls = toBoolean(config.getOrDefault("differentiateNulls", false));
        this.optimizations = (Map<String, Object>) config.getOrDefault("useOptimizations", Collections.emptyMap());
        this.optimizationType = OptimizationType.valueOf(optimizations
                .getOrDefault("type", OptimizationType.UNWIND_BATCH.toString())
                .toString()
                .toUpperCase());
        this.batchSize = ((Number) config.getOrDefault("batchSize", DEFAULT_BATCH_SIZE)).intValue();
        this.sampling = toBoolean(config.getOrDefault("sampling", false));
        this.samplingConfig = (Map<String, Object>) config.getOrDefault("samplingConfig", new HashMap<>());
        this.unwindBatchSize =
                ((Number) getOptimizations().getOrDefault("unwindBatchSize", DEFAULT_UNWIND_BATCH_SIZE)).intValue();
        this.awaitForIndexes = ((Number) config.getOrDefault("awaitForIndexes", 300)).longValue();
        this.multipleRelationshipsWithType = toBoolean(config.get(RELS_WITH_TYPE_KEY));
        this.source = new NodeConfig((Map<String, String>) config.get("source"));
        this.target = new NodeConfig((Map<String, String>) config.get("target"));
        validate();
    }

    private void validate() {
        if (OptimizationType.UNWIND_BATCH_PARAMS.equals(this.optimizationType)
                && !ExportFormat.CYPHER_SHELL.equals(this.format)) {
            throw new RuntimeException(
                    "`useOptimizations: 'UNWIND_BATCH_PARAMS'` can be used only in combination with `format: 'CYPHER_SHELL' but got [format:`"
                            + this.format + "]");
        }
        // CSV doesn't use optimization type
        if (!OptimizationType.NONE.equals(this.optimizationType)
                && this.unwindBatchSize > this.batchSize
                && !ExportFormat.CSV.equals(this.format)) {
            throw new RuntimeException("`unwindBatchSize` must be <= `batchSize`, but got [unwindBatchSize:"
                    + unwindBatchSize + ", batchSize:" + batchSize + "]");
        }
    }

    private void exportQuotes(Map<String, Object> config) {
        try {
            this.quotes = (String) config.getOrDefault("quotes", DEFAULT_QUOTES);

            if (!quotes.equals(ALWAYS_QUOTES) && !quotes.equals(NO_QUOTES) && !quotes.equals(IF_NEEDED_QUOTES)) {
                throw new RuntimeException("The string value of the field quote is not valid");
            }

        } catch (ClassCastException e) { // backward compatibility
            this.quotes = toBoolean(config.get("quotes")) ? ALWAYS_QUOTES : NO_QUOTES;
        }
    }

    public boolean getRelsInBetween() {
        return nodesOfRelationships;
    }

    private static String delim(String value) {
        if (value.length() == 1) return value;
        if (value.contains("\\t")) return String.valueOf('\t');
        if (value.contains(" ")) return " ";
        throw new RuntimeException("Illegal delimiter '" + value + "'");
    }

    public String defaultRelationshipType() {
        return config.getOrDefault("defaultRelationshipType", "RELATED").toString();
    }

    public NodeConfig getSource() {
        return source;
    }

    public NodeConfig getTarget() {
        return target;
    }

    public boolean readLabels() {
        return toBoolean(config.getOrDefault("readLabels", false));
    }

    public boolean storeNodeIds() {
        return toBoolean(config.getOrDefault("storeNodeIds", false));
    }

    public boolean separateFiles() {
        return toBoolean(config.getOrDefault("separateFiles", false));
    }

    private static Set<String> convertCaption(Object value) {
        if (value == null) return null;
        if (!(value instanceof List)) throw new RuntimeException("Only array of Strings are allowed!");
        List<String> strings = (List<String>) value;
        return new HashSet<>(strings);
    }

    public boolean streamStatements() {
        return streamStatements;
    }

    public boolean writeNodeProperties() {
        return writeNodeProperties;
    }

    public boolean writeRelationshipProperties() {
        return writeRelationshipProperties;
    }

    public long getTimeoutSeconds() {
        return Util.toLong(config.getOrDefault("timeoutSeconds", 100));
    }

    public int getUnwindBatchSize() {
        return unwindBatchSize;
    }

    public Map<String, Object> getOptimizations() {
        return optimizations;
    }

    public boolean isSeparateHeader() {
        return this.separateHeader;
    }

    public String getArrayDelim() {
        return arrayDelim;
    }

    public OptimizationType getOptimizationType() {
        return optimizationType;
    }

    public long getAwaitForIndexes() {
        return awaitForIndexes;
    }

    public Map<String, Object> getSamplingConfig() {
        return samplingConfig;
    }

    public boolean isSampling() {
        return sampling;
    }

    public boolean ifNotExists() {
        return ifNotExists;
    }

    public boolean shouldSaveIndexNames() {
        return saveIndexNames;
    }
}
