package apoc.result;

import apoc.export.cypher.ExportFileManager;
import apoc.export.util.ExportConfig;
import java.io.StringWriter;
import org.neo4j.procedure.Description;

public class DataProgressInfo implements ProgressInfo {
    @Description("The name of the file to which the data was exported.")
    public final String file;

    @Description("The number of batches the export was run in.")
    public long batches;

    @Description("A summary of the exported data.")
    public String source;

    @Description("The format the file is exported in.")
    public final String format;

    @Description("The number of exported nodes.")
    public long nodes;

    @Description("The number of exported relationships.")
    public long relationships;

    @Description("The number of exported properties.")
    public long properties;

    @Description("The duration of the export.")
    public long time;

    @Description("The number of rows returned.")
    public long rows;

    @Description("The size of the batches the export was run in.")
    public long batchSize;

    @Description("The executed Cypher Statements.")
    public Object cypherStatements;

    @Description("The executed node statements.")
    public Object nodeStatements;

    @Description("The executed relationship statements.")
    public Object relationshipStatements;

    @Description("The executed schema statements.")
    public Object schemaStatements;

    @Description("The executed cleanup statements.")
    public Object cleanupStatements;

    public DataProgressInfo(String file, String source, String format) {
        this.file = file;
        this.source = source;
        this.format = format;
    }

    public DataProgressInfo(DataProgressInfo pi) {
        this.file = pi.file;
        this.format = pi.format;
        this.source = pi.source;
        this.nodes = pi.nodes;
        this.relationships = pi.relationships;
        this.properties = pi.properties;
        this.time = pi.time;
        this.rows = pi.rows;
        this.batchSize = pi.batchSize;
        this.batches = pi.batches;
    }

    @Override
    public String toString() {
        return String.format("nodes = %d rels = %d properties = %d", nodes, relationships, properties);
    }

    public DataProgressInfo update(long nodes, long relationships, long properties) {
        this.nodes += nodes;
        this.relationships += relationships;
        this.properties += properties;
        return this;
    }

    public DataProgressInfo updateTime(long start) {
        this.time = System.currentTimeMillis() - start;
        return this;
    }

    public DataProgressInfo done(long start) {
        return updateTime(start);
    }

    public void nextRow() {
        this.rows++;
    }

    public DataProgressInfo drain(StringWriter writer, ExportConfig config) {
        return this;
    }

    @Override
    public void setBatches(long batches) {
        this.batches = batches;
    }

    @Override
    public void setRows(long rows) {
        this.rows = rows;
    }

    @Override
    public long getBatchSize() {
        return this.batchSize;
    }

    public DataProgressInfo enrich(ExportFileManager fileInfo) {
        cypherStatements = fileInfo.drain("cypher");
        nodeStatements = fileInfo.drain("nodes");
        relationshipStatements = fileInfo.drain("relationships");
        schemaStatements = fileInfo.drain("schema");
        cleanupStatements = fileInfo.drain("cleanup");
        return this;
    }

    public static final DataProgressInfo EMPTY = new DataProgressInfo(null, null, null);
}
