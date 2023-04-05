package apoc.export.arrow;

import apoc.Pools;
import apoc.result.ByteArrayResult;
import apoc.result.ProgressInfo;
import org.neo4j.cypher.export.SubGraph;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Result;
import org.neo4j.logging.Log;
import org.neo4j.procedure.TerminationGuard;

import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_EXPORT_FILE_ENABLED;
import static apoc.ApocConfig.EXPORT_NOT_ENABLED_ERROR;
import static apoc.ApocConfig.apocConfig;

public class ExportArrowService {

    public static final String EXPORT_TO_FILE_ARROW_ERROR = EXPORT_NOT_ENABLED_ERROR +
            "\nOtherwise, if you are running in a cloud environment without filesystem access, use the apoc.export.arrow.stream.* procedures to stream the export back to your client.";
    private final GraphDatabaseService db;
    private final Pools pools;
    private final TerminationGuard terminationGuard;
    private final Log logger;

    public ExportArrowService(GraphDatabaseService db, Pools pools, TerminationGuard terminationGuard, Log logger) {
        this.db = db;
        this.pools = pools;
        this.terminationGuard = terminationGuard;
        this.logger = logger;
    }

    public Stream<ByteArrayResult> stream(Object data, ArrowConfig config) {
        if (data instanceof Result) {
            return new ExportResultStreamStrategy(db, pools, terminationGuard, logger).export((Result) data, config);
        } else {
            return new ExportGraphStreamStrategy(db, pools, terminationGuard, logger).export((SubGraph) data, config);
        }
    }

    public Stream<ProgressInfo> file(String fileName, Object data, ArrowConfig config) {
        // we cannot use apocConfig().checkWriteAllowed(..) because the error is confusing
        //  since it says "... use the `{stream:true}` config", but with arrow procedures the streaming mode is implemented via different procedures
        if (!apocConfig().getBoolean(APOC_EXPORT_FILE_ENABLED)) {
            throw new RuntimeException(EXPORT_TO_FILE_ARROW_ERROR);
        }
        if (data instanceof Result) {
            return new ExportResultFileStrategy(fileName, db, pools, terminationGuard, logger).export((Result) data, config);
        } else {
            return new ExportGraphFileStrategy(fileName, db, pools, terminationGuard, logger).export((SubGraph) data, config);
        }
    }
}