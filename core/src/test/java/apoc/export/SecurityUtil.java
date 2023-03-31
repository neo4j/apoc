package apoc.export;

import com.ctc.wstx.exc.WstxUnexpectedCharException;
import com.fasterxml.jackson.core.JsonParseException;
import com.nimbusds.jose.util.Pair;
import org.xml.sax.SAXParseException;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static apoc.ApocConfig.APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM;
import static apoc.ApocConfig.APOC_IMPORT_FILE_ENABLED;
import static apoc.ApocConfig.APOC_IMPORT_FILE_USE_NEO4J_CONFIG;
import static apoc.ApocConfig.apocConfig;

public class SecurityUtil {
    public static final Map<String, Class<?>> ALLOWED_EXCEPTIONS = Map.of(
            // load allowed exception
            "apoc.load.json", JsonParseException.class,
            "apoc.load.jsonArray", JsonParseException.class,
            "apoc.load.jsonParams", JsonParseException.class,
            "apoc.load.xml", SAXParseException.class,

            // import allowed exception
            "apoc.import.json", JsonParseException.class,
            "apoc.import.csv", NoSuchElementException.class,
            "apoc.import.graphml", JsonParseException.class,
            "apoc.import.xml", WstxUnexpectedCharException.class
    );

    public static Stream<Pair<String, String>> IMPORT_PROCEDURES = Stream.of(
            Pair.of("json", "($fileName)"),
            Pair.of("csv", "([{fileName: $fileName, labels: ['Person']}], [], {})"),
            Pair.of("csv", "([], [{fileName: $fileName, type: 'KNOWS'}], {})"),
            Pair.of("graphml", "($fileName, {})"),
            Pair.of("xml", "($fileName)")
    );

    public static Stream<Pair<String, String>> LOAD_PROCEDURES = Stream.of(
            Pair.of("json", "($fileName, '', {})"),
            Pair.of("jsonArray", "($fileName, '', {})"),
            Pair.of("jsonParams", "($fileName, {}, '')"),
            Pair.of("xml", "($fileName, '', {}, false)")
    );

    public static void setFileApocConfigs(boolean importEnabled, boolean useNeo4jConfs, boolean allowReadFromFs) {
        apocConfig().setProperty(APOC_IMPORT_FILE_ENABLED, importEnabled);
        apocConfig().setProperty(APOC_IMPORT_FILE_USE_NEO4J_CONFIG, useNeo4jConfs);
        apocConfig().setProperty(APOC_IMPORT_FILE_ALLOW__READ__FROM__FILESYSTEM, allowReadFromFs);
    }
}
