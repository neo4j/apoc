package apoc.util;

import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.procedure.*;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author mh
 * @since 26.05.16
 */
public class Utils {

    @Context
    public TerminationGuard terminationGuard;

    @UserFunction("apoc.util.sha1")
    @Description("Returns the SHA1 of the concatenation of all string values in the given list.\n" +
            "SHA1 is a weak hashing algorithm which is unsuitable for cryptographic use-cases.")
    public String sha1(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha1Hex(value);
    }

    @UserFunction("apoc.util.sha256")
    @Description("Returns the SHA256 of the concatenation of all string values in the given list.")
    public String sha256(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha256Hex(value);
    }

    @UserFunction("apoc.util.sha384")
    @Description("Returns the SHA384 of the concatenation of all string values in the given list.")
    public String sha384(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha384Hex(value);
    }

    @UserFunction("apoc.util.sha512")
    @Description("Returns the SHA512 of the concatenation of all string values in the list.")
    public String sha512(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.sha512Hex(value);
    }

    @UserFunction("apoc.util.md5")
    @Description("Returns the MD5 checksum of the concatenation of all string values in the given list.\n" +
            "MD5 is a weak hashing algorithm which is unsuitable for cryptographic use-cases.")
    public String md5(@Name("values") List<Object> values) {
        String value = values.stream().map(v -> v == null ? "" : v.toString()).collect(Collectors.joining());
        return DigestUtils.md5Hex(value);
    }

    @Procedure("apoc.util.sleep")
    @Description("Causes the currently running Cypher to sleep for the given duration of milliseconds (the transaction termination is honored).")
    public void sleep(@Name("duration") long duration) throws InterruptedException {
        long started = System.currentTimeMillis();
        while (System.currentTimeMillis()-started < duration) {
            try {
                Thread.sleep(5);
                terminationGuard.check();
            } catch (TransactionTerminatedException e) {
                return;
            }
        }
    }

    @Procedure("apoc.util.validate")
    @Description("If the given predicate is true an exception is thrown.")
    public void validate(@Name("predicate") boolean predicate, @Name("message") String message, @Name("params") List<Object> params) {
        if (predicate) {
            if (params!=null && !params.isEmpty()) message = String.format(message,params.toArray(new Object[params.size()]));
            throw new RuntimeException(message);
        }
    }

    @UserFunction("apoc.util.validatePredicate")
    @Description("If the given predicate is true an exception is thrown, otherwise it returns true (for use inside `WHERE` subclauses).")
    public boolean validatePredicate(@Name("predicate") boolean predicate, @Name("message") String message, @Name("params") List<Object> params) {
        if (predicate) {
            if (params!=null && !params.isEmpty()) message = String.format(message,params.toArray(new Object[params.size()]));
            throw new RuntimeException(message);
        }

        return true;
    }

    @UserFunction("apoc.util.decompress")
    @Description("Unzips the given byte array.")
    public String decompress(@Name("data") byte[] data, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        CompressionConfig conf = new CompressionConfig(config, CompressionAlgo.GZIP.name());
        return CompressionAlgo.valueOf(conf.getCompressionAlgo()).decompress(data, conf.getCharset());
    }

    @UserFunction("apoc.util.compress")
    @Description("Zips the given string.")
    public byte[] compress(@Name("data") String data, @Name(value = "config", defaultValue = "{}") Map<String, Object> config) throws Exception {

        CompressionConfig conf = new CompressionConfig(config, CompressionAlgo.GZIP.name());
        return CompressionAlgo.valueOf(conf.getCompressionAlgo()).compress(data, conf.getCharset());
    }
}
