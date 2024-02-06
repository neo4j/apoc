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
package apoc.util;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.codec.binary.StringUtils.getBytesUtf8;

import java.security.MessageDigest;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.procedure.*;

/**
 * @author mh
 * @since 26.05.16
 */
public class Utils {

    @Context
    public TerminationGuard terminationGuard;

    @UserFunction("apoc.util.sha1")
    @Description("Returns the SHA1 of the concatenation of all `STRING` values in the given `LIST<ANY>`.\n"
            + "SHA1 is a weak hashing algorithm which is unsuitable for cryptographic use-cases.")
    public String sha1(@Name("values") List<Object> values) {
        return hexHash(DigestUtils.getSha1Digest(), values);
    }

    @UserFunction("apoc.util.sha256")
    @Description("Returns the SHA256 of the concatenation of all `STRING` values in the given `LIST<ANY>`.")
    public String sha256(@Name("values") List<Object> values) {
        return hexHash(DigestUtils.getSha256Digest(), values);
    }

    @UserFunction("apoc.util.sha384")
    @Description("Returns the SHA384 of the concatenation of all `STRING` values in the given `LIST<ANY>`.")
    public String sha384(@Name("values") List<Object> values) {
        return hexHash(DigestUtils.getSha384Digest(), values);
    }

    @UserFunction("apoc.util.sha512")
    @Description("Returns the SHA512 of the concatenation of all `STRING` values in the `LIST<ANY>`.")
    public String sha512(@Name("values") List<Object> values) {
        return hexHash(DigestUtils.getSha512Digest(), values);
    }

    @UserFunction("apoc.util.md5")
    @Description("Returns the MD5 checksum of the concatenation of all `STRING` values in the given `LIST<ANY>`.\n"
            + "MD5 is a weak hashing algorithm which is unsuitable for cryptographic use-cases.")
    public String md5(@Name("values") List<Object> values) {
        return hexHash(DigestUtils.getMd5Digest(), values);
    }

    @Procedure("apoc.util.sleep")
    @Description(
            "Causes the currently running Cypher to sleep for the given duration of milliseconds (the transaction termination is honored).")
    public void sleep(@Name("duration") long duration) throws InterruptedException {
        long started = System.currentTimeMillis();
        while (System.currentTimeMillis() - started < duration) {
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
    public void validate(
            @Name("predicate") boolean predicate,
            @Name("message") String message,
            @Name("params") List<Object> params) {
        if (predicate) {
            if (params != null && !params.isEmpty())
                message = String.format(message, params.toArray(new Object[params.size()]));
            throw new RuntimeException(message);
        }
    }

    @UserFunction("apoc.util.validatePredicate")
    @Description(
            "If the given predicate is true an exception is thrown, otherwise it returns true (for use inside `WHERE` subclauses).")
    public boolean validatePredicate(
            @Name("predicate") boolean predicate,
            @Name("message") String message,
            @Name("params") List<Object> params) {
        if (predicate) {
            if (params != null && !params.isEmpty())
                message = String.format(message, params.toArray(new Object[params.size()]));
            throw new RuntimeException(message);
        }

        return true;
    }

    @UserFunction("apoc.util.decompress")
    @Description("Unzips the given byte array.")
    public String decompress(
            @Name("data") byte[] data, @Name(value = "config", defaultValue = "{}") Map<String, Object> config)
            throws Exception {

        CompressionConfig conf = new CompressionConfig(config, CompressionAlgo.GZIP.name());
        return CompressionAlgo.valueOf(conf.getCompressionAlgo()).decompress(data, conf.getCharset());
    }

    @UserFunction("apoc.util.compress")
    @Description("Zips the given `STRING`.")
    public byte[] compress(
            @Name("data") String data, @Name(value = "config", defaultValue = "{}") Map<String, Object> config)
            throws Exception {

        CompressionConfig conf = new CompressionConfig(config, CompressionAlgo.GZIP.name());
        return CompressionAlgo.valueOf(conf.getCompressionAlgo()).compress(data, conf.getCharset());
    }

    private static String hexHash(final MessageDigest digest, final List<Object> values) {
        for (final var value : values) digest.update(getBytesUtf8(toHashString(value)));
        return Hex.encodeHexString(digest.digest());
    }

    /*
     * This is not the most efficient way to produce a hash, but it is backwards compatible.
     * This function is only intended to be used on strings (as documented on the hash functions above)
     * But it turns out that is not how everyone is using it, so as a safety we have stable implementations
     * for all neo4j types.
     */
    private static String toHashString(Object value) {
        if (value instanceof String string) return string;
        else if (value == null) return "";
        else if (value instanceof List<?> list) {
            return list.stream().map(Utils::toHashString).collect(joining(", ", "[", "]"));
        } else if (value instanceof Map<?, ?> map) {
            return map.entrySet().stream()
                    .map(e -> Map.entry(e.getKey().toString(), toHashString(e.getValue())))
                    .sorted(Map.Entry.comparingByKey())
                    .map(e -> e.getKey() + "=" + e.getValue())
                    .collect(joining(", ", "{", "}"));
        } else if (value.getClass().isArray()) {
            if (value instanceof Object[] objectArray) return Arrays.toString(objectArray);
            else if (value instanceof int[] intArray) return Arrays.toString(intArray);
            else if (value instanceof long[] longArray) return Arrays.toString(longArray);
            else if (value instanceof double[] doubleArray) return Arrays.toString(doubleArray);
            else if (value instanceof short[] shortArray) return Arrays.toString(shortArray);
            else if (value instanceof boolean[] boolArray) return Arrays.toString(boolArray);
            else if (value instanceof byte[] byteArray) return Arrays.toString(byteArray);
            else if (value instanceof float[] floatArray) return Arrays.toString(floatArray);
            else if (value instanceof char[] charArray) return Arrays.toString(charArray);
            else return value.toString();
        } else return value.toString();
    }
}
