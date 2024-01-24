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

import static apoc.util.MapUtil.map;
import static apoc.util.TestUtil.testCallEmpty;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.toUnmodifiableMap;
import static java.util.stream.IntStream.range;
import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.QueryExecutionException;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.test.rule.DbmsRule;
import org.neo4j.test.rule.ImpermanentDbmsRule;
import org.neo4j.values.storable.RandomValues;
import org.neo4j.values.storable.ValueType;

/**
 * @author mh
 * @since 26.05.16
 */
public class UtilsTest {

    private static final String SIMPLE_STRING = "Test";
    private static final String COMPLEX_STRING = "Mätrix II 哈哈\uD83D\uDE04123";

    @ClassRule
    public static DbmsRule db = new ImpermanentDbmsRule();

    @BeforeClass
    public static void setUp() {
        TestUtil.registerProcedure(db, Utils.class);
    }

    @AfterClass
    public static void teardown() {
        db.shutdown();
    }

    @Test
    public void testMultipleCharsetsCompressionWithDifferentResults() {

        List<String> listCompressed = new ArrayList<>();

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-8'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-16'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-16BE'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-16LE'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {charset: 'ISO-8859-1'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {charset: 'UTF-32'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {charset: 'US-ASCII'}) AS value",
                map("text", COMPLEX_STRING),
                r -> listCompressed.add(encodeBase64FromBytesToString((byte[]) r.get("value"))));

        // expected all different compressed string in complex string
        long sizeArray = listCompressed.size();
        assertEquals(sizeArray, new HashSet<>(listCompressed).size());
    }

    @Test
    public void testValueMainCompressorAlgoOnSimpleString() {

        String TEST_TO_GZIP = "H4sIAAAAAAAA/wtJLS4BADLRTXgEAAAA";
        String TEST_TO_DEFLATE = "eJwLSS0uAQAD3QGh";
        String TEST_TO_BZIP2 = "QlpoOTFBWSZTWdliiV0AAAADgAQAAgAMACAAMM00GaaJni7kinChIbLFEro=";

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {compression: 'GZIP'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(TEST_TO_GZIP, encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {compression: 'BZIP2'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(TEST_TO_BZIP2, encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {compression: 'DEFLATE'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(TEST_TO_DEFLATE, encodeBase64FromBytesToString((byte[]) r.get("value"))));

        TestUtil.testCall(
                db,
                "RETURN apoc.util.compress($text, {compression: 'NONE'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, new String((byte[]) r.get("value"))));
    }

    @Test
    public void testWrongDecompressionFromPreviousDifferentCompressionAlgo() {
        RuntimeException e = assertThrows(
                RuntimeException.class,
                () -> TestUtil.testCall(
                        db,
                        "WITH apoc.util.compress('test', {compression: 'GZIP'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'DEFLATE'}) AS value",
                        r -> {}));

        String expectedMessage =
                "Failed to invoke function `apoc.util.decompress`: Caused by: java.util.zip.ZipException: incorrect header check";
        assertEquals(expectedMessage, e.getMessage());
    }

    @Test
    public void testWrongDecompressionFromPreviousDifferentCharset() {

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-8'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-8'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-8'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertNotEquals(COMPLEX_STRING, r.get("value")));
    }

    @Test
    public void testCompressAndDecompressWithMultipleCompressionCharsetsReturningStartString() {

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text) AS compressed RETURN apoc.util.decompress(compressed) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-8'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-8'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16BE'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16BE'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-16LE'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-16LE'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'UTF-32'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'UTF-32'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'US-ASCII'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'US-ASCII'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {charset: 'ISO-8859-1'}) AS compressed RETURN apoc.util.decompress(compressed, {charset: 'ISO-8859-1'}) AS value",
                map("text", SIMPLE_STRING),
                r -> assertEquals(SIMPLE_STRING, r.get("value")));
    }

    @Test
    public void testCompressAndDecompressWithMultipleCompressionAlgosReturningStartString() {

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text) AS compressed RETURN apoc.util.decompress(compressed) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'BZIP2'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'BZIP2'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'DEFLATE'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'DEFLATE'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'BLOCK_LZ4'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'BLOCK_LZ4'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'FRAMED_SNAPPY'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'FRAMED_SNAPPY'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));

        TestUtil.testCall(
                db,
                "WITH apoc.util.compress($text, {compression: 'NONE'}) AS compressed RETURN apoc.util.decompress(compressed, {compression: 'NONE'}) AS value",
                map("text", COMPLEX_STRING),
                r -> assertEquals(COMPLEX_STRING, r.get("value")));
    }

    @Test
    public void testSha1() {
        TestUtil.testCall(
                db,
                "RETURN apoc.util.sha1(['ABC']) AS value",
                r -> assertEquals("3c01bdbb26f358bab27f267924aa2c9a03fcfdb8", r.get("value")));
    }

    @Test
    public void testMd5() {
        TestUtil.testCall(
                db,
                "RETURN apoc.util.md5(['ABC']) AS value",
                r -> assertEquals("902fbdd2b1df0c4f70b4a5d23525e932", r.get("value")));
    }

    @Test
    public void md5WithNestedTypes() {
        TestUtil.testCall(
                db,
                "RETURN apoc.util.md5(['ABC', true, {b: 'hej', a: [1, 2]}, ['hej', {b:'hå'}]]) AS value",
                r -> assertEquals("f0603bd38267c6e59c0cf6896f4b4bcd", r.get("value")));
    }

    @Test
    public void md5IsStableOnAllTypes() {
        hashIsStable("md5");
    }

    @Test
    public void sha1IsStableOnAllTypes() {
        hashIsStable("sha1");
    }

    @Test
    public void sha256IsStableOnAllTypes() {
        hashIsStable("sha256");
    }

    @Test
    public void sha384IsStableOnAllTypes() {
        hashIsStable("sha384");
    }

    public void hashIsStable(String hashFunc) {
        final var seed = new Random().nextLong();
        final var rand = RandomValues.create(new Random(seed));
        final var randStorables = stream(ValueType.values())
                .map(t -> rand.nextValueOfType(t).asObject())
                .toList();
        final var randMap =
                range(0, randStorables.size()).boxed().collect(toUnmodifiableMap(i -> "p" + i, randStorables::get));

        // Create node with random property values
        try (final var tx = db.beginTx()) {
            final var node = tx.createNode(Label.label("HashFunctionsAreStable"));
            final var rel = node.createRelationshipTo(node, RelationshipType.withName("R"));
            for (int i = randStorables.size() - 1; i >= 0; --i) {
                node.setProperty("p" + i, randStorables.get(i));
                rel.setProperty("p" + i, randStorables.get(i));
            }
            tx.commit();
        }

        try (final var tx = db.beginTx()) {
            for (int i = 0; i < randStorables.size(); ++i) {
                final var value = randStorables.get(i);
                final var query =
                        """
                        match (n:HashFunctionsAreStable)
                        return
                          apoc.util.%s([n.%s]) as hash1,
                          apoc.util.%s([$value]) as hash2,
                          apoc.util.%s($list) as hash3
                        """
                                .formatted(hashFunc, "p" + i, hashFunc, hashFunc);
                final var params = Map.of("value", value, "list", List.of(value));
                assertStableHash(seed, value, tx, query, params);
            }

            final var mapQuery =
                    """
                    match (n:HashFunctionsAreStable)
                    return
                      apoc.util.%s([properties(n)]) as hash1,
                      apoc.util.%s($value) as hash2
                    """
                            .formatted(hashFunc, hashFunc);
            assertStableHash(seed, randMap, tx, mapQuery, Map.of("value", List.of(randMap)));

            final var listCypher = IntStream.range(0, randStorables.size())
                    .mapToObj(i -> "n.p" + i)
                    .collect(Collectors.joining(","));
            final var listQuery =
                    """
                match (n:HashFunctionsAreStable)
                return
                  apoc.util.%s([%s]) as hash1,
                  apoc.util.%s($value) as hash2
                """
                            .formatted(hashFunc, listCypher, hashFunc);
            assertStableHash(seed, randStorables, tx, listQuery, Map.of("value", randStorables));

            final var listOfListsQuery =
                    """
                match (n:HashFunctionsAreStable)
                return
                  apoc.util.%s([[%s]]) as hash1,
                  apoc.util.%s([$value]) as hash2
                """
                            .formatted(hashFunc, listCypher, hashFunc);
            assertStableHash(seed, randStorables, tx, listOfListsQuery, Map.of("value", randStorables));

            final var nodesHashQuery =
                """
                match (a:HashFunctionsAreStable)-[r]->(b:HashFunctionsAreStable)
                return
                  apoc.util.%s([a, b, r, [a, b, r]]) as hash1,
                  apoc.util.%s([a, b, r, [a, b, r]]) as hash2
                """.formatted(hashFunc, hashFunc);
            assertStableHash(seed, "nodes and rels", tx, nodesHashQuery, Map.of());
        } finally {
            db.executeTransactionally("cypher runtime=slotted match (n) detach delete n");
        }
    }

    private static void assertStableHash(
            long seed, Object val, Transaction tx, String query, Map<String, Object> params) {
        final var a = tx.execute(query, params).stream().toList();
        final var b = tx.execute(query, params).stream().toList();
        final var message = "%s should have stable hash (seed=%s)".formatted(val, seed);
        assertEquals(message, a, b);
        assertEquals(1, a.size());
        final var first = a.get(0).entrySet().iterator().next();
        for (final var e : a.get(0).entrySet()) {
            assertEquals(message, first.getValue(), e.getValue());
        }
    }

    @Test
    public void testValidateFalse() {
        TestUtil.testResult(db, "CALL apoc.util.validate(false,'message',null)", r -> assertEquals(false, r.hasNext()));
    }

    @Test
    public void testValidateTrue() {
        try {
            db.executeTransactionally("CALL apoc.util.validate(true,'message %d',[42])");
            fail("should have failed");
        } catch (QueryExecutionException qee) {
            assertEquals(
                    "Failed to invoke procedure `apoc.util.validate`: Caused by: java.lang.RuntimeException: message 42",
                    qee.getCause().getCause().getMessage());
        }
    }

    @Test
    public void testValidatePredicateReturn() {
        TestUtil.testCall(
                db,
                "RETURN apoc.util.validatePredicate(false,'message',null) AS value",
                r -> assertEquals(true, r.get("value")));
    }

    @Test
    public void testValidatePredicateTrue() {
        db.executeTransactionally("CREATE (:Person {predicate: true})");
        TestUtil.testFail(
                db,
                "MATCH (n:Person) RETURN apoc.util.validatePredicate(n.predicate,'message %d',[42]) AS n",
                QueryExecutionException.class);
    }

    @Test
    public void testSleep() {
        String cypherSleep = "call apoc.util.sleep($duration)";
        testCallEmpty(db, cypherSleep, MapUtil.map("duration", 0l)); // force building query plan

        long duration = 300;
        TestUtil.assertDuration(Matchers.greaterThanOrEqualTo(duration), () -> {
            testCallEmpty(db, cypherSleep, MapUtil.map("duration", duration));
            return null;
        });
    }

    @Test
    public void testSleepWithTerminate() {
        String cypherSleep = "call apoc.util.sleep($duration)";
        testCallEmpty(db, cypherSleep, MapUtil.map("duration", 0l)); // force building query plan

        long duration = 10000;
        TestUtil.assertDuration(Matchers.lessThan(duration), () -> {
            final Transaction[] tx = new Transaction[1];

            Future future = Executors.newSingleThreadScheduledExecutor().submit(() -> {
                tx[0] = db.beginTx();
                try {
                    Result result = tx[0].execute(cypherSleep, MapUtil.map("duration", duration));
                    tx[0].commit();
                    return result;
                } finally {
                    tx[0].close();
                }
            });

            sleepUntil(dummy -> tx[0] != null);
            tx[0].terminate();
            try {
                future.get();
            } catch (InterruptedException | ExecutionException e) {
                throw new RuntimeException(e);
            }
            return null;
        });
    }

    private void sleepUntil(Predicate<Void> predicate) {
        while (!predicate.test(null)) {
            try {
                Thread.sleep(5);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private String encodeBase64FromBytesToString(byte[] list) {
        return Base64.getEncoder().encodeToString(list);
    }
}
